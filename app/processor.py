"""Network-bound domain probes. Network failures are data, not worker crashes."""
import asyncio
import ipaddress
import re
import time
from html.parser import HTMLParser

import dns.asyncresolver
import dns.exception
import httpx

from .schemas import DnsRecordType, DomainStatus, HttpScheme

LABEL = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
RECORD_TYPES = tuple(DnsRecordType)


class TitleParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.inside = False
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'title':
            self.inside = True

    def handle_endtag(self, tag):
        if tag.lower() == 'title':
            self.inside = False

    def handle_data(self, data):
        if self.inside:
            self.parts.append(data)


def normalize_domain(raw: str) -> str:
    """Reject URLs, IP literals, credentials, and malformed DNS names."""
    domain = raw.strip().lower().rstrip('.')
    try:
        domain = domain.encode('idna').decode('ascii')
    except UnicodeError as exc:
        raise ValueError('Invalid internationalized domain') from exc
    if len(domain) > 253 or '.' not in domain or any(
        not LABEL.fullmatch(label) for label in domain.split('.')
    ):
        raise ValueError('Invalid domain name')
    try:
        ipaddress.ip_address(domain)
    except ValueError:
        return domain
    raise ValueError('IP addresses are not accepted')


async def lookup_dns(domain: str, timeout: float) -> dict[str, list[str]]:
    resolver = dns.asyncresolver.Resolver()
    resolver.timeout = min(2.0, timeout)
    resolver.lifetime = timeout

    async def query(record: str):
        try:
            answer = await resolver.resolve(domain, record, raise_on_no_answer=False)
            if answer.rrset is None:
                return []
            return [str(value).strip('"') for value in answer]
        except (dns.exception.DNSException, OSError):
            return []

    values = await asyncio.gather(*(query(t) for t in RECORD_TYPES))
    return dict(zip(RECORD_TYPES, values))


def public_ips(addresses: list[str]) -> bool:
    """Fail closed when a hostname has no A record or any non-global A address."""
    if not addresses:
        return False
    try:
        return all(ipaddress.ip_address(ip).is_global for ip in addresses)
    except ValueError:
        return False


async def probe_http(domain: str, timeout: float) -> dict:
    # Redirects disabled: avoids following an unexpected redirect into private networks.
    # Streaming avoids downloading an unbounded page body.
    errors = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False) as client:
        for scheme in HttpScheme:
            started = time.perf_counter()
            try:
                async with client.stream('GET', f'{scheme}://{domain}/') as response:
                    content_type = response.headers.get('content-type', '').lower()
                    body = bytearray()
                    if 'text/html' in content_type:
                        async for chunk in response.aiter_bytes():
                            body.extend(chunk[:max(0, 65536 - len(body))])
                            if len(body) >= 65536:
                                break
                    elapsed = round((time.perf_counter() - started) * 1000, 2)
                    parser = TitleParser()
                    if body:
                        parser.feed(body.decode('utf-8', errors='replace'))
                    return {
                        'scheme': scheme, 'http_status': response.status_code,
                        'title': ''.join(parser.parts).strip()[:300] or None,
                        'response_time_ms': elapsed,
                    }
            except (httpx.RequestError, asyncio.TimeoutError) as exc:
                errors.append(f'{scheme.upper()}: {type(exc).__name__}')
    raise RuntimeError('; '.join(errors))


async def process_domain(domain: str, dns_timeout: float = 4, http_timeout: float = 5) -> dict:
    result = {'domain': domain, 'status': DomainStatus.FAILED, 'ip_addresses': [], 'dns': {}}
    try:
        normalize_domain(domain)
    except ValueError as exc:
        result['error'] = str(exc)
        return result

    try:
        records = await asyncio.wait_for(lookup_dns(domain, dns_timeout), timeout=dns_timeout + 1)
        result['dns'] = records
        result['ip_addresses'] = records['A']
        if not records['A']:
            result['error'] = 'DNS A record not found or DNS lookup failed'
            return result
        if not public_ips(records['A']) or (records['AAAA'] and not public_ips(records['AAAA'])):
            result['error'] = 'Non-public IP addresses are not permitted'
            return result
        try:
            result.update(await probe_http(domain, http_timeout))
        except (RuntimeError, asyncio.TimeoutError) as exc:
            # DNS succeeded; HTTP availability is a separate observation.
            result['http_error'] = str(exc)
        result['status'] = DomainStatus.COMPLETED
    except Exception as exc:
        result['error'] = f'Unexpected processing error: {type(exc).__name__}'
    return result

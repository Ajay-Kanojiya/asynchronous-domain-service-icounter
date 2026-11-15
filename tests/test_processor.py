import asyncio
import pytest

from app import processor


@pytest.mark.parametrize('domain', [
    'https://example.com', 'example.com/path', '-bad.com', 'bad..com',
    '127.0.0.1', 'localhost', 'a b.com', 'example.com:8080',
])
def test_invalid_domain(domain):
    with pytest.raises(ValueError):
        processor.normalize_domain(domain)


def test_normalizes_internationalized_domains():
    assert processor.normalize_domain(' EXAMPLE.COM. ') == 'example.com'
    assert processor.normalize_domain('bücher.de') == 'xn--bcher-kva.de'


def test_public_ip_validation():
    assert processor.public_ips(['8.8.8.8'])
    assert not processor.public_ips(['8.8.8.8', '127.0.0.1'])
    assert not processor.public_ips(['10.1.2.3'])
    assert not processor.public_ips([])


@pytest.mark.asyncio
async def test_dns_failure_is_per_domain(monkeypatch):
    async def no_dns(*args, **kwargs):
        return {kind: [] for kind in processor.RECORD_TYPES}

    monkeypatch.setattr(processor, 'lookup_dns', no_dns)
    result = await processor.process_domain('invalid.example')
    assert result['status'] == 'FAILED'
    assert 'DNS' in result['error']


@pytest.mark.asyncio
async def test_http_failure_preserves_dns_result(monkeypatch):
    async def dns_ok(*args, **kwargs):
        return {**{k: [] for k in processor.RECORD_TYPES}, 'A': ['8.8.8.8']}

    async def http_down(*args, **kwargs):
        raise RuntimeError('HTTPS: timeout; HTTP: timeout')

    monkeypatch.setattr(processor, 'lookup_dns', dns_ok)
    monkeypatch.setattr(processor, 'probe_http', http_down)
    result = await processor.process_domain('example.com')
    assert result['status'] == 'COMPLETED'
    assert result['ip_addresses'] == ['8.8.8.8']
    assert result['http_error']


def test_title_parser():
    parser = processor.TitleParser()
    parser.feed('<html><head><title>Example &amp; Test</title></head></html>')
    assert ''.join(parser.parts) == 'Example & Test'

@pytest.mark.asyncio
async def test_dns_timeout_is_bounded(monkeypatch):
    async def slow_dns(*args, **kwargs):
        await asyncio.sleep(2)
    monkeypatch.setattr(processor, 'lookup_dns', slow_dns)
    result = await processor.process_domain('example.com', dns_timeout=0.01)
    assert result['status'] == 'FAILED'
    assert 'TimeoutError' in result['error']


@pytest.mark.asyncio
async def test_rejects_private_ipv6_even_if_ipv4_is_public(monkeypatch):
    async def mock_dns(*args, **kwargs):
        return {**{k: [] for k in processor.RECORD_TYPES}, 'A': ['8.8.8.8'], 'AAAA': ['::1']}
    monkeypatch.setattr(processor, 'lookup_dns', mock_dns)
    result = await processor.process_domain('example.com')
    assert result['status'] == 'FAILED'
    assert 'Non-public' in result['error']

"""Redis-backed job store shared by every API instance and Celery worker.

Keys (both expire after JOB_TTL_SECONDS):
  job:{id}          hash  created_at, domains (JSON list, submission order)
  job:{id}:results  hash  domain -> JSON result (in_progress / completed / failed)
A domain with no entry in the results hash is still pending.
"""
import json
import os
import uuid
from datetime import datetime, timezone

import redis
import redis.asyncio as aioredis
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from .schemas import DomainStatus, JobStatus

JOB_TTL_SECONDS = int(os.getenv('JOB_TTL_SECONDS', '7200'))


def meta_key(job_id: str) -> str:
    return f'job:{job_id}'


def results_key(job_id: str) -> str:
    return f'job:{job_id}:results'


def job_status(statuses: list[DomainStatus]) -> JobStatus:
    if all(s.is_finished for s in statuses):
        return JobStatus.COMPLETED_WITH_ERRORS if DomainStatus.FAILED in statuses else JobStatus.COMPLETED
    if all(s == DomainStatus.PENDING for s in statuses):
        return JobStatus.PENDING
    return JobStatus.IN_PROGRESS


class JobStore:
    """Async store used by the API."""

    def __init__(self, client: aioredis.Redis):
        self.redis = client

    @classmethod
    def from_url(cls, url: str) -> 'JobStore':
        return cls(aioredis.from_url(
            url, decode_responses=True, retry=AsyncRetry(ExponentialBackoff(), 3),
            retry_on_error=[redis.ConnectionError, redis.TimeoutError]))

    async def close(self):
        await self.redis.aclose()

    async def ready(self) -> bool:
        return await self.redis.ping()

    async def create_job(self, domains: list[str]) -> str:
        job_id = str(uuid.uuid4())
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(meta_key(job_id), mapping={
                'created_at': datetime.now(timezone.utc).isoformat(),
                'domains': json.dumps(domains),
            })
            pipe.expire(meta_key(job_id), JOB_TTL_SECONDS)
            await pipe.execute()
        return job_id

    async def delete_job(self, job_id: str):
        await self.redis.delete(meta_key(job_id), results_key(job_id))

    async def get_job(self, job_id: str) -> dict | None:
        try:
            job_id = str(uuid.UUID(job_id))
        except ValueError:
            return None
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hgetall(meta_key(job_id))
            pipe.hgetall(results_key(job_id))
            meta, stored = await pipe.execute()
        if not meta:
            return None
        results = [
            json.loads(stored[d]) if d in stored else {'domain': d, 'status': DomainStatus.PENDING}
            for d in json.loads(meta['domains'])
        ]
        statuses = [DomainStatus(r['status']) for r in results]
        return {
            'job_id': job_id,
            'status': job_status(statuses),
            'created_at': meta['created_at'],
            'total': len(results),
            'processed': sum(s.is_finished for s in statuses),
            'results': results,
        }


# Sync helpers used by Celery worker threads.

def sync_client(url: str) -> redis.Redis:
    return redis.Redis.from_url(
        url, decode_responses=True, retry=Retry(ExponentialBackoff(), 3),
        retry_on_error=[redis.ConnectionError, redis.TimeoutError])


def mark_in_progress(client: redis.Redis, job_id: str, domain: str):
    # HSETNX: a redelivered task never moves an already finished domain back to in_progress.
    with client.pipeline(transaction=True) as pipe:
        pipe.hsetnx(results_key(job_id), domain, json.dumps({'domain': domain, 'status': DomainStatus.IN_PROGRESS}))
        pipe.expire(results_key(job_id), JOB_TTL_SECONDS)
        pipe.execute()


def save_result(client: redis.Redis, job_id: str, result: dict):
    # Plain overwrite keyed by domain, so running the same task twice is harmless (idempotent).
    with client.pipeline(transaction=True) as pipe:
        pipe.hset(results_key(job_id), result['domain'], json.dumps(result))
        pipe.expire(results_key(job_id), JOB_TTL_SECONDS)
        pipe.execute()

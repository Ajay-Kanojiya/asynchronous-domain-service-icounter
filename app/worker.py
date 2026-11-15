"""Celery worker: one task per domain, so each domain is processed (and fails) independently.

Run: celery -A app.worker worker --pool=threads --concurrency=10 --loglevel=INFO
"""
import asyncio
import logging
import os

import redis
from celery import Celery

from . import storage
from .processor import process_domain
from .schemas import DomainStatus

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
TASK_DEADLINE_SECONDS = float(os.getenv('TASK_DEADLINE_SECONDS', '20'))

celery_app = Celery('domain_service', broker=REDIS_URL)
celery_app.conf.update(
    # Ack only after the task finishes: if a worker dies mid-task, Redis redelivers it
    # once the visibility timeout passes (at-least-once delivery).
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_transport_options={'visibility_timeout': int(os.getenv('VISIBILITY_TIMEOUT_SECONDS', '300'))},
    task_ignore_result=True,  # results live in our own job store, not the Celery result backend
    broker_connection_retry_on_startup=True,
)

log = logging.getLogger(__name__)
_client: redis.Redis | None = None


def redis_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = storage.sync_client(REDIS_URL)
    return _client


async def run_with_deadline(domain: str) -> dict:
    async with asyncio.timeout(TASK_DEADLINE_SECONDS):
        return await process_domain(domain)


@celery_app.task(name='process_domain', autoretry_for=(redis.RedisError,),
                 retry_backoff=True, max_retries=5)
def process_domain_task(job_id: str, domain: str):
    client = redis_client()
    storage.mark_in_progress(client, job_id, domain)
    try:
        # Celery tasks are synchronous; each task runs the async probe in its own event loop.
        result = asyncio.run(run_with_deadline(domain))
    except TimeoutError:
        result = {'domain': domain, 'status': DomainStatus.FAILED, 'error': 'Processing deadline exceeded'}
    except Exception as exc:
        log.exception('Processing %s crashed', domain)
        result = {'domain': domain, 'status': DomainStatus.FAILED, 'error': f'Internal error: {type(exc).__name__}'}
    storage.save_result(client, job_id, result)


def enqueue_job(job_id: str, domains: list[str]):
    for domain in domains:
        process_domain_task.delay(job_id, domain)

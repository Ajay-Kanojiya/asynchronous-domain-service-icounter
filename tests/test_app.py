import asyncio
import json

import httpx
import pytest

from app import main, storage, worker


@pytest.fixture
async def client(store):
    queued = []
    app = main.create_app(store_factory=lambda: store, enqueue=lambda j, d: queued.append((j, d)))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
            c.queued = queued
            yield c


def fake_processor(monkeypatch, failing=()):
    async def process(domain):
        if domain in failing:
            return {'domain': domain, 'status': 'FAILED', 'error': 'DNS resolution failed'}
        return {'domain': domain, 'status': 'COMPLETED', 'http_status': 200}
    monkeypatch.setattr(worker, 'process_domain', process)


async def run_task(job_id, domain):
    # The Celery task calls asyncio.run(), so execute it in a thread like a real worker would.
    await asyncio.to_thread(worker.process_domain_task, job_id, domain)


def test_dedup():
    assert main.sanitize_domains([' Example.COM ', 'example.com.', 'bad domain']) == ['example.com', 'bad domain']


@pytest.mark.asyncio
async def test_submit_returns_immediately_and_enqueues_unique_domains(client):
    response = await client.post('/jobs', json={'domains': ['Example.COM', 'example.com', 'google.com']})
    assert response.status_code == 202
    job_id = response.json()['job_id']
    assert response.json()['status'] == 'PENDING'
    assert response.json()['total'] == 2
    assert client.queued == [(job_id, ['example.com', 'google.com'])]

    job = (await client.get(f'/jobs/{job_id}')).json()
    assert job['status'] == 'PENDING'
    assert job['total'] == 2 and job['processed'] == 0


@pytest.mark.asyncio
async def test_partial_failure_end_to_end(client, sync_redis, monkeypatch):
    fake_processor(monkeypatch, failing={'invalid.example'})
    job_id = (await client.post('/jobs', json={'domains': ['example.com', 'invalid.example']})).json()['job_id']

    await run_task(job_id, 'example.com')
    job = (await client.get(f'/jobs/{job_id}')).json()
    assert job['status'] == 'IN_PROGRESS' and job['processed'] == 1

    await run_task(job_id, 'invalid.example')
    job = (await client.get(f'/jobs/{job_id}')).json()
    assert job['status'] == 'COMPLETED_WITH_ERRORS'
    assert [r['status'] for r in job['results']] == ['COMPLETED', 'FAILED']  # submission order kept


@pytest.mark.asyncio
async def test_redelivered_task_does_not_regress_finished_result(client, sync_redis, monkeypatch):
    fake_processor(monkeypatch)
    job_id = (await client.post('/jobs', json={'domains': ['example.com']})).json()['job_id']
    await run_task(job_id, 'example.com')
    # Simulates Celery redelivering the same message after a lost ack.
    storage.mark_in_progress(sync_redis, job_id, 'example.com')
    assert (await client.get(f'/jobs/{job_id}')).json()['status'] == 'COMPLETED'


@pytest.mark.asyncio
async def test_task_deadline_marks_domain_failed(client, sync_redis, monkeypatch):
    async def hung(domain):
        await asyncio.sleep(5)
    monkeypatch.setattr(worker, 'process_domain', hung)
    monkeypatch.setattr(worker, 'TASK_DEADLINE_SECONDS', 0.01)
    job_id = (await client.post('/jobs', json={'domains': ['example.com']})).json()['job_id']
    await run_task(job_id, 'example.com')
    result = (await client.get(f'/jobs/{job_id}')).json()['results'][0]
    assert result == {'domain': 'example.com', 'status': 'FAILED', 'error': 'Processing deadline exceeded'}


@pytest.mark.asyncio
async def test_unexpected_crash_marks_domain_failed(client, sync_redis, monkeypatch):
    async def crash(domain):
        raise KeyError('boom')
    monkeypatch.setattr(worker, 'process_domain', crash)
    job_id = (await client.post('/jobs', json={'domains': ['example.com']})).json()['job_id']
    await run_task(job_id, 'example.com')
    result = (await client.get(f'/jobs/{job_id}')).json()['results'][0]
    assert result['status'] == 'FAILED' and 'KeyError' in result['error']


@pytest.mark.asyncio
async def test_queue_outage_returns_503_and_leaves_no_job(store, server):
    def broken(job_id, domains):
        raise ConnectionError('broker down')
    app = main.create_app(store_factory=lambda: store, enqueue=broken)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
            assert (await c.post('/jobs', json={'domains': ['example.com']})).status_code == 503
    assert await store.redis.keys('job:*') == []


@pytest.mark.asyncio
async def test_validation_and_not_found(client):
    assert (await client.post('/jobs', json={'domains': []})).status_code == 422
    assert (await client.post('/jobs', json={'domains': ['a.com'] * 51})).status_code == 422
    assert (await client.get('/jobs/not-a-uuid')).status_code == 422  # path param validated as UUID
    assert (await client.get('/jobs/00000000-0000-0000-0000-000000000000')).status_code == 404
    assert (await client.get('/readyz')).status_code == 200


@pytest.mark.asyncio
async def test_jobs_expire(client, store):
    job_id = (await client.post('/jobs', json={'domains': ['example.com']})).json()['job_id']
    assert 0 < await store.redis.ttl(storage.meta_key(job_id)) <= storage.JOB_TTL_SECONDS
    assert json.loads(await store.redis.hget(storage.meta_key(job_id), 'domains')) == ['example.com']


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [
    {'domains': ['example.com', '   ']},           # blank item
    {'domains': ['x' * 254]},                      # longer than any valid hostname
    {'domains': 'example.com'},                    # not a list
    {'domains': ['example.com'], 'extra': True},   # unknown field
    {},
])
async def test_request_schema_rejects_bad_payloads(client, body):
    assert (await client.post('/jobs', json=body)).status_code == 422


@pytest.mark.asyncio
async def test_response_schema(client, sync_redis, monkeypatch):
    fake_processor(monkeypatch)
    job_id = (await client.post('/jobs', json={'domains': ['example.com', 'google.com']})).json()['job_id']
    await run_task(job_id, 'example.com')
    job = (await client.get(f'/jobs/{job_id}')).json()
    assert set(job) == {'job_id', 'status', 'created_at', 'total', 'processed', 'results'}
    assert job['results'][0] == {'domain': 'example.com', 'status': 'COMPLETED', 'http_status': 200}
    assert job['results'][1] == {'domain': 'google.com', 'status': 'PENDING'}  # None fields omitted
    assert (await client.get('/healthz')).json() == {'status': 'OK'}


def test_job_status_enum_derivation():
    from app.schemas import DomainStatus as D, JobStatus as J
    from app.storage import job_status
    assert job_status([D.PENDING, D.PENDING]) is J.PENDING
    assert job_status([D.COMPLETED, D.PENDING]) is J.IN_PROGRESS
    assert job_status([D.IN_PROGRESS]) is J.IN_PROGRESS
    assert job_status([D.COMPLETED, D.COMPLETED]) is J.COMPLETED
    assert job_status([D.COMPLETED, D.FAILED]) is J.COMPLETED_WITH_ERRORS

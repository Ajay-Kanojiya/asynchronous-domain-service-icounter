"""HTTP API: stores the job in Redis, enqueues one Celery task per domain, returns 202."""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import RedirectResponse

from .processor import normalize_domain
from .schemas import (ErrorResponse, HealthResponse, HealthStatus, JobCreatedResponse,
                      JobCreateRequest, JobResponse, JobStatus)
from .storage import JobStore
from .worker import enqueue_job

log = logging.getLogger(__name__)


def sanitize_domains(domains: list[str]) -> list[str]:
    """Normalize and deduplicate within one job; keep invalid values as failed tasks."""
    output = []
    for value in domains:
        try:
            name = normalize_domain(value)
        except ValueError:
            name = value.strip().lower()
        if name not in output:
            output.append(name)
    return output


def create_app(store_factory=None, enqueue=enqueue_job) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = store_factory() if store_factory else JobStore.from_url(
            os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
        app.state.store = store
        try:
            yield
        finally:
            await store.close()

    app = FastAPI(
        title='Domain Processing API',
        version='1.0.0',
        description=(
            'Submit domains for asynchronous processing (DNS records, IPs, HTTP status, '
            'page title, response time) and poll for results.\n\n'
            '1. `POST /jobs` returns a `job_id` immediately.\n'
            '2. `GET /jobs/{job_id}` returns per-domain results as they complete.'
        ),
        openapi_tags=[
            {'name': 'jobs', 'description': 'Submit jobs and fetch results'},
            {'name': 'health', 'description': 'Liveness and readiness probes'},
        ],
        docs_url='/docs',
        redoc_url='/redoc',
        lifespan=lifespan,
    )

    @app.get('/', include_in_schema=False)
    async def root():
        return RedirectResponse('/docs')

    @app.post('/jobs', status_code=status.HTTP_202_ACCEPTED, response_model=JobCreatedResponse,
              tags=['jobs'], summary='Submit a processing job',
              responses={503: {'model': ErrorResponse, 'description': 'Queue unavailable, retry later'}})
    async def submit(request: JobCreateRequest) -> JobCreatedResponse:
        domains = sanitize_domains(request.domains)
        job_id = await app.state.store.create_job(domains)
        try:
            # Celery's publish call is blocking, so keep it off the event loop.
            await asyncio.to_thread(enqueue, job_id, domains)
        except Exception:
            log.exception('Enqueue failed for job %s', job_id)
            await app.state.store.delete_job(job_id)
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, 'Queue unavailable, please retry')
        return JobCreatedResponse(job_id=job_id, status=JobStatus.PENDING, total=len(domains))

    # exclude_none: pending/failed domains only show the fields that apply to them.
    @app.get('/jobs/{job_id}', response_model=JobResponse, response_model_exclude_none=True,
             tags=['jobs'], summary='Get job status and results',
             responses={404: {'model': ErrorResponse, 'description': 'Job not found'}})
    async def retrieve(job_id: UUID) -> JobResponse:
        job = await app.state.store.get_job(str(job_id))
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, 'Job not found')
        return JobResponse.model_validate(job)

    @app.get('/healthz', response_model=HealthResponse, tags=['health'], summary='Liveness probe')
    async def live() -> HealthResponse:
        return HealthResponse(status=HealthStatus.OK)

    @app.get('/readyz', response_model=HealthResponse, tags=['health'],
             summary='Readiness probe (checks Redis)',
             responses={503: {'model': ErrorResponse, 'description': 'Redis unavailable'}})
    async def ready() -> HealthResponse:
        try:
            await app.state.store.ready()
        except Exception:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, 'Redis unavailable')
        return HealthResponse(status=HealthStatus.READY)

    return app


app = create_app()

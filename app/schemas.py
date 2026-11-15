"""API contract: Pydantic request/response models and the enums shared across the service."""
from datetime import datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

MAX_DOMAINS_PER_JOB = 50


class JobStatus(StrEnum):
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'
    COMPLETED_WITH_ERRORS = 'COMPLETED_WITH_ERRORS'


class DomainStatus(StrEnum):
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'

    @property
    def is_finished(self) -> bool:
        return self in (DomainStatus.COMPLETED, DomainStatus.FAILED)


class DnsRecordType(StrEnum):
    A = 'A'
    AAAA = 'AAAA'
    TXT = 'TXT'
    CNAME = 'CNAME'
    MX = 'MX'
    NS = 'NS'


class HttpScheme(StrEnum):
    HTTPS = 'https'
    HTTP = 'http'


class HealthStatus(StrEnum):
    OK = 'OK'
    READY = 'READY'


# Each item is trimmed; empty or absurdly long strings are rejected with 422.
# Malformed-but-reasonable names are accepted and reported as failed results.
DomainInput = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=253)]


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    domains: list[DomainInput] = Field(
        min_length=1,
        max_length=MAX_DOMAINS_PER_JOB,
        description=f'1-{MAX_DOMAINS_PER_JOB} domain names. Duplicates (case-insensitive) are processed once.',
        examples=[['example.com', 'google.com', 'invalid.example']],
    )


class JobCreatedResponse(BaseModel):
    job_id: UUID
    status: JobStatus = Field(examples=[JobStatus.PENDING])
    total: int = Field(description='Unique domains queued after normalization and de-duplication')


class DnsRecords(BaseModel):
    A: list[str] = []
    AAAA: list[str] = []
    TXT: list[str] = []
    CNAME: list[str] = []
    MX: list[str] = []
    NS: list[str] = []


class DomainResult(BaseModel):
    domain: str
    status: DomainStatus
    ip_addresses: list[str] | None = None
    dns: DnsRecords | None = None
    scheme: HttpScheme | None = Field(None, description='Scheme that answered (HTTPS tried first)')
    http_status: int | None = None
    title: str | None = None
    response_time_ms: float | None = None
    error: str | None = Field(None, description='Why the domain failed')
    http_error: str | None = Field(None, description='DNS succeeded but the web server was unreachable')


class JobResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    created_at: datetime
    total: int
    processed: int = Field(description='Domains that are completed or failed')
    results: list[DomainResult]


class HealthResponse(BaseModel):
    status: HealthStatus


class ErrorResponse(BaseModel):
    detail: str

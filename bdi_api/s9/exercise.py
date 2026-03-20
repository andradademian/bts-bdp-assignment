from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


# Mock data

PIPELINE_RUNS = [
    PipelineRun(
        id="run-001",
        repository="bts-bdp-assignment",
        branch="main",
        status="success",
        triggered_by="push",
        started_at=datetime(2026, 3, 10, 10, 0, 0),
        finished_at=datetime(2026, 3, 10, 10, 5, 30),
        stages=["lint", "test", "build"],
    ),
    PipelineRun(
        id="run-002",
        repository="bts-bdp-assignment",
        branch="feat/add-info-endpoint",
        status="failure",
        triggered_by="pull_request",
        started_at=datetime(2026, 3, 11, 9, 0, 0),
        finished_at=datetime(2026, 3, 11, 9, 3, 0),
        stages=["lint", "test"],
    ),
    PipelineRun(
        id="run-003",
        repository="bts-bdp-assignment",
        branch="main",
        status="running",
        triggered_by="schedule",
        started_at=datetime(2026, 3, 12, 8, 0, 0),
        finished_at=None,
        stages=["lint", "test", "build"],
    ),
    PipelineRun(
        id="run-004",
        repository="bts-bdp-assignment",
        branch="main",
        status="pending",
        triggered_by="manual",
        started_at=datetime(2026, 3, 13, 7, 0, 0),
        finished_at=None,
        stages=["lint", "test", "build", "deploy"],
    ),
]

PIPELINE_STAGES = {
    "run-001": [
        PipelineStage(
            name="lint",
            status="success",
            started_at=datetime(2026, 3, 10, 10, 0, 0),
            finished_at=datetime(2026, 3, 10, 10, 0, 45),
            logs_url="/api/s9/pipelines/run-001/stages/lint/logs",
        ),
        PipelineStage(
            name="test",
            status="success",
            started_at=datetime(2026, 3, 10, 10, 0, 45),
            finished_at=datetime(2026, 3, 10, 10, 3, 20),
            logs_url="/api/s9/pipelines/run-001/stages/test/logs",
        ),
        PipelineStage(
            name="build",
            status="success",
            started_at=datetime(2026, 3, 10, 10, 3, 20),
            finished_at=datetime(2026, 3, 10, 10, 5, 30),
            logs_url="/api/s9/pipelines/run-001/stages/build/logs",
        ),
    ],
    "run-002": [
        PipelineStage(
            name="lint",
            status="success",
            started_at=datetime(2026, 3, 11, 9, 0, 0),
            finished_at=datetime(2026, 3, 11, 9, 0, 45),
            logs_url="/api/s9/pipelines/run-002/stages/lint/logs",
        ),
        PipelineStage(
            name="test",
            status="failure",
            started_at=datetime(2026, 3, 11, 9, 0, 45),
            finished_at=datetime(2026, 3, 11, 9, 3, 0),
            logs_url="/api/s9/pipelines/run-002/stages/test/logs",
        ),
    ],
    "run-003": [
        PipelineStage(
            name="lint",
            status="success",
            started_at=datetime(2026, 3, 12, 8, 0, 0),
            finished_at=datetime(2026, 3, 12, 8, 0, 45),
            logs_url="/api/s9/pipelines/run-003/stages/lint/logs",
        ),
        PipelineStage(
            name="test",
            status="success",
            started_at=datetime(2026, 3, 12, 8, 0, 45),
            finished_at=datetime(2026, 3, 12, 8, 3, 0),
            logs_url="/api/s9/pipelines/run-003/stages/test/logs",
        ),
        PipelineStage(
            name="build",
            status="running",
            started_at=datetime(2026, 3, 12, 8, 3, 0),
            finished_at=None,
            logs_url="/api/s9/pipelines/run-003/stages/build/logs",
        ),
    ],
    "run-004": [
        PipelineStage(
            name="lint",
            status="success",
            started_at=datetime(2026, 3, 13, 7, 0, 0),
            finished_at=datetime(2026, 3, 13, 7, 0, 45),
            logs_url="/api/s9/pipelines/run-004/stages/lint/logs",
        ),
        PipelineStage(
            name="test",
            status="success",
            started_at=datetime(2026, 3, 13, 7, 0, 45),
            finished_at=datetime(2026, 3, 13, 7, 3, 0),
            logs_url="/api/s9/pipelines/run-004/stages/test/logs",
        ),
        PipelineStage(
            name="build",
            status="success",
            started_at=datetime(2026, 3, 13, 7, 3, 0),
            finished_at=datetime(2026, 3, 13, 7, 5, 0),
            logs_url="/api/s9/pipelines/run-004/stages/build/logs",
        ),
        PipelineStage(
            name="deploy",
            status="pending",
            started_at=datetime(2026, 3, 13, 7, 5, 0),
            finished_at=None,
            logs_url="/api/s9/pipelines/run-004/stages/deploy/logs",
        ),
    ],
}


# Endpoints

@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    results = PIPELINE_RUNS.copy()

    if repository:
        results = [r for r in results if r.repository == repository]

    if status_filter:
        results = [r for r in results if r.status == status_filter]

    results.sort(key=lambda r: r.started_at, reverse=True)

    start = page * num_results
    end = start + num_results

    return results[start:end]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    pipeline_ids = [r.id for r in PIPELINE_RUNS]
    if pipeline_id not in pipeline_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_id}' not found",
        )

    return PIPELINE_STAGES.get(pipeline_id, [])

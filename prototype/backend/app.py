from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from backend.db import db_session, engine
from backend.dsl import PlanDSL
from backend.models import Base, DatasetVersion
from backend.plan_store import PlanConflict, create_plan, get_plan, undo, update_plan
from backend.query_engine import DSLValidationError, run_plan
from backend.settings import settings
from backend.whitelist import ALLOWED_COLUMNS


app = FastAPI(title="Hackathon budget selection prototype")


class QueryRequest(BaseModel):
    plan: PlanDSL
    dataset_version: Optional[str] = None
    save_plan: bool = True


class QueryResponse(BaseModel):
    dataset_version: str
    plan_id: Optional[str] = None
    plan_version: Optional[int] = None
    columns: list[str]
    rows: list[dict[str, Any]]


class PlanUpdateRequest(BaseModel):
    plan_id: str
    plan_version: int
    plan: PlanDSL
    dataset_version: Optional[str] = None


class PlanUpdateResponse(QueryResponse):
    pass


class PlanUndoRequest(BaseModel):
    plan_id: str
    plan_version: int


@app.on_event("startup")
def _startup():
    Base.metadata.create_all(bind=engine)


def _latest_dataset_version() -> str:
    with db_session() as s:
        dv = s.scalar(select(DatasetVersion).order_by(DatasetVersion.loaded_at.desc()))
        if dv is None:
            raise HTTPException(status_code=503, detail="dataset not loaded yet; run ETL first")
        return dv.dataset_version


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/api/meta")
def meta():
    return {
        "sources": list(ALLOWED_COLUMNS.keys()),
        "columns": {
            src: sorted(cols.keys())
            for src, cols in ALLOWED_COLUMNS.items()
        },
        "limits": {
            "max_limit": settings.max_limit,
            "default_limit": settings.default_limit,
            "max_filter_depth": settings.max_filter_depth,
        },
    }


@app.post("/api/query", response_model=QueryResponse)
def api_query(req: QueryRequest):
    dataset_version = req.dataset_version or _latest_dataset_version()
    with db_session() as s:
        try:
            result = run_plan(s, req.plan, dataset_version)
        except DSLValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))

        plan_id = None
        plan_version = None
        if req.save_plan:
            p = create_plan(s, dataset_version=dataset_version, plan_obj=req.plan.model_dump())
            plan_id = p.plan_id
            plan_version = p.plan_version

        return QueryResponse(
            dataset_version=dataset_version,
            plan_id=plan_id,
            plan_version=plan_version,
            columns=result["columns"],
            rows=result["rows"],
        )


@app.post("/api/plan/update", response_model=PlanUpdateResponse)
def api_plan_update(req: PlanUpdateRequest):
    dataset_version = req.dataset_version or _latest_dataset_version()
    with db_session() as s:
        try:
            result = run_plan(s, req.plan, dataset_version)
        except DSLValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        try:
            p = update_plan(
                s,
                plan_id=req.plan_id,
                expected_version=req.plan_version,
                new_plan_obj=req.plan.model_dump(),
            )
        except PlanConflict:
            raise HTTPException(status_code=409, detail="plan_version conflict")
        except KeyError:
            raise HTTPException(status_code=404, detail="plan not found")

        return PlanUpdateResponse(
            dataset_version=dataset_version,
            plan_id=p.plan_id,
            plan_version=p.plan_version,
            columns=result["columns"],
            rows=result["rows"],
        )


@app.post("/api/plan/undo", response_model=PlanUpdateResponse)
def api_plan_undo(req: PlanUndoRequest):
    with db_session() as s:
        p0 = get_plan(s, req.plan_id)
        if p0 is None:
            raise HTTPException(status_code=404, detail="plan not found")
        try:
            p = undo(s, plan_id=req.plan_id, expected_version=req.plan_version)
        except PlanConflict:
            raise HTTPException(status_code=409, detail="plan_version conflict")

        plan_obj = PlanDSL.model_validate(json.loads(p.current_plan_json))
        dataset_version = p.dataset_version
        try:
            result = run_plan(s, plan_obj, dataset_version)
        except DSLValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))

        return PlanUpdateResponse(
            dataset_version=dataset_version,
            plan_id=p.plan_id,
            plan_version=p.plan_version,
            columns=result["columns"],
            rows=result["rows"],
        )


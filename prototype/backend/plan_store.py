from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Plan


class PlanConflict(Exception):
    pass


def create_plan(session: Session, dataset_version: str, plan_obj: dict[str, Any]) -> Plan:
    plan_id = uuid.uuid4().hex
    p = Plan(
        plan_id=plan_id,
        plan_version=1,
        dataset_version=dataset_version,
        current_plan_json=json.dumps(plan_obj, ensure_ascii=False),
        history_json=json.dumps([plan_obj], ensure_ascii=False),
        created_at=dt.datetime.utcnow(),
        updated_at=dt.datetime.utcnow(),
    )
    session.add(p)
    session.flush()
    return p


def get_plan(session: Session, plan_id: str) -> Optional[Plan]:
    return session.scalar(select(Plan).where(Plan.plan_id == plan_id))


def update_plan(
    session: Session,
    plan_id: str,
    expected_version: int,
    new_plan_obj: dict[str, Any],
    history_keep: int = 10,
) -> Plan:
    p = get_plan(session, plan_id)
    if p is None:
        raise KeyError("plan not found")
    if p.plan_version != expected_version:
        raise PlanConflict("version conflict")
    history = json.loads(p.history_json or "[]")
    history.append(new_plan_obj)
    if len(history) > history_keep:
        history = history[-history_keep:]
    p.plan_version = p.plan_version + 1
    p.current_plan_json = json.dumps(new_plan_obj, ensure_ascii=False)
    p.history_json = json.dumps(history, ensure_ascii=False)
    p.updated_at = dt.datetime.utcnow()
    session.add(p)
    session.flush()
    return p


def undo(session: Session, plan_id: str, expected_version: int, history_keep: int = 10) -> Plan:
    p = get_plan(session, plan_id)
    if p is None:
        raise KeyError("plan not found")
    if p.plan_version != expected_version:
        raise PlanConflict("version conflict")
    history = json.loads(p.history_json or "[]")
    if len(history) <= 1:
        return p
    history.pop()  # remove current
    new_current = history[-1]
    p.plan_version = p.plan_version + 1
    p.current_plan_json = json.dumps(new_current, ensure_ascii=False)
    p.history_json = json.dumps(history[-history_keep:], ensure_ascii=False)
    p.updated_at = dt.datetime.utcnow()
    session.add(p)
    session.flush()
    return p


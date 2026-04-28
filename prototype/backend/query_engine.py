from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from backend.dsl import FilterAtom, FilterTree, PlanDSL
from backend.settings import settings
from backend.whitelist import ALLOWED_COLUMNS, RELATIONS, SOURCE_TABLES


class DSLValidationError(ValueError):
    pass


@dataclass
class CompiledQuery:
    sql: str
    params: dict[str, Any]
    out_columns: list[str]


def _parse_field(field: str, alias_to_source: dict[str, str]) -> tuple[str, str]:
    if "." not in field:
        raise DSLValidationError(f"Field must be qualified with alias: {field}")
    alias, col = field.split(".", 1)
    if alias not in alias_to_source:
        raise DSLValidationError(f"Unknown alias in field: {alias}")
    source = alias_to_source[alias]
    allowed = ALLOWED_COLUMNS[source]
    if col not in allowed:
        raise DSLValidationError(f"Column not allowed: {source}.{col}")
    expr = allowed[col].sql.replace("{a}", alias)
    return expr, f"{alias}.{allowed[col].out}"


def _compile_filter(
    node: FilterTree,
    alias_to_source: dict[str, str],
    params: dict[str, Any],
    depth: int,
) -> str:
    if depth > settings.max_filter_depth:
        raise DSLValidationError("filter_tree too deep")

    if isinstance(node, FilterAtom):
        expr, _ = _parse_field(node.field, alias_to_source)
        key = f"p{len(params) + 1}"
        cmp_ = node.cmp
        if cmp_ in {"=", "!=", ">", ">=", "<", "<="}:
            params[key] = node.value
            return f"({expr} {cmp_} :{key})"
        if cmp_ == "ilike":
            params[key] = str(node.value)
            return f"({expr} ILIKE :{key})"
        if cmp_ == "in":
            if not isinstance(node.value, list):
                raise DSLValidationError("IN expects list")
            if len(node.value) == 0:
                return "(1=0)"
            keys = []
            for v in node.value[:1000]:
                k = f"p{len(params) + 1}"
                params[k] = v
                keys.append(f":{k}")
            return f"({expr} IN ({', '.join(keys)}))"
        if cmp_ == "between":
            if not (isinstance(node.value, list) and len(node.value) == 2):
                raise DSLValidationError("BETWEEN expects [a,b]")
            k1 = f"p{len(params) + 1}"
            params[k1] = node.value[0]
            k2 = f"p{len(params) + 1}"
            params[k2] = node.value[1]
            return f"({expr} BETWEEN :{k1} AND :{k2})"
        raise DSLValidationError(f"Unsupported cmp: {cmp_}")

    op = getattr(node, "op", None)
    if op == "and":
        parts = [_compile_filter(x, alias_to_source, params, depth + 1) for x in node.items]
        if not parts:
            return "(1=1)"
        return "(" + " AND ".join(parts) + ")"
    if op == "or":
        parts = [_compile_filter(x, alias_to_source, params, depth + 1) for x in node.items]
        if not parts:
            return "(1=0)"
        return "(" + " OR ".join(parts) + ")"
    if op == "not":
        inner = _compile_filter(node.item, alias_to_source, params, depth + 1)
        return f"(NOT {inner})"

    raise DSLValidationError("Unknown filter node")


def compile_plan(plan: PlanDSL, dataset_version: str) -> CompiledQuery:
    if plan.limit < 0 or plan.offset < 0:
        raise DSLValidationError("limit/offset must be >= 0")
    limit = min(plan.limit or settings.default_limit, settings.max_limit)

    alias_to_source: dict[str, str] = {}
    for src in plan.sources:
        if src.alias in alias_to_source:
            raise DSLValidationError(f"Duplicate alias: {src.alias}")
        alias_to_source[src.alias] = src.name

    if not plan.sources:
        raise DSLValidationError("At least one source is required")

    base = plan.sources[0]
    base_table = SOURCE_TABLES[base.name]
    from_sql = f"FROM {base_table} {base.alias}"

    join_alias_state: dict[str, str] = {}
    join_sql_parts: list[str] = []
    for j in plan.joins:
        if j.left not in alias_to_source or j.right not in alias_to_source:
            raise DSLValidationError("join aliases must refer to declared sources")
        left_src = alias_to_source[j.left]
        right_src = alias_to_source[j.right]
        # basic sanity: relation must match declared right table (we still build template explicitly)
        if j.relation not in RELATIONS:
            raise DSLValidationError(f"Unknown relation: {j.relation}")
        join_sql_parts.extend(RELATIONS[j.relation](j.left, j.right, join_alias_state))

    select_exprs: list[str] = []
    out_cols: list[str] = []
    for col in plan.columns:
        expr, out = _parse_field(col, alias_to_source)
        select_exprs.append(f"{expr} AS \"{out}\"")
        out_cols.append(out)

    if not select_exprs:
        raise DSLValidationError("columns must not be empty")

    where_parts: list[str] = []
    params: dict[str, Any] = {}

    # dataset_version safety filter for each declared source alias
    for alias, source in alias_to_source.items():
        table = SOURCE_TABLES[source]
        # all our tables have dataset_version column
        where_parts.append(f"{alias}.dataset_version = :ds")
    params["ds"] = dataset_version

    if plan.filter_tree is not None:
        where_parts.append(_compile_filter(plan.filter_tree, alias_to_source, params, 0))

    order_by = ""
    for op in plan.operations:
        if op.type == "sort":
            expr, _ = _parse_field(op.by, alias_to_source)
            direction = "DESC" if op.direction == "desc" else "ASC"
            order_by = f"ORDER BY {expr} {direction}"
            break

    sql = "SELECT " + ", ".join(select_exprs) + " " + from_sql + " "
    if join_sql_parts:
        sql += " " + " ".join(join_sql_parts) + " "
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts) + " "
    if order_by:
        sql += order_by + " "
    sql += "LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = plan.offset

    return CompiledQuery(sql=sql, params=params, out_columns=out_cols)


def run_plan(session: Session, plan: PlanDSL, dataset_version: str) -> dict[str, Any]:
    compiled = compile_plan(plan, dataset_version)
    res: Result = session.execute(text(compiled.sql), compiled.params)
    rows = [dict(r._mapping) for r in res.fetchall()]
    return {"columns": compiled.out_columns, "rows": rows, "sql": compiled.sql}


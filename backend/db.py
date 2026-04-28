"""
Read-only подключение к SQLite + безопасное выполнение планов от GigaChat.
"""
import sqlite3
import os
import re
from typing import Any

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

WHITELIST_TABLES = {
    "mart_rchb", "mart_agreements",
    "mart_gz_budgetlines", "mart_gz_contracts",
    "mart_gz_payments", "mart_buau",
}

WHITELIST_JOIN_RELATIONS = {
    "gz_budgetlines_to_contracts_by_con_document_id": (
        "mart_gz_budgetlines", "mart_gz_contracts", "con_document_id"
    ),
    "gz_contracts_to_payments_by_con_document_id": (
        "mart_gz_contracts", "mart_gz_payments", "con_document_id"
    ),
    "rchb_to_buau_by_kcsr_norm": ("mart_rchb", "mart_buau", "kcsr_norm"),
    "rchb_to_agreements_by_kcsr_norm": ("mart_rchb", "mart_agreements", "kcsr_norm"),
    "rchb_to_gz_budgetlines_by_kcsr_norm": ("mart_rchb", "mart_gz_budgetlines", "kcsr_norm"),
}


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_column_name(col: str) -> str:
    """Проверить что имя колонки содержит только безопасные символы."""
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", col):
        return col
    raise ValueError(f"Небезопасное имя колонки: {col!r}")


def execute_plan(plan: dict) -> tuple[list[str], list[list[Any]]]:
    """
    Выполнить JSON-план от GigaChat.
    Возвращает (headers, rows).
    """
    sources = plan.get("sources", [])
    filters = plan.get("filters", {})
    columns = plan.get("columns", [])
    joins = plan.get("joins", [])
    limit = min(int(plan.get("limit", 500)), 2000)

    # Валидация таблиц
    for t in sources:
        if t not in WHITELIST_TABLES:
            raise ValueError(f"Таблица не в whitelist: {t!r}")

    if not sources:
        raise ValueError("sources пустой")

    primary = sources[0]

    # Собрать JOIN-ы
    joined_tables = [primary]
    join_clauses = []
    for rel in joins:
        if rel not in WHITELIST_JOIN_RELATIONS:
            continue
        left, right, key = WHITELIST_JOIN_RELATIONS[rel]
        if left in joined_tables and right not in joined_tables:
            join_clauses.append(
                f"LEFT JOIN {right} ON {left}.{key} = {right}.{key}"
            )
            joined_tables.append(right)

    # Колонки: если не указаны — берём первые 10 из primary таблицы
    if not columns:
        conn = _get_conn()
        cur = conn.execute(f"PRAGMA table_info({primary})")
        columns = [r[1] for r in cur.fetchall() if r[1] != "id"][:10]
        conn.close()

    safe_cols = []
    for c in columns:
        if "." in c:
            t, col = c.split(".", 1)
            safe_cols.append(f"{_safe_column_name(t)}.{_safe_column_name(col)}")
        else:
            safe_cols.append(_safe_column_name(c))

    select_expr = ", ".join(safe_cols) if safe_cols else f"{primary}.*"

    # Фильтры
    where_parts = []
    params: list[Any] = []

    if "kcsr_name_contains" in filters:
        where_parts.append(f"{primary}.kcsr_name LIKE ?")
        params.append(f"%{filters['kcsr_name_contains']}%")

    if "kcsr_norm_eq" in filters:
        where_parts.append(f"{primary}.kcsr_norm = ?")
        params.append(filters["kcsr_norm_eq"])

    if "budget_name_contains" in filters:
        col = "budget_name" if primary in ("mart_rchb", "mart_buau") else "caption"
        where_parts.append(f"{primary}.{col} LIKE ?")
        params.append(f"%{filters['budget_name_contains']}%")

    if "org_name_contains" in filters:
        if primary == "mart_buau":
            where_parts.append(f"mart_buau.org_name LIKE ?")
        elif primary == "mart_agreements":
            where_parts.append(f"mart_agreements.dd_recipient_caption LIKE ?")
        else:
            where_parts.append(f"{primary}.kcsr_name LIKE ?")
        params.append(f"%{filters['org_name_contains']}%")

    if "kfsr_code_eq" in filters:
        where_parts.append(f"{primary}.kfsr_code = ?")
        params.append(filters["kfsr_code_eq"])

    if "date_from" in filters:
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} >= ?")
        params.append(filters["date_from"])

    if "date_to" in filters:
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} <= ?")
        params.append(filters["date_to"])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    join_sql = " ".join(join_clauses)

    sql = f"""
        SELECT {select_expr}
        FROM {primary}
        {join_sql}
        {where_sql}
        LIMIT {limit}
    """

    conn = _get_conn()
    try:
        cur = conn.execute(sql, params)
        rows_raw = cur.fetchall()
        headers = [d[0] for d in cur.description] if cur.description else []
        rows = [[str(v) if v is not None else "" for v in row] for row in rows_raw]
    finally:
        conn.close()

    return headers, rows

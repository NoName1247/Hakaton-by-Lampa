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


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}


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
    conn = _get_conn()
    primary_columns = _table_columns(conn, primary)

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

    # Нормализация колонок из плана.
    # LLM иногда возвращает "*" или "table.*" — в этом случае используем дефолтный набор.
    normalized_columns: list[str] = []
    for c in columns:
        cs = str(c).strip()
        if not cs:
            continue
        if cs == "*" or cs.endswith(".*"):
            continue
        normalized_columns.append(cs)

    # Колонки: если не указаны/невалидны — берём первые 10 из primary таблицы
    if not normalized_columns:
        normalized_columns = [c for c in primary_columns if c != "id"][:10]

    # Схема доступных таблиц (primary + присоединенные joins)
    table_columns: dict[str, set[str]] = {primary: primary_columns}
    for jt in joined_tables:
        if jt != primary:
            table_columns[jt] = _table_columns(conn, jt)

    safe_cols = []
    for c in normalized_columns:
        try:
            if "." in c:
                t, col = c.split(".", 1)
                t = _safe_column_name(t)
                col = _safe_column_name(col)
                if t in table_columns and col in table_columns[t]:
                    safe_cols.append(f"{t}.{col}")
            else:
                col = _safe_column_name(c)
                # Сначала пробуем primary
                if col in primary_columns:
                    safe_cols.append(f"{primary}.{col}")
                else:
                    # Потом ищем в joined таблицах
                    owners = [t for t, cols in table_columns.items() if col in cols]
                    if len(owners) == 1:
                        safe_cols.append(f"{owners[0]}.{col}")
        except ValueError:
            # Пропускаем отдельные небезопасные/невалидные колонки из LLM-плана
            continue

    if not safe_cols:
        safe_cols = [f"{primary}.{c}" for c in primary_columns if c != "id"][:10]

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

    if "budget_name_contains_any" in filters:
        col = "budget_name" if primary in ("mart_rchb", "mart_buau") else "caption"
        vals = [str(v).strip() for v in filters.get("budget_name_contains_any", []) if str(v).strip()]
        if vals:
            where_parts.append("(" + " OR ".join([f"{primary}.{col} LIKE ?" for _ in vals]) + ")")
            params.extend([f"%{v}%" for v in vals])

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

    # Фильтр по месяцу через source_file (март -> март%, июнь -> июнь%)
    if "source_file_contains" in filters and "source_file" in primary_columns:
        where_parts.append(f"{primary}.source_file LIKE ?")
        params.append(f"%{filters['source_file_contains']}%")

    # Несколько месяцев/паттернов по source_file (OR)
    if "source_file_contains_any" in filters and "source_file" in primary_columns:
        vals = [str(v).strip() for v in filters.get("source_file_contains_any", []) if str(v).strip()]
        if vals:
            where_parts.append("(" + " OR ".join([f"{primary}.source_file LIKE ?" for _ in vals]) + ")")
            params.extend([f"%{v}%" for v in vals])

    # posting_date_contains: дата в формате dd.mm.yyyy — ищем по подстроке
    if "posting_date_contains" in filters and ("posting_date" in primary_columns or "close_date" in primary_columns):
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} LIKE ?")
        params.append(f"%{filters['posting_date_contains']}%")

    # posting_month: "03" или "3" → ищем .03. в дате dd.mm.yyyy
    if "posting_month" in filters and ("posting_date" in primary_columns or "close_date" in primary_columns):
        month = filters["posting_month"].zfill(2)
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} LIKE ?")
        params.append(f"%.{month}.%")

    # posting_year: "2025" → ищем .2025 в конце даты
    if "posting_year" in filters and ("posting_date" in primary_columns or "close_date" in primary_columns):
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} LIKE ?")
        params.append(f"%.{filters['posting_year']}")

    if "date_from" in filters and ("posting_date" in primary_columns or "close_date" in primary_columns):
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} >= ?")
        params.append(filters["date_from"])

    if "date_to" in filters and ("posting_date" in primary_columns or "close_date" in primary_columns):
        date_col = "posting_date" if primary in ("mart_rchb", "mart_buau") else "close_date"
        where_parts.append(f"{primary}.{date_col} <= ?")
        params.append(filters["date_to"])

    # Фильтр по % освоения для таблиц, где есть limit_amount + spend_amount
    # execution_percent = spend_amount / limit_amount * 100
    if "execution_percent_gt" in filters and ("limit_amount" in primary_columns and "spend_amount" in primary_columns):
        try:
            v = float(filters["execution_percent_gt"])
            where_parts.append(f"({primary}.limit_amount > 0 AND ({primary}.spend_amount * 100.0 / {primary}.limit_amount) > ?)")
            params.append(v)
        except (TypeError, ValueError):
            pass

    if "execution_percent_gte" in filters and ("limit_amount" in primary_columns and "spend_amount" in primary_columns):
        try:
            v = float(filters["execution_percent_gte"])
            where_parts.append(f"({primary}.limit_amount > 0 AND ({primary}.spend_amount * 100.0 / {primary}.limit_amount) >= ?)")
            params.append(v)
        except (TypeError, ValueError):
            pass

    if "execution_percent_lt" in filters and ("limit_amount" in primary_columns and "spend_amount" in primary_columns):
        try:
            v = float(filters["execution_percent_lt"])
            where_parts.append(f"({primary}.limit_amount > 0 AND ({primary}.spend_amount * 100.0 / {primary}.limit_amount) < ?)")
            params.append(v)
        except (TypeError, ValueError):
            pass

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    join_sql = " ".join(join_clauses)

    sql = f"""
        SELECT {select_expr}
        FROM {primary}
        {join_sql}
        {where_sql}
        LIMIT {limit}
    """

    try:
        cur = conn.execute(sql, params)
        rows_raw = cur.fetchall()
        headers = [d[0] for d in cur.description] if cur.description else []
        rows = [[str(v) if v is not None else "" for v in row] for row in rows_raw]
    finally:
        conn.close()

    return headers, rows

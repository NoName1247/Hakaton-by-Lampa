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


def _searchable_text_columns(primary_columns: set[str]) -> list[str]:
    """Колонки, по которым можно делать универсальный contains-поиск."""
    preferred = [
        "kfsr_name", "kcsr_name", "kvr_name", "kosgu_name",
        "budget_name", "caption", "org_name", "dd_recipient_caption",
        "source_file", "posting_date", "close_date",
        "kfsr_code", "kcsr_raw", "kcsr_norm", "kvr_code", "kosgu_code",
        "goal_code", "kvfo_code", "kvfo_name", "fund_source",
        "reg_number", "con_number", "con_document_id",
    ]
    cols = [c for c in preferred if c in primary_columns]
    # Подхватываем и другие текстовые поля по паттернам имени.
    for c in sorted(primary_columns):
        if c in cols:
            continue
        lc = c.lower()
        if any(k in lc for k in ("name", "caption", "code", "source", "date", "number", "document", "org")):
            cols.append(c)
    return cols[:30]


def _tokenize_query(text: str) -> list[str]:
    stop = {
        "дай", "мне", "все", "данные", "по", "за", "для", "где", "и", "или", "с", "на", "к", "от",
        "это", "этот", "эта", "эти", "какие", "какой", "какая", "покажи", "связанные", "связано",
        "нужны", "нужно", "нужен", "найди", "найти", "записи", "строки", "таблица", "таблицу",
        "про", "поиск", "показателю", "показатель"
    }
    toks = re.findall(r"[a-zа-я0-9]+", (text or "").lower())
    return [t for t in toks if len(t) >= 3 and t not in stop][:5]


def execute_plan(plan: dict) -> tuple[list[str], list[list[Any]]]:
    """
    Выполнить JSON-план от GigaChat.
    Возвращает (headers, rows).
    """
    sources = plan.get("sources", [])
    filters = plan.get("filters", {})
    columns = plan.get("columns", [])
    joins = plan.get("joins", [])
    limit = min(int(plan.get("limit", 20000)), 200000)
    offset = max(0, int(plan.get("offset", 0) or 0))

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

    if "kfsr_name_contains" in filters and "kfsr_name" in primary_columns:
        where_parts.append(f"{primary}.kfsr_name LIKE ?")
        params.append(f"%{filters['kfsr_name_contains']}%")

    # Универсальный contains-поиск по набору текстовых полей (OR).
    if "any_text_contains" in filters:
        txt = str(filters.get("any_text_contains", "")).strip()
        if txt:
            text_cols = _searchable_text_columns(primary_columns)
            if text_cols:
                where_parts.append("(" + " OR ".join([f"{primary}.{c} LIKE ?" for c in text_cols]) + ")")
                params.extend([f"%{txt}%"] * len(text_cols))

    # Универсальный поиск по нескольким токенам (AND между токенами; OR по колонкам внутри токена).
    if "any_text_contains_all" in filters:
        vals = [str(v).strip() for v in filters.get("any_text_contains_all", []) if str(v).strip()]
        text_cols = _searchable_text_columns(primary_columns)
        if vals and text_cols:
            for v in vals:
                where_parts.append("(" + " OR ".join([f"{primary}.{c} LIKE ?" for c in text_cols]) + ")")
                params.extend([f"%{v}%"] * len(text_cols))

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
        OFFSET {offset}
    """

    try:
        cur = conn.execute(sql, params)
        rows_raw = cur.fetchall()
        headers = [d[0] for d in cur.description] if cur.description else []
        rows = [[str(v) if v is not None else "" for v in row] for row in rows_raw]
    finally:
        conn.close()

    return headers, rows


def execute_plan_page(plan: dict, offset: int, page_size: int) -> tuple[list[str], list[list[Any]]]:
    """Постраничное выполнение плана."""
    p = dict(plan or {})
    p["offset"] = max(0, int(offset or 0))
    p["limit"] = max(1, min(int(page_size or 200), 5000))
    return execute_plan(p)


def count_plan_rows(plan: dict) -> int:
    """
    Приблизительный count для плана.
    Использует тот же план без offset и с большим limit.
    """
    p = dict(plan or {})
    p.pop("offset", None)
    p["limit"] = 200000
    _, rows = execute_plan(p)
    return len(rows)


def search_all_tables_any_text(query: str, total_limit: int = 3000) -> tuple[list[str], list[list[Any]]]:
    """
    Fallback: сквозной поиск по всем витринам.
    Возвращает унифицированную таблицу с источником строки.
    """
    tokens = _tokenize_query(query)
    if not tokens:
        return [], []

    conn = _get_conn()
    out_headers = [
        "source_table", "source_file", "budget_name", "caption",
        "posting_date", "close_date",
        "kfsr_code", "kfsr_name", "kcsr_raw", "kcsr_name", "kvr_code", "kosgu_code",
        "org_name", "dd_recipient_caption",
        "reg_number", "con_number",
        "limit_amount", "spend_amount", "payments_execution", "platezhka_amount", "con_amount",
    ]
    rows_out: list[list[Any]] = []
    per_table_limit = max(50, min(1000, total_limit // max(1, len(WHITELIST_TABLES))))

    try:
        for table in sorted(WHITELIST_TABLES):
            cols = _table_columns(conn, table)
            if not cols:
                continue
            text_cols = _searchable_text_columns(cols)
            if not text_cols:
                continue

            where_parts = []
            params: list[Any] = []
            for tok in tokens:
                where_parts.append("(" + " OR ".join([f"{table}.{c} LIKE ?" for c in text_cols]) + ")")
                params.extend([f"%{tok}%"] * len(text_cols))
            where_sql = " AND ".join(where_parts)

            def pick(col: str) -> str:
                return f"{table}.{col}" if col in cols else "''"

            sql = f"""
                SELECT
                    '{table}' as source_table,
                    {pick('source_file')} as source_file,
                    {pick('budget_name')} as budget_name,
                    {pick('caption')} as caption,
                    {pick('posting_date')} as posting_date,
                    {pick('close_date')} as close_date,
                    {pick('kfsr_code')} as kfsr_code,
                    {pick('kfsr_name')} as kfsr_name,
                    {pick('kcsr_raw')} as kcsr_raw,
                    {pick('kcsr_name')} as kcsr_name,
                    {pick('kvr_code')} as kvr_code,
                    {pick('kosgu_code')} as kosgu_code,
                    {pick('org_name')} as org_name,
                    {pick('dd_recipient_caption')} as dd_recipient_caption,
                    {pick('reg_number')} as reg_number,
                    {pick('con_number')} as con_number,
                    {pick('limit_amount')} as limit_amount,
                    {pick('spend_amount')} as spend_amount,
                    {pick('payments_execution')} as payments_execution,
                    {pick('platezhka_amount')} as platezhka_amount,
                    {pick('con_amount')} as con_amount
                FROM {table}
                WHERE {where_sql}
                LIMIT {per_table_limit}
            """
            cur = conn.execute(sql, params)
            for row in cur.fetchall():
                rows_out.append([str(v) if v is not None else "" for v in row])
                if len(rows_out) >= total_limit:
                    return out_headers, rows_out
    finally:
        conn.close()

    return out_headers, rows_out

"""
Динамически строит описание схемы SQLite для системного промпта GigaChat.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

WHITELIST_TABLES = [
    "mart_rchb",
    "mart_agreements",
    "mart_gz_budgetlines",
    "mart_gz_contracts",
    "mart_gz_payments",
    "mart_buau",
]


def get_schema_context() -> str:
    if not os.path.exists(DB_PATH):
        return "(база данных не загружена — запустите etl.py)"

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    lines = []
    for table in WHITELIST_TABLES:
        try:
            cursor = conn.execute(f"PRAGMA table_info({table})")
            cols = cursor.fetchall()
            if not cols:
                continue
            col_str = ", ".join(f"{c[1]} ({c[2]})" for c in cols if c[1] != "id")
            # Получить пример 1 строки для понимания данных
            try:
                sample = conn.execute(
                    f"SELECT * FROM {table} WHERE kcsr_norm != '' LIMIT 1"
                ).fetchone()
            except Exception:
                sample = conn.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()

            lines.append(f"Таблица `{table}`:")
            lines.append(f"  Колонки: {col_str}")
            if sample:
                col_names = [c[1] for c in cols]
                preview = {col_names[i]: str(sample[i])[:60] for i in range(min(8, len(sample)))}
                lines.append(f"  Пример: {preview}")
        except Exception as e:
            lines.append(f"Таблица `{table}`: ошибка — {e}")
    conn.close()
    return "\n".join(lines)


def get_table_schema(table: str) -> list[str]:
    """Вернуть список имён колонок таблицы."""
    if table not in WHITELIST_TABLES:
        return []
    if not os.path.exists(DB_PATH):
        return []
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cursor = conn.execute(f"PRAGMA table_info({table})")
    cols = [c[1] for c in cursor.fetchall() if c[1] != "id"]
    conn.close()
    return cols

"""
ETL: CSV -> SQLite (backend/data.db)
Запуск: python etl.py
"""
import sqlite3
import csv
import re
import os
import glob

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "backend", "data.db")


def kcsr_norm(raw: str) -> str:
    """Нормализация кода КЦСР: убираем точки, пробелы, приводим к единому виду."""
    if not raw:
        return ""
    return re.sub(r"[.\s]+", "", raw.strip().upper())


def parse_number(s: str) -> float | None:
    if not s or s.strip() == "":
        return None
    cleaned = s.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def load_rchb(conn: sqlite3.Connection):
    """РЧБ: полуструктурированные CSV с 10 строками метаданных перед заголовком."""
    conn.execute("DROP TABLE IF EXISTS mart_rchb")
    conn.execute("""
        CREATE TABLE mart_rchb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT,
            budget_name TEXT,
            posting_date TEXT,
            kfsr_code TEXT,
            kfsr_name TEXT,
            kcsr_raw TEXT,
            kcsr_norm TEXT,
            kcsr_name TEXT,
            kvr_code TEXT,
            kvr_name TEXT,
            kvsr_code TEXT,
            kvsr_name TEXT,
            kosgu_code TEXT,
            kosgu_name TEXT,
            goal_code TEXT,
            goal_name TEXT,
            kvfo_code TEXT,
            kvfo_name TEXT,
            fund_source TEXT,
            limit_amount REAL,
            limit_confirmed_bo REAL,
            limit_confirmed_nobo REAL,
            limit_remainder REAL,
            spend_amount REAL
        )
    """)
    conn.commit()

    rchb_dir = os.path.join(BASE, "1. РЧБ")
    files = sorted(glob.glob(os.path.join(rchb_dir, "*.csv")))

    for fpath in files:
        fname = os.path.basename(fpath)
        rows_inserted = 0
        with open(fpath, encoding="utf-8-sig", errors="replace") as f:
            lines = f.readlines()

        # Найти строку заголовка: первая строка где первая ячейка == "Бюджет"
        header_idx = None
        for i, line in enumerate(lines):
            first_cell = line.split(";")[0].strip().strip('"')
            if first_cell == "Бюджет":
                header_idx = i
                break

        if header_idx is None:
            print(f"  [WARN] {fname}: заголовок не найден, пропуск")
            continue

        header = [h.strip().strip('"') for h in lines[header_idx].split(";")]

        def col(row_cells, name):
            try:
                idx = header.index(name)
                return row_cells[idx].strip().strip('"') if idx < len(row_cells) else ""
            except ValueError:
                return ""

        for line in lines[header_idx + 1:]:
            cells = line.rstrip("\n").split(";")
            first = cells[0].strip().strip('"') if cells else ""
            if not first or first.startswith("Итого"):
                continue

            raw_kcsr = col(cells, "КЦСР")

            # Определяем какие числовые колонки есть (у разных месяцев разные названия)
            limit_col = next((h for h in header if h.startswith("Лимиты ПБС")), None)
            bo_col = next((h for h in header if "по БО" in h), None)
            nobo_col = next((h for h in header if "без БО" in h), None)
            rem_col = next((h for h in header if "Остаток лимитов" in h), None)
            spend_col = "Всего выбытий (бух.уч.)"

            conn.execute("""
                INSERT INTO mart_rchb (
                    source_file, budget_name, posting_date,
                    kfsr_code, kfsr_name, kcsr_raw, kcsr_norm, kcsr_name,
                    kvr_code, kvr_name, kvsr_code, kvsr_name,
                    kosgu_code, kosgu_name, goal_code, goal_name,
                    kvfo_code, kvfo_name, fund_source,
                    limit_amount, limit_confirmed_bo, limit_confirmed_nobo,
                    limit_remainder, spend_amount
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                fname,
                col(cells, "Бюджет"),
                col(cells, "Дата проводки"),
                col(cells, "КФСР"),
                col(cells, "Наименование КФСР"),
                raw_kcsr,
                kcsr_norm(raw_kcsr),
                col(cells, "Наименование КЦСР"),
                col(cells, "КВР"),
                col(cells, "Наименование КВР"),
                col(cells, "КВСР"),
                col(cells, "Наименование КВСР"),
                col(cells, "КОСГУ"),
                col(cells, "Наименование КОСГУ"),
                col(cells, "Код цели"),
                col(cells, "Наименование Код цели"),
                col(cells, "КВФО"),
                col(cells, "Наименование КВФО"),
                col(cells, "Источник средств"),
                parse_number(col(cells, limit_col) if limit_col else ""),
                parse_number(col(cells, bo_col) if bo_col else ""),
                parse_number(col(cells, nobo_col) if nobo_col else ""),
                parse_number(col(cells, rem_col) if rem_col else ""),
                parse_number(col(cells, spend_col)),
            ))
            rows_inserted += 1

        conn.commit()
        print(f"  [РЧБ] {fname}: {rows_inserted} строк")


def load_agreements(conn: sqlite3.Connection):
    """Соглашения: стандартные CSV с заголовком на строке 1."""
    conn.execute("DROP TABLE IF EXISTS mart_agreements")
    conn.execute("""
        CREATE TABLE mart_agreements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT,
            period_of_date TEXT,
            documentclass_id TEXT,
            budget_id TEXT,
            caption TEXT,
            document_id TEXT,
            close_date TEXT,
            reg_number TEXT,
            amount_1year REAL,
            dd_estimate_caption TEXT,
            dd_recipient_caption TEXT,
            kadmr_code TEXT,
            kfsr_code TEXT,
            kcsr_raw TEXT,
            kcsr_norm TEXT,
            kvr_code TEXT,
            dd_purposefulgrant_code TEXT,
            kesr_code TEXT
        )
    """)
    conn.commit()

    agr_dir = os.path.join(BASE, "2. Соглашения")
    files = sorted(glob.glob(os.path.join(agr_dir, "*.csv")))

    for fpath in files:
        fname = os.path.basename(fpath)
        rows_inserted = 0
        with open(fpath, encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_kcsr = row.get("kcsr_code", "")
                conn.execute("""
                    INSERT INTO mart_agreements (
                        source_file, period_of_date, documentclass_id, budget_id,
                        caption, document_id, close_date, reg_number, amount_1year,
                        dd_estimate_caption, dd_recipient_caption, kadmr_code,
                        kfsr_code, kcsr_raw, kcsr_norm, kvr_code,
                        dd_purposefulgrant_code, kesr_code
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    fname,
                    row.get("period_of_date", ""),
                    row.get("documentclass_id", ""),
                    row.get("budget_id", ""),
                    row.get("caption", ""),
                    row.get("document_id", ""),
                    row.get("close_date", ""),
                    row.get("reg_number", ""),
                    parse_number(row.get("amount_1year", "")),
                    row.get("dd_estimate_caption", ""),
                    row.get("dd_recipient_caption", ""),
                    row.get("kadmr_code", ""),
                    row.get("kfsr_code", ""),
                    raw_kcsr,
                    kcsr_norm(raw_kcsr),
                    row.get("kvr_code", ""),
                    row.get("dd_purposefulgrant_code", ""),
                    row.get("kesr_code", ""),
                ))
                rows_inserted += 1
        conn.commit()
        print(f"  [Соглашения] {fname}: {rows_inserted} строк")


def load_gz(conn: sqlite3.Connection):
    """ГЗ: три файла — бюджетные строки, контракты, платежки."""
    gz_dir = os.path.join(BASE, "3. ГЗ")

    # Бюджетные строки
    conn.execute("DROP TABLE IF EXISTS mart_gz_budgetlines")
    conn.execute("""
        CREATE TABLE mart_gz_budgetlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            con_document_id TEXT,
            kfsr_code TEXT,
            kcsr_raw TEXT,
            kcsr_norm TEXT,
            kvr_code TEXT,
            kesr_code TEXT,
            kvsr_code TEXT,
            kdf_code TEXT,
            kde_code TEXT,
            kdr_code TEXT,
            kif_code TEXT,
            purposefulgrant TEXT
        )
    """)
    bl_path = os.path.join(gz_dir, "Бюджетные строки.csv")
    if os.path.exists(bl_path):
        with open(bl_path, encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            cnt = 0
            for row in reader:
                raw_kcsr = row.get("kcsr_code", "")
                conn.execute("""
                    INSERT INTO mart_gz_budgetlines (
                        con_document_id, kfsr_code, kcsr_raw, kcsr_norm,
                        kvr_code, kesr_code, kvsr_code, kdf_code,
                        kde_code, kdr_code, kif_code, purposefulgrant
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    str(row.get("con_document_id", "")),
                    row.get("kfsr_code", ""),
                    raw_kcsr, kcsr_norm(raw_kcsr),
                    row.get("kvr_code", ""),
                    row.get("kesr_code", ""),
                    row.get("kvsr_code", ""),
                    row.get("kdf_code", ""),
                    row.get("kde_code", ""),
                    row.get("kdr_code", ""),
                    row.get("kif_code", ""),
                    row.get("purposefulgrant", ""),
                ))
                cnt += 1
        conn.commit()
        print(f"  [ГЗ/БС] Бюджетные строки.csv: {cnt} строк")

    # Контракты
    conn.execute("DROP TABLE IF EXISTS mart_gz_contracts")
    conn.execute("""
        CREATE TABLE mart_gz_contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            con_document_id TEXT,
            con_number TEXT,
            con_date TEXT,
            con_amount REAL,
            zakazchik_key TEXT
        )
    """)
    con_path = os.path.join(gz_dir, "Контракты и договора.csv")
    if os.path.exists(con_path):
        with open(con_path, encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            cnt = 0
            for row in reader:
                conn.execute("""
                    INSERT INTO mart_gz_contracts (
                        con_document_id, con_number, con_date, con_amount, zakazchik_key
                    ) VALUES (?,?,?,?,?)
                """, (
                    str(row.get("con_document_id", "")),
                    row.get("con_number", ""),
                    row.get("con_date", ""),
                    parse_number(row.get("con_amount", "")),
                    row.get("zakazchik_key", ""),
                ))
                cnt += 1
        conn.commit()
        print(f"  [ГЗ/К] Контракты и договора.csv: {cnt} строк")

    # Платежки
    conn.execute("DROP TABLE IF EXISTS mart_gz_payments")
    conn.execute("""
        CREATE TABLE mart_gz_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            con_document_id TEXT,
            platezhka_paydate TEXT,
            platezhka_key TEXT,
            platezhka_num TEXT,
            platezhka_amount REAL
        )
    """)
    pay_path = os.path.join(gz_dir, "Платежки.csv")
    if os.path.exists(pay_path):
        with open(pay_path, encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            cnt = 0
            for row in reader:
                conn.execute("""
                    INSERT INTO mart_gz_payments (
                        con_document_id, platezhka_paydate, platezhka_key,
                        platezhka_num, platezhka_amount
                    ) VALUES (?,?,?,?,?)
                """, (
                    str(row.get("con_document_id", "")),
                    row.get("platezhka_paydate", ""),
                    row.get("platezhka_key", ""),
                    row.get("platezhka_num", ""),
                    parse_number(row.get("platezhka_amount", "")),
                ))
                cnt += 1
        conn.commit()
        print(f"  [ГЗ/П] Платежки.csv: {cnt} строк")


def load_buau(conn: sqlite3.Connection):
    """БУАУ: несколько файлов с итоговой строкой в конце."""
    conn.execute("DROP TABLE IF EXISTS mart_buau")
    conn.execute("""
        CREATE TABLE mart_buau (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT,
            budget_name TEXT,
            posting_date TEXT,
            kfsr_code TEXT,
            kcsr_raw TEXT,
            kcsr_norm TEXT,
            kvr_code TEXT,
            kosgu_code TEXT,
            subsidy_code TEXT,
            industry_code TEXT,
            kvfo_code TEXT,
            org_name TEXT,
            grantor_name TEXT,
            payments_with_return REAL,
            payments_execution REAL,
            payments_restore REAL
        )
    """)
    conn.commit()

    buau_dir = os.path.join(BASE, "4. Выгрузка БУАУ")
    files = sorted(glob.glob(os.path.join(buau_dir, "*.csv")))

    for fpath in files:
        fname = os.path.basename(fpath)
        rows_inserted = 0
        with open(fpath, encoding="utf-8-sig", errors="replace") as f:
            lines = f.readlines()

        if not lines:
            continue

        header = [h.strip().strip('"') for h in lines[0].rstrip("\n").split(";")]

        def col(cells, name):
            try:
                idx = header.index(name)
                return cells[idx].strip().strip('"') if idx < len(cells) else ""
            except ValueError:
                return ""

        for line in lines[1:]:
            cells = line.rstrip("\n").split(";")
            first = cells[0].strip().strip('"') if cells else ""
            if not first or first.startswith("Итого"):
                continue

            raw_kcsr = col(cells, "КЦСР")
            conn.execute("""
                INSERT INTO mart_buau (
                    source_file, budget_name, posting_date,
                    kfsr_code, kcsr_raw, kcsr_norm, kvr_code, kosgu_code,
                    subsidy_code, industry_code, kvfo_code,
                    org_name, grantor_name,
                    payments_with_return, payments_execution, payments_restore
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                fname,
                col(cells, "Бюджет"),
                col(cells, "Дата проводки"),
                col(cells, "КФСР"),
                raw_kcsr, kcsr_norm(raw_kcsr),
                col(cells, "КВР"),
                col(cells, "КОСГУ"),
                col(cells, "Код субсидии"),
                col(cells, "Отраслевой код"),
                col(cells, "КВФО"),
                col(cells, "Организация"),
                col(cells, "Орган, предоставляющий субсидии"),
                parse_number(col(cells, "Выплаты с учетом возврата")),
                parse_number(col(cells, "Выплаты - Исполнение")),
                parse_number(col(cells, "Выплаты - Восстановление выплат - год")),
            ))
            rows_inserted += 1

        conn.commit()
        print(f"  [БУАУ] {fname}: {rows_inserted} строк")


def build_indexes(conn: sqlite3.Connection):
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rchb_kcsr ON mart_rchb(kcsr_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agr_kcsr ON mart_agreements(kcsr_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gz_bl_kcsr ON mart_gz_budgetlines(kcsr_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gz_bl_doc ON mart_gz_budgetlines(con_document_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gz_con_doc ON mart_gz_contracts(con_document_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gz_pay_doc ON mart_gz_payments(con_document_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_buau_kcsr ON mart_buau(kcsr_norm)")
    conn.commit()
    print("  [INDEX] индексы созданы")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    print(f"=== ETL → {DB_PATH} ===")
    load_rchb(conn)
    load_agreements(conn)
    load_gz(conn)
    load_buau(conn)
    build_indexes(conn)
    conn.close()
    print("=== DONE ===")


if __name__ == "__main__":
    main()

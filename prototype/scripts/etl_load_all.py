from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import delete, select

from backend.db import engine, db_session
from backend.etl_utils import (
    dataset_version_from_checksum,
    iter_csv_rows,
    iter_csv_rows_with_preamble,
    normalize_kcsr,
    safe_date,
    safe_float,
    sha256_many,
)
from backend.models import (
    Agreement,
    Base,
    Buau,
    DatasetVersion,
    GzBudgetLine,
    GzContract,
    GzPayment,
    Rchb,
)


DATA_ROOT = Path("/opt/hakaton")


def _load_rchb(dataset_version: str, files: list[Path]) -> int:
    inserted = 0
    with db_session() as s:
        for file in files:
            for row in iter_csv_rows_with_preamble(file, header_startswith="Бюджет;Дата проводки;"):
                kcsr_raw = row.get("КЦСР")
                kcsr_norm, kcsr_digits = normalize_kcsr(kcsr_raw)
                obj = Rchb(
                    dataset_version=dataset_version,
                    source_file=str(file),
                    budget_name=row.get("Бюджет"),
                    posting_date=safe_date(row.get("Дата проводки")),
                    kcsr_raw=kcsr_raw,
                    kcsr_norm=kcsr_norm,
                    kcsr_digits=kcsr_digits,
                    kcsr_name=row.get("Наименование КЦСР"),
                    kfsr_code=row.get("КФСР"),
                    kfsr_name=row.get("Наименование КФСР"),
                    kvr_code=row.get("КВР"),
                    kvr_name=row.get("Наименование КВР"),
                    kosgu_code=row.get("КОСГУ"),
                    kosgu_name=row.get("Наименование КОСГУ"),
                    limit_amount=safe_float(row.get("Лимиты ПБС 2025 год")),
                    spend_amount=safe_float(row.get("Всего выбытий (бух.уч.)")),
                )
                s.add(obj)
                inserted += 1
        return inserted


def _load_buau(dataset_version: str, files: list[Path]) -> int:
    inserted = 0
    with db_session() as s:
        for file in files:
            for row in iter_csv_rows(file, delimiter=";"):
                # filter totals rows
                if (row.get("Бюджет") or "").strip().startswith("Итого"):
                    continue
                kcsr_raw = row.get("КЦСР")
                kcsr_norm, kcsr_digits = normalize_kcsr(kcsr_raw)
                obj = Buau(
                    dataset_version=dataset_version,
                    source_file=str(file),
                    budget_name=row.get("Бюджет"),
                    org_name=row.get("Организация"),
                    posting_date=safe_date(row.get("Дата проводки")),
                    kcsr_raw=kcsr_raw,
                    kcsr_norm=kcsr_norm,
                    kcsr_digits=kcsr_digits,
                    kfsr_code=row.get("КФСР"),
                    kvr_code=row.get("КВР"),
                    kosgu_code=row.get("КОСГУ"),
                    spend_amount=safe_float(row.get("Выплаты - Исполнение")),
                )
                s.add(obj)
                inserted += 1
        return inserted


def _load_agreements(dataset_version: str, files: list[Path]) -> int:
    inserted = 0
    with db_session() as s:
        for file in files:
            for row in iter_csv_rows(file, delimiter=","):
                kcsr_raw = row.get("kcsr_code")
                kcsr_norm, kcsr_digits = normalize_kcsr(kcsr_raw)
                obj = Agreement(
                    dataset_version=dataset_version,
                    source_file=str(file),
                    close_date=safe_date(row.get("close_date")),
                    reg_number=row.get("reg_number"),
                    main_reg_number=row.get("main_reg_number"),
                    budget_name=row.get("caption"),
                    recipient_name=row.get("dd_recipient_caption"),
                    kcsr_raw=kcsr_raw,
                    kcsr_norm=kcsr_norm,
                    kcsr_digits=kcsr_digits,
                    amount_1year=safe_float(row.get("amount_1year")),
                )
                s.add(obj)
                inserted += 1
        return inserted


def _load_gz(dataset_version: str, gz_dir: Path) -> dict[str, int]:
    counts = {"gz_budget_lines": 0, "gz_contracts": 0, "gz_payments": 0}

    with db_session() as s:
        # budget lines
        for row in iter_csv_rows(gz_dir / "Бюджетные строки.csv", delimiter=";"):
            kcsr_raw = row.get("kcsr_code")
            kcsr_norm, kcsr_digits = normalize_kcsr(kcsr_raw)
            s.add(
                GzBudgetLine(
                    dataset_version=dataset_version,
                    source_file=str(gz_dir / "Бюджетные строки.csv"),
                    con_document_id=row.get("con_document_id"),
                    kcsr_raw=kcsr_raw,
                    kcsr_norm=kcsr_norm,
                    kcsr_digits=kcsr_digits,
                    kfsr_code=row.get("kfsr_code"),
                    kvr_code=row.get("kvr_code"),
                    kesr_code=row.get("kesr_code"),
                    purposefulgrant=row.get("purposefulgrant"),
                )
            )
            counts["gz_budget_lines"] += 1

        # contracts
        for row in iter_csv_rows(gz_dir / "Контракты и договора.csv", delimiter=";"):
            s.add(
                GzContract(
                    dataset_version=dataset_version,
                    source_file=str(gz_dir / "Контракты и договора.csv"),
                    con_document_id=row.get("con_document_id"),
                    con_number=row.get("con_number"),
                    con_date=safe_date(row.get("con_date")),
                    con_amount=safe_float(row.get("con_amount")),
                )
            )
            counts["gz_contracts"] += 1

        # payments
        for row in iter_csv_rows(gz_dir / "Платежки.csv", delimiter=";"):
            s.add(
                GzPayment(
                    dataset_version=dataset_version,
                    source_file=str(gz_dir / "Платежки.csv"),
                    con_document_id=row.get("con_document_id"),
                    pay_date=safe_date(row.get("platezhka_paydate")),
                    platezhka_key=row.get("platezhka_key"),
                    platezhka_num=row.get("platezhka_num"),
                    amount=safe_float(row.get("platezhka_amount")),
                )
            )
            counts["gz_payments"] += 1

    return counts


def main():
    Base.metadata.create_all(bind=engine)

    rchb_files = sorted((DATA_ROOT / "1. РЧБ").glob("*.csv"))
    agreements_files = sorted((DATA_ROOT / "2. Соглашения").glob("*.csv"))
    buau_files = sorted((DATA_ROOT / "4. Выгрузка БУАУ").glob("*.csv"))
    gz_dir = DATA_ROOT / "3. ГЗ"

    all_files = rchb_files + agreements_files + buau_files + [
        gz_dir / "Бюджетные строки.csv",
        gz_dir / "Контракты и договора.csv",
        gz_dir / "Платежки.csv",
    ]
    checksum = sha256_many(all_files)
    dataset_version = dataset_version_from_checksum(checksum)

    with db_session() as s:
        existing = s.scalar(select(DatasetVersion).where(DatasetVersion.checksum == checksum))
        if existing is not None:
            print(f"Dataset already loaded: {existing.dataset_version}")
            return

    # load data
    rchb_count = _load_rchb(dataset_version, rchb_files)
    buau_count = _load_buau(dataset_version, buau_files)
    agreements_count = _load_agreements(dataset_version, agreements_files)
    gz_counts = _load_gz(dataset_version, gz_dir)

    summary = {
        "rchb_files": len(rchb_files),
        "agreements_files": len(agreements_files),
        "buau_files": len(buau_files),
        "rchb_rows": rchb_count,
        "buau_rows": buau_count,
        "agreements_rows": agreements_count,
        **gz_counts,
    }

    with db_session() as s:
        s.add(
            DatasetVersion(
                dataset_version=dataset_version,
                checksum=checksum,
                source_summary=json.dumps(summary, ensure_ascii=False),
            )
        )

    print(f"Loaded dataset_version={dataset_version}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


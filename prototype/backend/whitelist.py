from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal


SourceName = Literal["rchb", "buau", "agreements", "gz_budget_lines", "gz_contracts", "gz_payments"]


@dataclass(frozen=True)
class ColumnSpec:
    # SQL expression, using source alias (e.g. "{a}.spend_amount")
    sql: str
    # stable output name
    out: str


SOURCE_TABLES: dict[SourceName, str] = {
    "rchb": "rchb",
    "buau": "buau",
    "agreements": "agreements",
    "gz_budget_lines": "gz_budget_lines",
    "gz_contracts": "gz_contracts",
    "gz_payments": "gz_payments",
}


def _expr(col: str) -> Callable[[str], str]:
    return lambda a: f"{a}.{col}"


def _execution_percent(a: str) -> str:
    # protect divide-by-zero / nulls
    return f"CASE WHEN {a}.limit_amount IS NULL OR {a}.limit_amount = 0 THEN NULL ELSE ({a}.spend_amount / {a}.limit_amount) END"


ALLOWED_COLUMNS: dict[SourceName, dict[str, ColumnSpec]] = {
    "rchb": {
        "budget_name": ColumnSpec(_expr("budget_name")("{a}"), "budget_name"),
        "posting_date": ColumnSpec(_expr("posting_date")("{a}"), "posting_date"),
        "kcsr_raw": ColumnSpec(_expr("kcsr_raw")("{a}"), "kcsr_raw"),
        "kcsr_norm": ColumnSpec(_expr("kcsr_norm")("{a}"), "kcsr_norm"),
        "kcsr_name": ColumnSpec(_expr("kcsr_name")("{a}"), "kcsr_name"),
        "kfsr_code": ColumnSpec(_expr("kfsr_code")("{a}"), "kfsr_code"),
        "kvr_code": ColumnSpec(_expr("kvr_code")("{a}"), "kvr_code"),
        "kosgu_code": ColumnSpec(_expr("kosgu_code")("{a}"), "kosgu_code"),
        "limit_amount": ColumnSpec(_expr("limit_amount")("{a}"), "limit_amount"),
        "spend_amount": ColumnSpec(_expr("spend_amount")("{a}"), "spend_amount"),
        "execution_percent": ColumnSpec(_execution_percent("{a}"), "execution_percent"),
    },
    "buau": {
        "budget_name": ColumnSpec(_expr("budget_name")("{a}"), "budget_name"),
        "org_name": ColumnSpec(_expr("org_name")("{a}"), "org_name"),
        "posting_date": ColumnSpec(_expr("posting_date")("{a}"), "posting_date"),
        "kcsr_raw": ColumnSpec(_expr("kcsr_raw")("{a}"), "kcsr_raw"),
        "kcsr_norm": ColumnSpec(_expr("kcsr_norm")("{a}"), "kcsr_norm"),
        "kfsr_code": ColumnSpec(_expr("kfsr_code")("{a}"), "kfsr_code"),
        "kvr_code": ColumnSpec(_expr("kvr_code")("{a}"), "kvr_code"),
        "kosgu_code": ColumnSpec(_expr("kosgu_code")("{a}"), "kosgu_code"),
        "spend_amount": ColumnSpec(_expr("spend_amount")("{a}"), "spend_amount"),
    },
    "agreements": {
        "close_date": ColumnSpec(_expr("close_date")("{a}"), "close_date"),
        "reg_number": ColumnSpec(_expr("reg_number")("{a}"), "reg_number"),
        "main_reg_number": ColumnSpec(_expr("main_reg_number")("{a}"), "main_reg_number"),
        "budget_name": ColumnSpec(_expr("budget_name")("{a}"), "budget_name"),
        "recipient_name": ColumnSpec(_expr("recipient_name")("{a}"), "recipient_name"),
        "kcsr_raw": ColumnSpec(_expr("kcsr_raw")("{a}"), "kcsr_raw"),
        "kcsr_norm": ColumnSpec(_expr("kcsr_norm")("{a}"), "kcsr_norm"),
        "amount_1year": ColumnSpec(_expr("amount_1year")("{a}"), "amount_1year"),
    },
    "gz_budget_lines": {
        "con_document_id": ColumnSpec(_expr("con_document_id")("{a}"), "con_document_id"),
        "kcsr_raw": ColumnSpec(_expr("kcsr_raw")("{a}"), "kcsr_raw"),
        "kcsr_norm": ColumnSpec(_expr("kcsr_norm")("{a}"), "kcsr_norm"),
        "kfsr_code": ColumnSpec(_expr("kfsr_code")("{a}"), "kfsr_code"),
        "kvr_code": ColumnSpec(_expr("kvr_code")("{a}"), "kvr_code"),
        "kesr_code": ColumnSpec(_expr("kesr_code")("{a}"), "kesr_code"),
        "purposefulgrant": ColumnSpec(_expr("purposefulgrant")("{a}"), "purposefulgrant"),
    },
    "gz_contracts": {
        "con_document_id": ColumnSpec(_expr("con_document_id")("{a}"), "con_document_id"),
        "con_number": ColumnSpec(_expr("con_number")("{a}"), "con_number"),
        "con_date": ColumnSpec(_expr("con_date")("{a}"), "con_date"),
        "con_amount": ColumnSpec(_expr("con_amount")("{a}"), "con_amount"),
    },
    "gz_payments": {
        "con_document_id": ColumnSpec(_expr("con_document_id")("{a}"), "con_document_id"),
        "pay_date": ColumnSpec(_expr("pay_date")("{a}"), "pay_date"),
        "platezhka_key": ColumnSpec(_expr("platezhka_key")("{a}"), "platezhka_key"),
        "platezhka_num": ColumnSpec(_expr("platezhka_num")("{a}"), "platezhka_num"),
        "amount": ColumnSpec(_expr("amount")("{a}"), "amount"),
    },
}


# relation -> join templates
# Each template returns SQL join fragment (without leading "JOIN") and may include extra joins.
RELATIONS: dict[str, Callable[[str, str, dict[str, str]], list[str]]] = {}


def _register_relation(name: str):
    def dec(fn):
        RELATIONS[name] = fn
        return fn

    return dec


@_register_relation("rchb_to_buau_by_kcsr_norm")
def _r_to_buau(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    return [f"LEFT JOIN buau {right} ON {left}.kcsr_norm = {right}.kcsr_norm"]


@_register_relation("rchb_to_agreements_by_kcsr_norm")
def _r_to_agreements(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    return [f"LEFT JOIN agreements {right} ON {left}.kcsr_norm = {right}.kcsr_norm"]


@_register_relation("rchb_to_gz_budgetlines_by_kcsr_norm")
def _r_to_gz_budgetlines(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    return [f"LEFT JOIN gz_budget_lines {right} ON {left}.kcsr_norm = {right}.kcsr_norm"]


@_register_relation("gz_budgetlines_to_contracts_by_con_document_id")
def _gz_bl_to_contracts(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    return [f"LEFT JOIN gz_contracts {right} ON {left}.con_document_id = {right}.con_document_id"]


@_register_relation("gz_contracts_to_payments_by_con_document_id")
def _gz_contracts_to_payments(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    return [f"LEFT JOIN gz_payments {right} ON {left}.con_document_id = {right}.con_document_id"]


@_register_relation("rchb_to_gz_contracts_via_kcsr")
def _r_to_gz_contracts_via_kcsr(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    # requires budget lines alias; if right is contracts alias, create an internal budget_lines alias
    bl_alias = aliases.get("__gz_bl_via_kcsr") or "gzb"
    aliases["__gz_bl_via_kcsr"] = bl_alias
    return [
        f"LEFT JOIN gz_budget_lines {bl_alias} ON {left}.kcsr_norm = {bl_alias}.kcsr_norm",
        f"LEFT JOIN gz_contracts {right} ON {bl_alias}.con_document_id = {right}.con_document_id",
    ]


@_register_relation("rchb_to_gz_payments_via_kcsr")
def _r_to_gz_payments_via_kcsr(left: str, right: str, aliases: dict[str, str]) -> list[str]:
    bl_alias = aliases.get("__gz_bl_via_kcsr") or "gzb"
    aliases["__gz_bl_via_kcsr"] = bl_alias
    con_alias = aliases.get("__gz_con_via_kcsr") or "gzc"
    aliases["__gz_con_via_kcsr"] = con_alias
    return [
        f"LEFT JOIN gz_budget_lines {bl_alias} ON {left}.kcsr_norm = {bl_alias}.kcsr_norm",
        f"LEFT JOIN gz_contracts {con_alias} ON {bl_alias}.con_document_id = {con_alias}.con_document_id",
        f"LEFT JOIN gz_payments {right} ON {con_alias}.con_document_id = {right}.con_document_id",
    ]


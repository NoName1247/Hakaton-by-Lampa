from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class SourceSpec(BaseModel):
    name: Literal["rchb", "buau", "agreements", "gz_budget_lines", "gz_contracts", "gz_payments"]
    alias: str = Field(min_length=1, max_length=8, pattern=r"^[a-zA-Z][a-zA-Z0-9_]*$")


class JoinSpec(BaseModel):
    left: str
    right: str
    relation: Literal[
        "rchb_to_buau_by_kcsr_norm",
        "rchb_to_agreements_by_kcsr_norm",
        "rchb_to_gz_budgetlines_by_kcsr_norm",
        "gz_budgetlines_to_contracts_by_con_document_id",
        "gz_contracts_to_payments_by_con_document_id",
        "rchb_to_gz_contracts_via_kcsr",
        "rchb_to_gz_payments_via_kcsr",
    ]


CmpOp = Literal["=", "!=", ">", ">=", "<", "<=", "in", "between", "ilike"]


class FilterAtom(BaseModel):
    op: Literal["atom"] = "atom"
    field: str
    cmp: CmpOp
    value: object


class FilterAnd(BaseModel):
    op: Literal["and"]
    items: list["FilterTree"]


class FilterOr(BaseModel):
    op: Literal["or"]
    items: list["FilterTree"]


class FilterNot(BaseModel):
    op: Literal["not"]
    item: "FilterTree"


FilterTree = Annotated[Union[FilterAtom, FilterAnd, FilterOr, FilterNot], Field(discriminator="op")]


class SortOp(BaseModel):
    type: Literal["sort"]
    by: str
    direction: Literal["asc", "desc"] = "asc"


Operation = Annotated[Union[SortOp], Field(discriminator="type")]


class PlanDSL(BaseModel):
    sources: list[SourceSpec]
    joins: list[JoinSpec] = Field(default_factory=list)
    filter_tree: Optional[FilterTree] = None
    columns: list[str]
    operations: list[Operation] = Field(default_factory=list)
    limit: int = 200
    offset: int = 0


PlanDSL.model_rebuild()


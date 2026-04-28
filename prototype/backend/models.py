from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy import Date, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"

    dataset_version: Mapped[str] = mapped_column(String(64), primary_key=True)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)
    checksum: Mapped[str] = mapped_column(String(128))
    source_summary: Mapped[str] = mapped_column(Text)


class Rchb(Base):
    __tablename__ = "rchb"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    budget_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    posting_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True, index=True)

    kcsr_raw: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    kcsr_norm: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_digits: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    kfsr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kfsr_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kvr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kvr_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kosgu_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kosgu_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    limit_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    spend_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class Buau(Base):
    __tablename__ = "buau"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    budget_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    org_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    posting_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True, index=True)

    kcsr_raw: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    kcsr_norm: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_digits: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    kfsr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kvr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kosgu_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    spend_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class Agreement(Base):
    __tablename__ = "agreements"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    close_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True, index=True)
    reg_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    main_reg_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    budget_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recipient_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    kcsr_raw: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    kcsr_norm: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_digits: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    amount_1year: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class GzBudgetLine(Base):
    __tablename__ = "gz_budget_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    con_document_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_raw: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    kcsr_norm: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kcsr_digits: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    kfsr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kvr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kesr_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    purposefulgrant: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class GzContract(Base):
    __tablename__ = "gz_contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    con_document_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    con_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    con_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)
    con_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class GzPayment(Base):
    __tablename__ = "gz_payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    source_file: Mapped[str] = mapped_column(Text)
    loaded_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)

    con_document_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    pay_date: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True, index=True)
    platezhka_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    platezhka_num: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class Plan(Base):
    __tablename__ = "plans"

    plan_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    plan_version: Mapped[int] = mapped_column(Integer, default=1)
    dataset_version: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=dt.datetime.utcnow)
    current_plan_json: Mapped[str] = mapped_column(Text)
    history_json: Mapped[str] = mapped_column(Text)


from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from backend.settings import settings


def create_db_engine() -> Engine:
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        future=True,
    )
    return engine


engine = create_db_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def db_session():
    session = SessionLocal()
    try:
        session.execute(
            text("SET LOCAL statement_timeout = :ms"),
            {"ms": settings.statement_timeout_ms},
        )
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


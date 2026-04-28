from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/hakaton"
    redis_url: str = "redis://localhost:6379/0"

    # safety defaults for query execution
    statement_timeout_ms: int = 15_000
    max_limit: int = 500
    default_limit: int = 200
    max_filter_depth: int = 4


settings = Settings()


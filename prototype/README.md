# Hackathon pre-release prototype

This folder contains a runnable backend + ETL for the pre-release plan.

## Quickstart (Linux)

### 1) Create venv and install deps

```bash
cd /opt/hakaton/prototype
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

### 2) Start PostgreSQL and Redis (no docker-compose required)

```bash
docker network create hakaton-net || true

docker rm -f hakaton-pg hakaton-redis >/dev/null 2>&1 || true

docker run -d --name hakaton-pg --network hakaton-net \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=hakaton \
  -p 5432:5432 postgres:16

docker run -d --name hakaton-redis --network hakaton-net \
  -p 6379:6379 redis:7
```

### 3) Run ETL (loads CSVs into Postgres)

```bash
. .venv/bin/activate
python -m scripts.etl_load_all
```

### 4) Start API

```bash
. .venv/bin/activate
uvicorn backend.app:app --reload --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000/docs`.

## Configuration

Environment variables (defaults are OK if you used the commands above):

- `DATABASE_URL`: e.g. `postgresql+psycopg://postgres:postgres@localhost:5432/hakaton`
- `REDIS_URL`: e.g. `redis://localhost:6379/0` (optional for now)


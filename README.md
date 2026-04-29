# Lampa Budget Assistant

Веб-приложение для анализа бюджетных выгрузок с AI-ассистентом на базе GigaChat.

**Стек:** FastAPI + SQLite (backend), single-file HTML (frontend), GigaChat API (AI).

---

## Структура

```
.
├── backend/
│   ├── main.py        # API, бизнес-логика, rate-limit, security
│   ├── db.py          # Выполнение JSON-планов к SQLite
│   ├── etl.py         # Загрузка CSV → data.db
│   └── gigachat.py    # OAuth + запросы в GigaChat
├── index.html         # Фронтенд (один файл)
├── .env               # Конфигурация (не коммитить)
└── .env.example       # Пример конфигурации
```

---

## Быстрый старт

```bash
cd /opt/hakaton
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
cp .env.example .env
# Заполните .env (см. ниже)
```

Если `backend/data.db` уже есть — сразу запускайте backend:

```bash
python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Если нужно пересобрать БД из CSV:

```bash
python3 backend/etl.py
python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Фронтенд доступен на `http://localhost:8000` (backend отдаёт `index.html` на корне).

---

## Переменные окружения (`.env`)

| Переменная | Обязательна | По умолчанию | Описание |
|---|---|---|---|
| `GIGACHAT_AUTHORIZATION_KEY` | ✅ | — | Ключ авторизации GigaChat (Base64, с `Basic` или без) |
| `GIGACHAT_SCOPE` | | `GIGACHAT_API_PERS` | Скоуп GigaChat |
| `GIGACHAT_OAUTH_URL` | | `https://ngw.devices.sberbank.ru:9443/api/v2/oauth` | OAuth URL |
| `GIGACHAT_VERIFY_TLS` | | `true` | Проверка TLS (`true`/`false`) |
| `LAMPA_ALLOWED_ORIGINS` | | `http://139.60.162.135:8080,http://localhost:8080` | CORS-источники (через запятую) |
| `LAMPA_API_KEY` | | — | API-ключ backend. Если пусто — авторизация отключена |
| `LAMPA_RATE_LIMIT_ENABLED` | | `true` | Включить rate-limit |
| `LAMPA_RATE_LIMIT_PER_MIN` | | `120` | Лимит запросов в минуту |
| `LAMPA_RATE_LIMIT_WINDOW_SEC` | | `60` | Окно rate-limit (сек) |

Пример `.env`:

```env
GIGACHAT_AUTHORIZATION_KEY=your_base64_key
LAMPA_API_KEY=your_secret_api_key
```

---

## API

| Метод | Эндпоинт | Описание |
|---|---|---|
| `GET` | `/health` | Healthcheck |
| `GET` | `/api/schema` | Схема БД |
| `POST` | `/api/query` | Запрос к данным |
| `POST` | `/api/query/page` | Запрос с пагинацией |
| `POST` | `/api/chat` | Чат с AI-ассистентом |

---

## Требования

- Python 3.10+ (рекомендуется 3.11/3.12)
- Доступ к GigaChat API

---

## Авторы

Команда **lampa** — хакатон БФТ × Минфин АО.

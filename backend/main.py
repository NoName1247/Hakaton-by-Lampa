import json, re, os, sys, math, statistics, time, uuid
from collections import defaultdict, deque
from typing import Optional, Any
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import gigachat as gc
import db as database
from schema_context import get_schema_context
from prompts import build_query_prompt, build_chat_prompt, build_intent_prompt, build_plan_prompt, build_patch_prompt

app = FastAPI(title="Lampa Budget API", version="1.0")


def _parse_csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    out = [v.strip() for v in str(raw).split(",") if v.strip()]
    return out or [default]


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int, min_value: int | None = None) -> int:
    try:
        out = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        out = default
    if min_value is not None:
        out = max(min_value, out)
    return out


ALLOWED_ORIGINS = _parse_csv_env(
    "LAMPA_ALLOWED_ORIGINS",
    "http://139.60.162.135:8080,http://localhost:8080,http://127.0.0.1:8080"
)
API_KEY = os.getenv("LAMPA_API_KEY", "").strip()
RATE_LIMIT_ENABLED = _env_bool("LAMPA_RATE_LIMIT_ENABLED", True)
RATE_LIMIT_PER_MIN = _env_int("LAMPA_RATE_LIMIT_PER_MIN", 120, min_value=10)
RATE_LIMIT_WINDOW_SEC = _env_int("LAMPA_RATE_LIMIT_WINDOW_SEC", 60, min_value=10)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": f"HTTP_{exc.status_code}",
            "detail": str(exc.detail),
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    METRICS["errors_total"] += 1
    _json_log("unhandled_exception", error=str(exc))
    return JSONResponse(
        status_code=500,
        content={
            "code": "INTERNAL_ERROR",
            "detail": "Внутренняя ошибка сервиса. Повторите позже.",
        },
    )

class QueryRequest(BaseModel):
    query: str
    current_table: Optional[dict] = None
    page_size: Optional[int] = 300

class ChatRequest(BaseModel):
    message: str
    current_table: dict
    history: Optional[list[dict]] = None
    query_id: Optional[str] = None
    session_id: Optional[str] = None
    table_version: Optional[int] = None
    selection_clusters: Optional[list[dict[str, Any]]] = None


class QueryPageRequest(BaseModel):
    query_id: str
    offset: int
    limit: int = 300

COLUMN_LABELS = {
    "kcsr_raw":"КЦСР","kcsr_norm":"КЦСР (норм.)","kcsr_name":"Наименование КЦСР",
    "kfsr_code":"КФСР","kfsr_name":"Наименование КФСР","kvr_code":"КВР",
    "kvr_name":"Наименование КВР","kosgu_code":"КОСГУ","budget_name":"Бюджет",
    "caption":"Наименование бюджета","posting_date":"Дата проводки",
    "limit_amount":"Лимиты (руб.)","spend_amount":"Расходы (руб.)",
    "payments_execution":"Выплаты - Исполнение","payments_with_return":"Выплаты с учётом возврата",
    "org_name":"Организация","dd_recipient_caption":"Получатель",
    "amount_1year":"Сумма соглашения","reg_number":"Рег. номер",
    "close_date":"Дата закрытия","con_number":"Номер контракта",
    "con_date":"Дата контракта","con_amount":"Сумма контракта",
    "platezhka_amount":"Сумма платежа","platezhka_paydate":"Дата платежа",
    "source_file":"Файл-источник","execution_percent":"% освоения",
}
LABEL_TO_CANONICAL = {v: k for k, v in COLUMN_LABELS.items()}

def _labels(hdrs): return [COLUMN_LABELS.get(h,h) for h in hdrs]
def _to_canonical_header(h): return LABEL_TO_CANONICAL.get(h, h)
def _normalize_headers_to_canonical(headers: list[str]) -> list[str]:
    return [_to_canonical_header(h) for h in (headers or [])]
def _norm(s): return re.sub(r"[^a-zа-я0-9]+","",str(s or "").lower().replace("ё","е"))
def _to_num(v):
    try: return float(str(v).replace("%","").replace(",",".").replace(" ","").replace("\xa0",""))
    except: return None


QUERY_CACHE_TTL_SEC = 30 * 60
QUERY_CACHE_MAX_ITEMS = 40

class SessionState:
    def __init__(self, session_id: str, base_plan: dict, ai_comment: str = ""):
        self.session_id = session_id
        self.base_plan = dict(base_plan or {})
        self.current_plan = dict(base_plan or {})
        self.table_state = {"headers": [], "rows": []}
        self.patch_history: list[dict] = []
        self.chat_history: list[dict] = []
        self.version = 0
        self.ai_comment = ai_comment
        self.created_at = time.time()
        self.updated_at = time.time()
    
    def update_table(self, headers: list[str], rows: list[list]) -> int:
        self.table_state = {
            "headers": list(headers or []),
            "rows": [list(r) for r in (rows or [])]
        }
        self.version += 1
        self.updated_at = time.time()
        return self.version
    
    def update_plan(self, new_plan: dict) -> int:
        self.current_plan = dict(new_plan or {})
        self.version += 1
        self.updated_at = time.time()
        return self.version
    
    def add_patch(self, patch: dict) -> int:
        self.patch_history.append({
            "patch": dict(patch),
            "timestamp": time.time(),
            "version": self.version
        })
        self.version += 1
        self.updated_at = time.time()
        return self.version
    
    def add_chat_message(self, role: str, message: str):
        self.chat_history.append({
            "role": role,
            "message": message,
            "timestamp": time.time()
        })
        self.updated_at = time.time()
    
    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "base_plan": self.base_plan,
            "current_plan": self.current_plan,
            "table_state": self.table_state,
            "patch_history": self.patch_history,
            "chat_history": self.chat_history[-20:],
            "version": self.version,
            "ai_comment": self.ai_comment,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

SESSION_STORE: dict[str, SessionState] = {}
QUERY_CACHE: dict[str, dict] = {}
RATE_LIMIT_STATE: dict[str, deque] = defaultdict(deque)
METRICS = {
    "requests_total": 0,
    "errors_total": 0,
    "rate_limited_total": 0,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_log(event: str, **kwargs):
    payload = {"ts_ms": _now_ms(), "event": event}
    payload.update(kwargs)
    try:
        print(json.dumps(payload, ensure_ascii=False), flush=True)
    except Exception:
        pass


def _extract_client_ip(req: Request) -> str:
    xfwd = req.headers.get("x-forwarded-for", "").strip()
    if xfwd:
        return xfwd.split(",")[0].strip()
    if req.client and req.client.host:
        return req.client.host
    return "unknown"


@app.middleware("http")
async def security_and_metrics_middleware(request: Request, call_next):
    started = time.time()
    METRICS["requests_total"] += 1
    path = request.url.path
    ip = _extract_client_ip(request)

    if path.startswith("/api/"):
        if API_KEY:
            provided = request.headers.get("x-api-key", "").strip()
            if not provided:
                auth = request.headers.get("authorization", "")
                if auth.lower().startswith("bearer "):
                    provided = auth[7:].strip()
            if provided != API_KEY:
                METRICS["errors_total"] += 1
                return JSONResponse(status_code=401, content={"code": "HTTP_401", "detail": "Требуется корректный API key."})

        if RATE_LIMIT_ENABLED:
            now = time.time()
            q = RATE_LIMIT_STATE[ip]
            while q and now - q[0] > RATE_LIMIT_WINDOW_SEC:
                q.popleft()
            if len(q) >= RATE_LIMIT_PER_MIN:
                METRICS["errors_total"] += 1
                METRICS["rate_limited_total"] += 1
                return JSONResponse(status_code=429, content={"code": "HTTP_429", "detail": "Слишком много запросов. Повторите позже."})
            q.append(now)

    try:
        response = await call_next(request)
    except Exception:
        METRICS["errors_total"] += 1
        _json_log("request_error", method=request.method, path=path, ip=ip)
        raise

    elapsed_ms = int((time.time() - started) * 1000)
    if response.status_code >= 400:
        METRICS["errors_total"] += 1
    _json_log(
        "request",
        method=request.method,
        path=path,
        status=response.status_code,
        elapsed_ms=elapsed_ms,
        ip=ip
    )
    return response


def _session_cleanup():
    now = time.time()
    stale_sessions = [k for k, v in SESSION_STORE.items() if now - v.updated_at > QUERY_CACHE_TTL_SEC]
    for k in stale_sessions:
        SESSION_STORE.pop(k, None)
    if len(SESSION_STORE) > QUERY_CACHE_MAX_ITEMS:
        keys_by_age = sorted(SESSION_STORE.keys(), key=lambda k: SESSION_STORE[k].updated_at)
        for k in keys_by_age[: max(0, len(SESSION_STORE) - QUERY_CACHE_MAX_ITEMS)]:
            SESSION_STORE.pop(k, None)
    
    now = time.time()
    stale = [k for k, v in QUERY_CACHE.items() if now - float(v.get("created_at", 0)) > QUERY_CACHE_TTL_SEC]
    for k in stale:
        QUERY_CACHE.pop(k, None)
    if len(QUERY_CACHE) > QUERY_CACHE_MAX_ITEMS:
        keys_by_age = sorted(QUERY_CACHE.keys(), key=lambda k: QUERY_CACHE[k].get("created_at", 0))
        for k in keys_by_age[: max(0, len(QUERY_CACHE) - QUERY_CACHE_MAX_ITEMS)]:
            QUERY_CACHE.pop(k, None)


def _session_create(base_plan: dict, headers: list[str], rows: list[list], ai_comment: str = "") -> SessionState:
    _session_cleanup()
    session_id = uuid.uuid4().hex
    session = SessionState(session_id, base_plan, ai_comment)
    session.update_table(headers, rows)
    SESSION_STORE[session_id] = session
    return session


def _session_get(session_id: Optional[str]) -> Optional[SessionState]:
    if not session_id:
        return None
    _session_cleanup()
    return SESSION_STORE.get(session_id)


def _cache_cleanup():
    now = time.time()
    stale = [k for k, v in QUERY_CACHE.items() if now - float(v.get("created_at", 0)) > QUERY_CACHE_TTL_SEC]
    for k in stale:
        QUERY_CACHE.pop(k, None)
    if len(QUERY_CACHE) > QUERY_CACHE_MAX_ITEMS:
        keys_by_age = sorted(QUERY_CACHE.keys(), key=lambda k: QUERY_CACHE[k].get("created_at", 0))
        for k in keys_by_age[: max(0, len(QUERY_CACHE) - QUERY_CACHE_MAX_ITEMS)]:
            QUERY_CACHE.pop(k, None)


def _cache_put(headers: list[str], rows: list[list], plan: dict, ai_comment: str) -> str:
    _cache_cleanup()
    qid = uuid.uuid4().hex
    QUERY_CACHE[qid] = {
        "headers": list(headers or []),
        "rows": [list(r) for r in (rows or [])],
        "plan": dict(plan or {}),
        "ai_comment": ai_comment,
        "created_at": time.time(),
    }
    return qid


def _cache_get(query_id: Optional[str]) -> Optional[dict]:
    if not query_id:
        return None
    _cache_cleanup()
    return QUERY_CACHE.get(query_id)

# ── JSON extraction ─────────────────────────────────────────────────────────

def _extract_first_json_object(text):
    start = text.find("{")
    if start == -1: raise ValueError("no JSON")
    depth=0; in_str=False; esc=False
    for i,ch in enumerate(text[start:],start=start):
        if in_str:
            if esc: esc=False
            elif ch=="\\": esc=True
            elif ch=='"': in_str=False
            continue
        if ch=='"': in_str=True
        elif ch=="{": depth+=1
        elif ch=="}":
            depth-=1
            if depth==0: return text[start:i+1]
    raise ValueError("no JSON end")

def _extract_json(text):
    text = re.sub(r"```(?:json)?\s*","",text)
    text = re.sub(r"```","",text)
    raw = _extract_first_json_object(text)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        fixed = re.sub(r",\s*([}\]])", r"\1", raw)
        return json.loads(fixed)


def _normalize_query_plan(plan):
    """Нормализует план от LLM для устойчивого исполнения сложных запросов."""
    if not isinstance(plan, dict):
        return plan
    filters = plan.setdefault("filters", {})
    if not isinstance(filters, dict):
        plan["filters"] = {}
        filters = plan["filters"]

    b = str(filters.get("budget_name_contains", "")).lower()
    if b:
        if any(x in b for x in ("бурей", "бурея", "бао", "раен", "район", "округ")):
            filters["budget_name_contains_any"] = ["Бурейского", "Бурея"]
        if "амур" in b and ("област" in b or "регион" in b):
            filters["budget_name_contains_any"] = ["Амурской области", "Областной бюджет Амурской области"]

    sfc = str(filters.get("source_file_contains", "")).lower()
    if sfc:
        months = ["январ", "феврал", "март", "апрел", "май", "июн", "июл", "август", "сентябр", "октябр", "ноябр", "декабр"]
        found = [m for m in months if m in sfc]
        if len(found) >= 2:
            filters["source_file_contains_any"] = found
            filters.pop("source_file_contains", None)

    # Если пользователь просит "все данные", поднимаем лимит выборки.
    # LLM иногда не задает limit, тогда дефолт может вернуть только часть строк.
    q = str(plan.get("_user_query", "")).lower()
    if any(k in q for k in ("все данные", "дай все", "полностью", "целиком", "всю таблицу", "все по")):
        try:
            current_limit = int(plan.get("limit", 0) or 0)
        except (TypeError, ValueError):
            current_limit = 0
        plan["limit"] = max(current_limit, 2000)

    # Универсальный поиск "по любому показателю":
    # если LLM не выставил конкретные semantic-фильтры, добавляем токены
    # для сквозного contains-поиска по текстовым полям.
    has_specific_filters = any(
        k in filters for k in (
            "kcsr_name_contains", "kcsr_norm_eq", "budget_name_contains", "budget_name_contains_any",
            "org_name_contains", "kfsr_code_eq", "kfsr_name_contains", "source_file_contains",
            "source_file_contains_any", "posting_month", "posting_year", "date_from", "date_to",
        )
    )
    if not has_specific_filters:
        # Выделяем смысловые токены из пользовательского текста.
        raw_tokens = re.findall(r"[a-zа-я0-9]+", q)
        stop = {
            "дай", "мне", "все", "данные", "по", "за", "для", "где", "и", "или", "с", "на", "к", "от",
            "это", "этот", "эта", "эти", "какие", "какой", "какая", "покажи", "связанные", "связано",
            "нужны", "нужно", "нужен", "найди", "найти", "записи", "строки", "таблица", "таблицу",
            "про", "поиск", "показателю", "показатель"
        }
        tokens = [t for t in raw_tokens if len(t) >= 4 and t not in stop]
        # Ограничиваем количество, чтобы запрос не стал слишком тяжелым.
        if tokens:
            filters["any_text_contains_all"] = tokens[:4]

    # Подбор primary-источника по тематике запроса (чтобы реально использовать все наборы данных).
    if any(k in q for k in ("соглашен", "регномер", "получател", "dd_recipient")):
        plan["sources"] = ["mart_agreements"]
    elif any(k in q for k in ("платеж", "платежк", "кассовые выплаты", "platezhka")):
        plan["sources"] = ["mart_gz_payments"]
    elif any(k in q for k in ("контракт", "договор", "con_number", "con_amount")):
        plan["sources"] = ["mart_gz_contracts"]
    elif any(k in q for k in ("буау", "учрежден", "организац", "org_name")):
        plan["sources"] = ["mart_buau"]
    elif any(k in q for k in ("гз", "бюджетные строки", "purposefulgrant", "con_document_id")):
        plan["sources"] = ["mart_gz_budgetlines"]

    # Тематический запрос "дорожное хозяйство" — направляем в mart_rchb + kfsr_name.
    if any(k in q for k in ("дорожн", "дорожное хозяйство", "дорожным хозяйством")):
        try:
            sources = plan.get("sources", [])
            if not isinstance(sources, list):
                sources = []
            if "mart_rchb" not in sources:
                sources = ["mart_rchb"]
            plan["sources"] = sources
        except Exception:
            plan["sources"] = ["mart_rchb"]

        if not filters.get("kfsr_name_contains"):
            filters["kfsr_name_contains"] = "дорож"
        # Если LLM ошибочно положил "дорож..." в kcsr_name — снимаем слишком узкое условие.
        kcsr_contains = str(filters.get("kcsr_name_contains", "")).lower()
        if "дорож" in kcsr_contains:
            filters.pop("kcsr_name_contains", None)

    return plan

# ── UTILS ────────────────────────────────────────────────────────────────────

def _col_nums(rows, ci):
    """Числовые значения столбца ci."""
    return [v for v in (_to_num(r[ci]) for r in rows if ci < len(r)) if v is not None]

def _find_col(headers, hint):
    """Найти индекс столбца по подстрочному совпадению."""
    hint = str(hint or "").replace("кфрс", "кфср")
    hn = _norm(hint)
    # Точное совпадение нормализованного
    for i,h in enumerate(headers):
        if _norm(h) == hn: return i
    # Вхождение
    for i,h in enumerate(headers):
        if hn in _norm(h): return i
    # Частичное по токенам
    tokens = [t for t in re.findall(r"[а-яa-z0-9]+", hint.lower()) if len(t) >= 4]
    for token in tokens:
        for i,h in enumerate(headers):
            if token in _norm(h): return i
    return None

def _fmt(v, ndigits=2):
    if v is None: return ""
    if isinstance(v, float):
        return str(round(v, ndigits)).rstrip("0").rstrip(".") if "." in str(round(v, ndigits)) else str(int(round(v, 0)))
    return str(v)

# ── ФИЛЬТР СТРОК (универсальный) ─────────────────────────────────────────────

def _try_row_filter(headers, rows, msg_l):
    """
    Обрабатывает паттерны:
      "оставь строки где [поле] [содержит/=] [значение]"
      "оставь только те строки в которых [поле] [значение]"
      "покажи строки где [поле] [значение]"
      "отфильтруй по [поле] = [значение]"
      "фильтр: [поле] = [значение]"
    Возвращает (filtered_rows, msg) или None.
    """
    # Нормализуем запрос
    patterns = [
        # "где поле содержит/= значение"
        r"(?:где|в которых|в которых|у которых)\s+(.+?)\s+(?:содержит|=|==|равно|равен|является|это)\s+[\"']?(.+?)[\"']?\s*$",
        # "по полю значение" или "поле: значение"
        r"(?:где|в которых)\s+(.+?)\s+[\"']?([а-яёa-z0-9\s\-\.\/]+)[\"']?\s*$",
        # "фильтр поле = значение"
        r"фильтр(?:уй)?\s+(?:по\s+)?(.+?)\s*[=:]\s*[\"']?(.+?)[\"']?\s*$",
    ]
    for pat in patterns:
        m = re.search(pat, msg_l.strip())
        if m:
            col_hint = m.group(1).strip()
            val_hint = m.group(2).strip().lower()
            # исключаем служебные слова как col_hint
            if col_hint in ("строки","строках","которые","только"):
                continue
            ci = _find_col(headers, col_hint)
            if ci is not None:
                filtered = [r for r in rows if val_hint in str(r[ci] if ci<len(r) else "").lower()]
                return filtered, f"Отфильтровано по «{headers[ci]}» = «{val_hint}»: {len(filtered)} строк"
    return None

# ── LOCAL COMMAND PROCESSOR ──────────────────────────────────────────────────

# Слова, однозначно указывающие на вопрос (без изменения таблицы)
QUESTION_MARKERS = (
    "что такое","что означает","как работает","объясни","расскажи","почему",
    "зачем","когда","сколько стоит","как называется","кто такой","что это",
    "какой","чем отличается","в чём разница","как понять","как считается",
)

GREETING_MARKERS = (
    "привет", "приветик", "здравствуй", "здравствуйте", "добрый день",
    "добрый вечер", "доброе утро", "хай", "hello", "hi", "hey",
)


def _is_greeting(text: str) -> bool:
    t = (text or "").strip().lower()
    t = re.sub(r"[!?.;,:\s]+$", "", t)
    return any(t == g or t.startswith(g + " ") for g in GREETING_MARKERS)


TABLE_EDIT_MARKERS = (
    "добав", "удали", "убери", "очист", "отсорт", "переимен", "заполни",
    "поставь", "вставь", "сделай", "создай", "оставь", "покажи таблиц",
    "фильтр", "строк", "столб", "колонк", "яче", "транспони", "сумм",
    "средн", "медиан", "максим", "миним", "ранг", "процент освоения",
)


def _is_question_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if "?" in t:
        return True
    return any(t.startswith(q) for q in QUESTION_MARKERS)


def _is_table_edit_intent(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(k in t for k in TABLE_EDIT_MARKERS)


def _history_to_text(history: Optional[list[dict]], max_items: int = 12) -> str:
    if not history:
        return ""
    lines = []
    for item in history[-max_items:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        if role not in ("user", "ai", "assistant"):
            role = "user"
        role_lbl = "Пользователь" if role == "user" else "AI"
        lines.append(f"{role_lbl}: {text}")
    return "\n".join(lines)


def _resolve_followup_message(message: str, history: Optional[list[dict]]) -> str:
    """Раскрывает короткие фоллоуапы вроде 'да посчитай все' по истории чата."""
    msg = (message or "").strip()
    msg_l = msg.lower()
    if not history:
        return msg
    if msg_l not in ("да", "да.", "ок", "ок.", "да посчитай все", "посчитай все", "считай все", "да считай все"):
        return msg
    prev_user = None
    for item in reversed(history):
        if isinstance(item, dict) and str(item.get("role", "")).lower() == "user":
            text = str(item.get("text", "")).strip()
            if text and text.lower() != msg_l:
                prev_user = text
                break
    if not prev_user:
        return msg
    if "сумм" in prev_user.lower() and ("лимит" in prev_user.lower() or "колонк" in prev_user.lower()):
        return prev_user + " по всем доступным данным"
    return prev_user + ". Подтверждение пользователя: " + msg


def _selection_preview(selection_clusters: Optional[list[dict]], max_items: int = 4) -> str:
    if not selection_clusters:
        return "[]"
    out = []
    for cl in selection_clusters[:max_items]:
        if not isinstance(cl, dict):
            continue
        out.append({
            "range_label": cl.get("range_label"),
            "r1": cl.get("r1"), "c1": cl.get("c1"), "r2": cl.get("r2"), "c2": cl.get("c2"),
            "headers": (cl.get("headers") or [])[:25],
            "rows_preview": (cl.get("rows") or [])[:20],
        })
    return json.dumps(out, ensure_ascii=False)


def _plain_chat_answer(
    message: str,
    current_table: dict,
    schema: str,
    history: Optional[list[dict]] = None,
    selection_clusters: Optional[list[dict]] = None
) -> str:
    """Обычный текстовый ответ без JSON, для вопросов/уточнений."""
    cur = {
        "headers": current_table.get("headers", []),
        "rows_count": len(current_table.get("rows", [])),
        "rows_preview": current_table.get("rows", [])[:20],
    }
    user_payload = (
        "Вопрос пользователя:\n"
        f"{message}\n\n"
        "История диалога:\n"
        f"{_history_to_text(history)}\n\n"
        "Выделенные диапазоны (selection_clusters):\n"
        f"{_selection_preview(selection_clusters)}\n\n"
        "Текущая таблица (контекст):\n"
        f"{json.dumps(cur, ensure_ascii=False)}\n\n"
        "Схема БД:\n"
        f"{schema}"
    )
    system_prompt = (
        "Ты AI-помощник по бюджетным таблицам.\n"
        "Отвечай ТОЛЬКО обычным текстом на русском, без JSON и без markdown.\n"
        "Если пользователь спрашивает 'это все данные?' или про полноту данных, "
        "объясни, что на экране текущая выборка, укажи число строк из контекста и "
        "предложи переформулировать запрос для более широкой выборки.\n"
        "Если вопрос не про изменение таблицы — просто дай информативный ответ.\n"
        "Не пиши технические ошибки и не проси формат JSON."
    )
    return gc.chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_payload},
    ])


def _local_transform(current_table, message, selection_clusters: Optional[list[dict]] = None):
    """Локальная обработка команд без GigaChat. None = передать GigaChat."""
    headers = list(current_table.get("headers", []))
    rows    = [list(r) for r in current_table.get("rows", [])]
    msg  = (message or "").strip()
    msg_l = msg.lower()
    msg_n = _norm(msg)
    ncols = len(headers)
    nrows = len(rows)

    def _iter_selected_cells():
        if not selection_clusters:
            return
        for cl in selection_clusters:
            if not isinstance(cl, dict):
                continue
            try:
                r1 = max(0, int(cl.get("r1", 0)))
                c1 = max(0, int(cl.get("c1", 0)))
                r2 = min(nrows - 1, int(cl.get("r2", 0)))
                c2 = min(ncols - 1, int(cl.get("c2", 0)))
            except Exception:
                continue
            if r2 < r1 or c2 < c1:
                continue
            for rr in range(r1, r2 + 1):
                for cc in range(c1, c2 + 1):
                    yield rr, cc

    # ── Замена только в выделении: "замени X на Y в выделенном" ──────────────
    if selection_clusters and any(k in msg_l for k in ("в выделенном", "в выделении", "в выбранном", "тут")) and "замен" in msg_l:
        m_rep = re.search(r"замен[а-я]*\s+(.+?)\s+(?:на|->|=>)\s+(.+?)(?:\s+(?:в\s+выделенном|в\s+выделении|в\s+выбранном|тут))?\s*$", msg_l)
        if m_rep:
            old_val = m_rep.group(1).strip().strip("\"'")
            new_val = m_rep.group(2).strip().strip("\"'")
            changed = 0
            for rr, cc in _iter_selected_cells():
                cur = str(rows[rr][cc] if cc < len(rows[rr]) else "")
                if old_val in cur:
                    rows[rr][cc] = cur.replace(old_val, new_val)
                    changed += 1
            return {
                "action": "transform",
                "table": {"headers": headers, "rows": rows},
                "ai_message": f"Сделал замену в выделенных диапазонах. Изменено ячеек: {changed}."
            }

    # ── Простые реплики без изменения таблицы (чтобы не бить лимит GigaChat) ──
    simple_msg = msg_l.strip()
    if _is_greeting(simple_msg) or simple_msg in ("здарова",):
        return {"action": "message", "table": current_table, "ai_message": "Привет! Готов помочь с таблицей."}
    if simple_msg in ("спасибо", "спс", "благодарю"):
        return {"action": "message", "table": current_table, "ai_message": "Пожалуйста."}

    # ── Создать пустую таблицу заданного размера: "10 на 10 пустых клеточек" ──
    m_size = re.search(
        r"(?:сделай|создай|сформируй|построй|сделать)\s*(\d{1,3})\s*(?:на|x|х)\s*(\d{1,3}).*(?:пуст|клет|яче)",
        msg_l
    )
    if m_size:
        rows_n = max(1, min(int(m_size.group(1)), 200))
        cols_n = max(1, min(int(m_size.group(2)), 200))
        new_headers = [f"Столбец {i+1}" for i in range(cols_n)]
        new_rows = [[""] * cols_n for _ in range(rows_n)]
        return {
            "action": "transform",
            "table": {"headers": new_headers, "rows": new_rows},
            "ai_message": f"Создал пустую таблицу {rows_n}×{cols_n}."
        }

    # ── Очистить все ячейки, сохранив структуру ───────────────────────────────
    if re.search(r"(?:удали|очисти|убери)\s+все\s+(?:ячейки|данные\s+в\s+таблице|содержимое\s+таблицы)", msg_l):
        if ncols <= 0:
            return {"action": "message", "table": current_table, "ai_message": "Таблица уже пуста."}
        keep_rows = max(nrows, 1)
        new_rows = [[""] * ncols for _ in range(keep_rows)]
        return {
            "action": "transform",
            "table": {"headers": headers, "rows": new_rows},
            "ai_message": "Таблица очищена: все ячейки пустые."
        }

    # ── Обновить конкретную строку: "добавь в 6 строке полный текст ..." ──
    m_set_row = re.search(r"(?:добавь|вставь|запиши|поставь)\s+в\s+(\d+)\s*строк[еуы]\s+(?:полный\s+текст\s+)?(.+)$", msg_l)
    if m_set_row and ncols > 0:
        row_num = int(m_set_row.group(1))
        text_value = m_set_row.group(2).strip().strip("\"'")
        if row_num < 1:
            row_num = 1
        row_idx = row_num - 1
        while len(rows) <= row_idx:
            rows.append([""] * ncols)
        # Записываем в первый столбец, чтобы команда была предсказуемой
        rows[row_idx][0] = text_value
        return {
            "action": "transform",
            "table": {"headers": headers, "rows": rows},
            "ai_message": f"Добавил текст в строку {row_num}."
        }

    # ── НЕ ТРОГАТЬ ТАБЛИЦУ если явный вопрос ─────────────────────────────
    if any(msg_l.startswith(q) for q in QUESTION_MARKERS):
        return None  # пусть GigaChat ответит на вопрос

    # ── ФИЛЬТР СТРОК (первый приоритет — до "убрать") ────────────────────
    # Паттерны "оставь только те строки в которых..."
    row_filter_triggers = (
        "оставь только те строки",
        "оставить только те строки",
        "покажи строки где",
        "покажи только строки",
        "оставь строки",
        "оставить строки",
        "отфильтруй строки",
        "фильтр строк",
        "строки где",
        "строки в которых",
    )
    for trig in row_filter_triggers:
        # Если в команде есть явный глагол удаления — это не "оставь", а "удали".
        if trig in msg_l and not any(v in msg_l for v in ("удали", "убери", "исключи", "скрой")):
            # Обрезаем триггер и дальше парсим
            rest = msg_l[msg_l.index(trig)+len(trig):].strip()
            # Убираем "и оставь все поля" и подобные хвосты
            rest = re.sub(r"\s+(?:а|и)\s+(?:остальные|все)\s+поля.*$","", rest).strip()
            rest = re.sub(r"\s+(?:а|и)\s+(?:остальное|всё).*$","", rest).strip()

            # Паттерн: "[поле] [=|содержит|это] [значение]"
            m2 = re.search(
                r"^(.+?)\s+(?:содержит|=|==|равно|равен|является|это|—|-)\s*[\"']?(.+?)[\"']?\s*$",
                rest
            )
            if not m2:
                # Без явного оператора: "наименование кфср благоустройство"
                # Ищем: слова начала = возможное поле, последнее слово/фраза = значение
                # Стратегия: попробуем все разбивки
                words = rest.split()
                m2_col = None; m2_val = None
                for split_at in range(len(words)-1, 0, -1):
                    col_try = " ".join(words[:split_at])
                    val_try = " ".join(words[split_at:])
                    ci = _find_col(headers, col_try)
                    if ci is not None:
                        m2_col = ci; m2_val = val_try; break
                if m2_col is not None:
                    filtered = [r for r in rows if m2_val in str(r[m2_col] if m2_col<len(r) else "").lower()]
                    return {
                        "action":"transform",
                        "table":{"headers":headers,"rows":filtered},
                        "ai_message":f"Фильтр: «{headers[m2_col]}» содержит «{m2_val}» — {len(filtered)} строк"
                    }
            if m2:
                ci = _find_col(headers, m2.group(1).strip())
                if ci is not None:
                    val_hint = m2.group(2).strip().lower()
                    filtered = [r for r in rows if val_hint in str(r[ci] if ci<len(r) else "").lower()]
                    return {
                        "action":"transform",
                        "table":{"headers":headers,"rows":filtered},
                        "ai_message":f"Фильтр: «{headers[ci]}» содержит «{val_hint}» — {len(filtered)} строк"
                    }
            break

    # ── УДАЛИТЬ СТРОКИ ПО УСЛОВИЮ ("убери все данные где ...") ───────────
    # Примеры:
    # - "убери все данные где наименование кфср культура"
    # - "удали строки где кфср = культура"
    # - "убери все данные к кфрс культура" (с опечаткой)
    remove_row_triggers = (
        "убери все данные", "удали все данные", "исключи все данные",
        "удали строки где", "убери строки где", "исключи строки где",
    )
    for trig in remove_row_triggers:
        if trig in msg_l:
            rest = msg_l[msg_l.index(trig) + len(trig):].strip()
            rest = rest.replace("кфрс", "кфср")
            rest = re.sub(r"^к\s+", "", rest).strip()

            # Паттерн: "[поле] [=|содержит] [значение]"
            m_rm = re.search(
                r"^(.+?)\s+(?:содержит|=|==|равно|равен|является|это|—|-)\s*[\"']?(.+?)[\"']?\s*$",
                rest
            )

            if m_rm:
                col_hint = m_rm.group(1).strip()
                val_hint = m_rm.group(2).strip().lower()
                ci = _find_col(headers, col_hint)
                # Если выбрали "КФСР", а значение текстовое (например "культура"),
                # почти всегда имелся в виду "Наименование КФСР".
                if ci is not None and _norm(headers[ci]) == _norm("КФСР") and re.search(r"[а-яa-z]", val_hint):
                    alt = _find_col(headers, "Наименование КФСР")
                    if alt is not None:
                        ci = alt
                if ci is not None:
                    filtered = [r for r in rows if val_hint not in str(r[ci] if ci < len(r) else "").lower()]
                    removed_n = len(rows) - len(filtered)
                    return {
                        "action": "transform",
                        "table": {"headers": headers, "rows": filtered},
                        "ai_message": f"Удалил строки по условию «{headers[ci]} содержит {val_hint}». Удалено: {removed_n}."
                    }

            # Без явного оператора: "кфср культура" / "наименование кфср культура"
            words = rest.split()
            guessed_ci = None
            guessed_val = None
            for split_at in range(len(words) - 1, 0, -1):
                col_try = " ".join(words[:split_at]).strip()
                val_try = " ".join(words[split_at:]).strip()
                ci = _find_col(headers, col_try)
                if ci is not None and val_try:
                    guessed_ci, guessed_val = ci, val_try
                    break

            # Если колонка не угадалась, но есть слово "культура" — предполагаем Наименование КФСР
            if guessed_ci is None and "культура" in rest:
                ci_kfsr_name = _find_col(headers, "Наименование КФСР")
                if ci_kfsr_name is not None:
                    guessed_ci, guessed_val = ci_kfsr_name, "культура"

            # Коррекция: КФСР + текстовое значение => Наименование КФСР
            if guessed_ci is not None and _norm(headers[guessed_ci]) == _norm("КФСР") and guessed_val and re.search(r"[а-яa-z]", guessed_val):
                alt = _find_col(headers, "Наименование КФСР")
                if alt is not None:
                    guessed_ci = alt

            if guessed_ci is not None and guessed_val:
                filtered = [r for r in rows if guessed_val not in str(r[guessed_ci] if guessed_ci < len(r) else "").lower()]
                removed_n = len(rows) - len(filtered)
                return {
                    "action": "transform",
                    "table": {"headers": headers, "rows": filtered},
                    "ai_message": f"Удалил строки по условию «{headers[guessed_ci]} содержит {guessed_val}». Удалено: {removed_n}."
                }
            break

    # ── УБРАТЬ / УДАЛИТЬ КОЛОНКИ ─────────────────────────────────────────
    rm_verbs = ("убери","удали","скрой","убрать","удалить","скрыть","спрячь","исключи","исключить","удали")
    # Убедимся что речь о СТОЛБЦАХ, а не строках
    row_words = ("строк","ряд","запис","строки","строку")
    has_rm   = any(w in msg_l for w in rm_verbs)
    has_rows = any(w in msg_l for w in row_words)
    col_nouns = ("поля","поле","колонки","колонку","столбцы","столбец","столбик","column","col")
    has_col  = any(w in msg_n for w in [_norm(x) for x in col_nouns])

    # Важно: удаление колонок только если явно сказано про колонки/поля/столбцы.
    if has_rm and has_col and not has_rows and headers:
        # Словарь алиасов
        aliases = {
            "кфср":                   ["КФСР", "kfsr_code"],
            "кцср":                   ["КЦСР", "kcsr_raw", "kcsr_norm"],
            "квр":                    ["КВР", "kvr_code"],
            "косгу":                  ["КОСГУ", "kosgu_code"],
            "наименованиекфср":      ["Наименование КФСР","kfsr_name"],
            "наименованиекцср":      ["Наименование КЦСР","kcsr_name"],
            "наименованиекцсо":      ["Наименование КЦСР","kcsr_name"],
            "наименованиеквр":       ["Наименование КВР","kvr_name"],
            "наименованиекосгу":     ["Наименование КОСГУ","kosgu_name"],
            "наименованиебюджета":   ["Наименование бюджета","budget_name"],
            "файлисточник":          ["Файл-источник","source_file"],
            "датапроводки":          ["Дата проводки","posting_date"],
            "процентосвоения":       ["% освоения","execution_percent"],
        }
        to_remove = set()
        for key, cands in aliases.items():
            if key in msg_n:
                for c in cands:
                    for h in headers:
                        if _norm(h) == _norm(c): to_remove.add(h)

        # Явный паттерн: "убери столбец <название>" / "удали колонку <название>"
        m_col = re.search(r"(?:столбец|колонк[ауи]?|поле)\s+(.+)$", msg_l)
        if m_col:
            col_hint = m_col.group(1).strip().replace("кфрс", "кфср")
            ci = _find_col(headers, col_hint)
            if ci is not None:
                to_remove.add(headers[ci])

        # Токен-поиск по словам команды
        skip_words = {_norm(x) for x in list(col_nouns)+list(rm_verbs)} | {"поставь","поля","поле","все","только","такие","которые"}
        for token in re.findall(r"[a-zа-я0-9]+", msg_l):
            if len(token) < 4 or token in skip_words: continue
            for h in headers:
                if token in _norm(h): to_remove.add(h)

        if to_remove:
            keep = [i for i,h in enumerate(headers) if h not in to_remove]
            if not keep:
                return {"action":"message","table":current_table,"ai_message":"Нельзя убрать все столбцы."}
            return {
                "action":"transform",
                "table":{"headers":[headers[i] for i in keep],
                         "rows":[[r[i] if i<len(r) else "" for i in keep] for r in rows]},
                "ai_message":"Убрал: " + ", ".join(sorted(to_remove))
            }

    # ── ОСТАВИТЬ ТОЛЬКО КОЛОНКИ ──────────────────────────────────────────
    leave_col_triggers = ("оставь только поля","оставить только поля","оставь поля","оставить поля",
                          "покажи только поля","покажи только столбцы","оставь только столбцы")
    for trig in leave_col_triggers:
        if trig in msg_l:
            rest = msg_l[msg_l.index(trig)+len(trig):].strip()
            keep_h = []
            # Ищем совпадения по именам столбцов
            for h in headers:
                hn = _norm(h)
                if any(t in hn for t in re.findall(r"[а-яa-z0-9]+", rest) if len(t)>=4):
                    keep_h.append(h)
            if keep_h:
                keep_idx = [headers.index(h) for h in keep_h]
                return {
                    "action":"transform",
                    "table":{"headers":keep_h,"rows":[[r[i] if i<len(r) else "" for i in keep_idx] for r in rows]},
                    "ai_message":"Оставил столбцы: " + ", ".join(keep_h)
                }

    # ── ДОБАВИТЬ ПУСТЫЕ СТОЛБЦЫ ──────────────────────────────────────────
    m = re.search(r"добав[а-я]+\s+(\d+)\s+(?:пустых?\s+)?(?:столбц|колонк|поля|поле|column)", msg_l)
    if m:
        n = min(int(m.group(1)), 20)
        new_headers = headers + [f"Столбец {ncols+i+1}" for i in range(n)]
        new_rows = [r+[""]*n for r in rows]
        return {"action":"transform","table":{"headers":new_headers,"rows":new_rows},
                "ai_message":f"Добавил {n} пустых столбца"}

    # ── ДОБАВИТЬ ПУСТЫЕ СТРОКИ ────────────────────────────────────────────
    m = re.search(r"добав[а-я]+\s+(\d+)\s+(?:пустых?\s+)?(?:строк|ряд|row)", msg_l)
    if m:
        n = min(int(m.group(1)), 500)
        new_rows = rows + [[""]*ncols for _ in range(n)]
        return {"action":"transform","table":{"headers":headers,"rows":new_rows},
                "ai_message":f"Добавил {n} пустых строк"}

    # ── ЗАПОЛНИТЬ ВСЕ ЯЧЕЙКИ ЗНАЧЕНИЕМ ───────────────────────────────────
    if re.search(r"(?:поставь|заполни|установи|впиши).{0,30}(?:каждую|каждой|каждый|все|всех)", msg_l):
        m = re.search(r"(?:поставь|заполни|установи|впиши)\s+(?:в\s+)?(?:каждую\s+)?(?:ячейку\s+)?(?:значение\s+|значением\s+)?[\"']?(\S+)[\"']?", msg_l)
        if m:
            val = m.group(1).strip("\"'")
            new_rows = [[val]*ncols for _ in rows]
            return {"action":"transform","table":{"headers":headers,"rows":new_rows},
                    "ai_message":f"Заполнил все ячейки значением «{val}»"}

    # ── СУММА ПО КОНКРЕТНОЙ КОЛОНКЕ (без изменения таблицы) ─────────────
    # Примеры:
    # - "посчитай мне сумму всех лимитов"
    # - "мне нужна сумма колонки limit_remainder"
    # - "какая сумма по столбцу остаток лимитов"
    if ("сумм" in msg_l or "итог" in msg_l) and any(k in msg_l for k in ("посчитай", "сколько", "какая", "нужно", "дай", "мне")):
        target_ci = None
        target_hint = ""

        # Явное указание колонки/столбца/поля
        m_sum_col = re.search(r"(?:колонк[аиуе]|столбц[аеу]|пол[ея])\s+[\"']?(.+?)[\"']?\s*$", msg_l)
        if m_sum_col:
            target_hint = m_sum_col.group(1).strip()
            target_ci = _find_col(headers, target_hint)

        # Частые алиасы для "остатка лимитов"
        if target_ci is None and any(k in msg_l for k in ("limit remain", "limit_remainder", "remain", "остаток лимитов", "лимит remain", "лимит remainder")):
            for hint in ("limit_remainder", "remain", "остаток лимитов", "лимитов остаток", "лимит остаток"):
                ci = _find_col(headers, hint)
                if ci is not None:
                    target_ci = ci
                    target_hint = hint
                    break

        # Если про "лимиты" без точного имени — берем наиболее подходящую колонку лимита
        if target_ci is None and "лимит" in msg_l:
            for i, h in enumerate(headers):
                hn = _norm(h)
                if "limit_remainder" in hn or "остатоклимитов" in hn:
                    target_ci = i
                    break
            if target_ci is None:
                for i, h in enumerate(headers):
                    hn = _norm(h)
                    if "лимит" in hn or "limit" in hn:
                        target_ci = i
                        break

        # Последняя попытка: поиск по значимым токенам запроса
        if target_ci is None:
            for tok in re.findall(r"[a-zа-я0-9_]+", msg_l):
                if len(tok) < 4:
                    continue
                ci = _find_col(headers, tok)
                if ci is not None:
                    target_ci = ci
                    break

        if target_ci is not None:
            nums = _col_nums(rows, target_ci)
            if not nums:
                return {
                    "action": "message",
                    "table": current_table,
                    "ai_message": f"Колонка «{headers[target_ci]}» не содержит числовых значений для суммирования."
                }
            total = sum(nums)
            pretty = f"{total:,.2f}".replace(",", " ").replace(".00", "")
            return {
                "action": "message",
                "table": current_table,
                "ai_message": f"Сумма по колонке «{headers[target_ci]}»: {pretty}."
            }

    # ── АГРЕГИРУЮЩИЕ СТРОКИ (СУММА, СРЕДНЕЕ, МЕДИАНА, МИН, МАКС, СЧЁТ) ──
    agg_map = {
        ("сумм","итог","total","sum"):          ("сумма",  lambda nums: round(sum(nums),2)),
        ("средн","average","mean","avg"):        ("среднее",lambda nums: round(sum(nums)/len(nums),2) if nums else 0),
        ("медиан","median"):                     ("медиана",lambda nums: round(statistics.median(nums),2) if nums else 0),
        ("максим","наибольш","max"):             ("максимум",max),
        ("миним","наименьш","min"):              ("минимум", min),
        ("количеств","счёт","count","кол-во"):  ("кол-во", lambda nums: len(nums)),
    }
    agg_trig = ("добав","вычисли","посчитай","покажи","строку","вставь")
    if any(t in msg_l for t in agg_trig):
        for keys, (label, fn) in agg_map.items():
            if any(k in msg_l for k in keys):
                agg_row = []
                for ci in range(ncols):
                    nums = _col_nums(rows, ci)
                    if nums:
                        try: agg_row.append(_fmt(fn(nums)))
                        except: agg_row.append("")
                    else:
                        agg_row.append(label.upper() if ci==0 else "")
                return {"action":"transform","table":{"headers":headers,"rows":rows+[agg_row]},
                        "ai_message":f"Добавил строку «{label}» по каждому числовому столбцу"}

    # ── ДОБАВИТЬ КОЛОНКУ С АГРЕГАТОМ ─────────────────────────────────────
    add_col_agg = re.search(
        r"добав[а-я]+\s+(?:колонку|столбец|поле)\s+(?:с\s+)?(?:суммой|средним|медианой|максимумом|минимумом|процентом|процент)",
        msg_l
    )
    if add_col_agg:
        pass  # обрабатывается выше через agg_map

    # ── ДОБАВИТЬ КОЛОНКУ % ОСВОЕНИЯ ───────────────────────────────────────
    if ("освоени" in msg_l or "% освоени" in msg_l) and ("добав" in msg_l or "вычисли" in msg_l or "рассчитай" in msg_l):
        lim_idx = next((i for i,h in enumerate(headers) if "лимит" in h.lower()),None)
        spd_idx = next((i for i,h in enumerate(headers) if "расход" in h.lower() or "исполнен" in h.lower() or "выплат" in h.lower()),None)
        if lim_idx is not None and spd_idx is not None:
            new_h = headers + ["% освоения"]
            new_rows = []
            for r in rows:
                lim = _to_num(r[lim_idx] if lim_idx<len(r) else "")
                spd = _to_num(r[spd_idx] if spd_idx<len(r) else "")
                pct = round(spd/lim*100,2) if (lim and lim>0 and spd is not None) else 0
                new_rows.append(list(r)+[str(pct)+"%"])
            return {"action":"transform","table":{"headers":new_h,"rows":new_rows},
                    "ai_message":"Добавил колонку % освоения"}

    # ── ДОБАВИТЬ РАНГ-КОЛОНКУ ─────────────────────────────────────────────
    if re.search(r"добав[а-я]+\s+(?:колонку\s+)?(?:ранг|rank|рейтинг|место)", msg_l):
        m = re.search(r"(?:по\s+)(.+?)(?:\s+убыв|\s+возраст|$)", msg_l)
        col_hint = m.group(1).strip() if m else ""
        ci = _find_col(headers, col_hint) if col_hint else next(
            (i for i,h in enumerate(headers) if any(k in h.lower() for k in ("лимит","расход","сумм","выплат"))), None
        )
        if ci is not None:
            reverse = "убыв" in msg_l
            nums = [(i, _to_num(r[ci] if ci<len(r) else "")) for i,r in enumerate(rows)]
            sorted_idx = sorted(nums, key=lambda x: (x[1] is None, -(x[1] or 0) if reverse else (x[1] or 0)))
            ranks = {}
            for rank,(i,_) in enumerate(sorted_idx,1): ranks[i]=rank
            new_h = headers+["Ранг"]
            new_rows = [list(r)+[str(ranks.get(i,""))] for i,r in enumerate(rows)]
            return {"action":"transform","table":{"headers":new_h,"rows":new_rows},
                    "ai_message":f"Добавил ранг по «{headers[ci]}»"}

    # ── СОРТИРОВКА ────────────────────────────────────────────────────────
    sort_re = re.search(r"(?:отсортируй|сортируй|упорядочи|sort)(?:[а-я]*)?\s+(?:таблицу\s+)?(?:по\s+)?(.+?)(?:\s*$)", msg_l)
    if sort_re and rows:
        reverse = any(w in msg_l for w in ("убыв","desc","от большего","по уменьш"))
        col_hint = sort_re.group(1).strip()
        # Убираем хвосты "по убыванию/возрастанию"
        col_hint = re.sub(r"\s+(?:по\s+)?(?:убыв|возраст|desc|asc|от\s+большего|от\s+меньшего).*$","", col_hint).strip()
        ci = _find_col(headers, col_hint)
        if ci is not None:
            def skey(r):
                v = r[ci] if ci<len(r) else ""
                n = _to_num(v)
                return (0,n) if n is not None else (1,str(v).lower())
            return {"action":"transform","table":{"headers":headers,"rows":sorted(rows,key=skey,reverse=reverse)},
                    "ai_message":f"Отсортировал по «{headers[ci]}» {'↓' if reverse else '↑'}"}

    # ── УДАЛИТЬ ПУСТЫЕ СТРОКИ ────────────────────────────────────────────
    if re.search(r"(?:убери|удали|скрой)\s+пустые?\s+строки?",msg_l):
        filtered=[r for r in rows if any(str(v).strip() for v in r)]
        return {"action":"transform","table":{"headers":headers,"rows":filtered},
                "ai_message":f"Удалил пустые строки. Осталось: {len(filtered)}"}

    # ── ДЕДУПЛИКАЦИЯ ─────────────────────────────────────────────────────
    if re.search(r"(?:убери|удали|исключи)\s+дублик",msg_l) or "дедупликац" in msg_l:
        seen=set(); deduped=[]
        for r in rows:
            key=tuple(r)
            if key not in seen: seen.add(key); deduped.append(r)
        return {"action":"transform","table":{"headers":headers,"rows":deduped},
                "ai_message":f"Удалил дубликаты. Осталось: {len(deduped)} строк"}

    # ── ТРАНСПОНИРОВАТЬ ───────────────────────────────────────────────────
    if "транспониру" in msg_l or "transpose" in msg_l:
        if rows:
            t=list(map(list,zip(*rows)))
            return {"action":"transform","table":{"headers":[f"Строка {i+1}" for i in range(len(rows))],"rows":t},
                    "ai_message":"Таблица транспонирована"}

    # ── ПЕРЕИМЕНОВАТЬ СТОЛБЕЦ ─────────────────────────────────────────────
    m = re.search(r"переименуй\s+(?:столбец|колонку|поле)\s+[\"']?(.+?)[\"']?\s+(?:в|на)\s+[\"']?(.+?)[\"']?\s*$", msg_l)
    if m:
        old_hint = m.group(1).strip(); new_name = m.group(2).strip()
        ci = _find_col(headers, old_hint)
        if ci is not None:
            new_h = list(headers); new_h[ci] = new_name
            return {"action":"transform","table":{"headers":new_h,"rows":rows},
                    "ai_message":f"«{headers[ci]}» → «{new_name}»"}

    # ── ВЫЧИСЛИТЬ КОЛОНКУ ─────────────────────────────────────────────────
    # "добавь столбец = [col1] / [col2] * 100"
    m = re.search(r"добав[а-я]+\s+(?:столбец|колонку|поле)\s+[\"']?(.+?)[\"']?\s*=\s*(.+)$", msg_l)
    if m:
        new_col_name = m.group(1).strip().title()
        formula = m.group(2).strip()
        # Ищем col1 op col2
        fm = re.search(r"[\"']?(.+?)[\"']?\s*([+\-*/])\s*[\"']?(.+?)[\"']?\s*(?:\*\s*(\d+))?$", formula)
        if fm:
            ci1 = _find_col(headers, fm.group(1).strip())
            op  = fm.group(2)
            ci2 = _find_col(headers, fm.group(3).strip())
            mult = float(fm.group(4)) if fm.group(4) else 1
            if ci1 is not None and ci2 is not None:
                new_h = headers+[new_col_name]; new_rows=[]
                for r in rows:
                    a=_to_num(r[ci1] if ci1<len(r) else ""); b=_to_num(r[ci2] if ci2<len(r) else "")
                    try:
                        if op=="+" : res=_fmt((a+b)*mult)
                        elif op=="-": res=_fmt((a-b)*mult)
                        elif op=="*": res=_fmt((a*b)*mult)
                        elif op=="/": res=_fmt((a/b)*mult) if b else ""
                        else: res=""
                    except: res=""
                    new_rows.append(list(r)+[res])
                return {"action":"transform","table":{"headers":new_h,"rows":new_rows},
                        "ai_message":f"Добавил вычисляемый столбец «{new_col_name}»"}

    # ── СТАТИСТИЧЕСКАЯ СВОДКА ─────────────────────────────────────────────
    if re.search(r"(?:статистик|сводк|describe|info|покажи\s+статистику)", msg_l):
        stat_rows=[]
        for ci,h in enumerate(headers):
            nums=_col_nums(rows,ci)
            if nums:
                med=_fmt(statistics.median(nums))
                stat_rows.append([h,str(len(nums)),_fmt(sum(nums)),_fmt(sum(nums)/len(nums)),
                                   med,_fmt(min(nums)),_fmt(max(nums))])
        if stat_rows:
            return {"action":"transform",
                    "table":{"headers":["Столбец","Кол-во","Сумма","Среднее","Медиана","Мин","Макс"],"rows":stat_rows},
                    "ai_message":"Статистическая сводка по числовым столбцам"}

    return None  # передать GigaChat


def _classify_intent(message: str, has_session: bool) -> dict:
    """Классифицирует намерение пользователя используя Intent промпт."""
    try:
        intent_prompt = build_intent_prompt()
        context = f"Сообщение пользователя: {message}\nЕсть ли активная сессия с таблицей: {'да' if has_session else 'нет'}"
        raw = gc.chat([
            {"role": "system", "content": intent_prompt},
            {"role": "user", "content": context}
        ])
        result = _extract_json(raw)
        return result
    except Exception:
        return {"intent": "clarify", "confidence": 0.0, "reason": "Ошибка классификации"}


def apply_patch(table_state: dict, patch: dict) -> dict:
    """
    Применяет patch-план к состоянию таблицы детерминированно.
    
    Возвращает новое состояние таблицы: {"headers": [...], "rows": [[...]]}
    """
    operation = patch.get("operation", "")
    params = patch.get("params", {})
    headers = list(table_state.get("headers", []))
    rows = [list(r) for r in table_state.get("rows", [])]
    
    if operation == "set_cells":
        cells = params.get("cells", [])
        for cell in cells:
            r, c, value = cell.get("r"), cell.get("c"), cell.get("value")
            if 0 <= r < len(rows) and 0 <= c < len(rows[r]):
                rows[r][c] = value
    
    elif operation == "replace_in_range":
        range_spec = params.get("range", {})
        r1, c1 = range_spec.get("r1", 0), range_spec.get("c1", 0)
        r2, c2 = range_spec.get("r2", len(rows)), range_spec.get("c2", len(headers))
        find = params.get("find", "")
        replace = params.get("replace", "")
        
        for r_idx in range(max(0, r1), min(len(rows), r2 + 1)):
            for c_idx in range(max(0, c1), min(len(headers), c2 + 1)):
                if c_idx < len(rows[r_idx]):
                    cell_val = str(rows[r_idx][c_idx] or "")
                    rows[r_idx][c_idx] = cell_val.replace(find, replace)
    
    elif operation == "delete_rows_where":
        condition = params.get("condition", {})
        column = condition.get("column")
        value = condition.get("value")
        operator = condition.get("operator", "==")
        
        if column and column in headers:
            col_idx = headers.index(column)
            new_rows = []
            for row in rows:
                cell_val = str(row[col_idx] if col_idx < len(row) else "").lower()
                target_val = str(value or "").lower()
                
                should_delete = False
                if operator == "contains":
                    should_delete = target_val in cell_val
                elif operator == "==":
                    should_delete = cell_val == target_val
                elif operator == "!=":
                    should_delete = cell_val != target_val
                
                if not should_delete:
                    new_rows.append(row)
            rows = new_rows
    
    elif operation == "delete_columns":
        cols_to_delete = params.get("columns", [])
        keep_indices = [i for i, h in enumerate(headers) if h not in cols_to_delete]
        headers = [headers[i] for i in keep_indices]
        rows = [[row[i] if i < len(row) else "" for i in keep_indices] for row in rows]
    
    elif operation == "keep_columns":
        cols_to_keep = params.get("columns", [])
        keep_indices = [i for i, h in enumerate(headers) if h in cols_to_keep]
        headers = [headers[i] for i in keep_indices]
        rows = [[row[i] if i < len(row) else "" for i in keep_indices] for row in rows]
    
    elif operation == "add_rows":
        count = params.get("count", 1)
        for _ in range(count):
            rows.append([""] * len(headers))
    
    elif operation == "add_columns":
        col_names = params.get("names", [])
        for name in col_names:
            headers.append(name)
            for row in rows:
                row.append("")
    
    elif operation == "sort_rows":
        sort_by = params.get("by", "")
        descending = params.get("desc", False)
        if sort_by in headers:
            col_idx = headers.index(sort_by)
            def sort_key(row):
                val = row[col_idx] if col_idx < len(row) else ""
                num_val = _to_num(val)
                return (num_val if num_val is not None else float('inf'), str(val))
            rows = sorted(rows, key=sort_key, reverse=descending)
    
    elif operation == "filter_rows":
        filter_col = params.get("column", "")
        filter_val = params.get("value", "")
        filter_op = params.get("operator", "contains")
        
        if filter_col in headers:
            col_idx = headers.index(filter_col)
            new_rows = []
            for row in rows:
                cell_val = str(row[col_idx] if col_idx < len(row) else "").lower()
                target = str(filter_val).lower()
                
                match = False
                if filter_op == "contains":
                    match = target in cell_val
                elif filter_op == "==":
                    match = cell_val == target
                elif filter_op == "!=":
                    match = cell_val != target
                
                if match:
                    new_rows.append(row)
            rows = new_rows
    
    elif operation == "rename_column":
        old_name = params.get("old", "")
        new_name = params.get("new", "")
        if old_name in headers:
            idx = headers.index(old_name)
            headers[idx] = new_name
    
    elif operation == "compute_column":
        new_col = params.get("name", "")
        formula = params.get("formula", {})
        headers.append(new_col)
        
        for row in rows:
            result = ""
            try:
                col1 = formula.get("col1", "")
                col2 = formula.get("col2", "")
                op = formula.get("op", "+")
                
                if col1 in headers and col2 in headers:
                    idx1 = headers.index(col1)
                    idx2 = headers.index(col2)
                    val1 = _to_num(row[idx1] if idx1 < len(row) else "")
                    val2 = _to_num(row[idx2] if idx2 < len(row) else "")
                    
                    if val1 is not None and val2 is not None:
                        if op == "+":
                            result = str(val1 + val2)
                        elif op == "-":
                            result = str(val1 - val2)
                        elif op == "*":
                            result = str(val1 * val2)
                        elif op == "/" and val2 != 0:
                            result = str(val1 / val2)
            except Exception:
                pass
            row.append(result)
    
    return {"headers": headers, "rows": rows}


# ── API ─────────────────────────────────────────────────────────────────────

def _add_pct(headers,rows):
    if "limit_amount" in headers and "spend_amount" in headers:
        li=headers.index("limit_amount"); si=headers.index("spend_amount")
        headers=headers+["execution_percent"]; nr=[]
        for row in rows:
            try:
                lim=float(row[li]) if row[li] else 0
                spd=float(row[si]) if row[si] else 0
                pct=round(spd/lim*100,2) if lim else 0
                nr.append(list(row)+[str(pct)+"%"])
            except: nr.append(list(row)+[""])
        return headers,nr
    return headers,rows

@app.get("/health")
def health():
    return {
        "status": "ok",
        "metrics": METRICS,
        "cache_items": len(QUERY_CACHE),
    }

@app.get("/api/schema")
def get_schema():
    return {"schema": get_schema_context()}

@app.post("/api/query")
def api_query(req: QueryRequest):
    schema=get_schema_context()
    try:
        raw=gc.chat([{"role":"system","content":build_query_prompt(schema)},{"role":"user","content":req.query}])
    except Exception as e:
        msg = str(e)
        if "Illegal header value" in msg or "Basic " in msg:
            raise HTTPException(502, "AI-сервис не настроен: проверьте ключ GigaChat в .env")
        if "429" in msg:
            raise HTTPException(429, "Сервис AI временно перегружен. Попробуйте повторить запрос через 10-20 секунд.")
        raise HTTPException(502, "GigaChat: " + msg)
    try: plan=_extract_json(raw)
    except Exception as e: raise HTTPException(422,"Bad JSON: "+str(e))
    plan["_user_query"] = req.query
    plan = _normalize_query_plan(plan)
    plan.pop("_user_query", None)
    ai_comment=plan.pop("ai_comment","Данные получены")
    try: headers,rows=database.execute_plan(plan)
    except ValueError as e: raise HTTPException(400,str(e))
    except Exception as e: raise HTTPException(500,str(e))

    headers,rows=_add_pct(headers,rows)
    # Fallback: если LLM-план не дал строк, пробуем сквозной поиск по всем таблицам.
    if not rows:
        try:
            fh, fr = database.search_all_tables_any_text(req.query, total_limit=3000)
            if fr:
                fallback_plan = {"fallback": "all_tables_any_text"}
                fallback_comment = "Данные найдены сквозным поиском по всем источникам (РЧБ, Соглашения, ГЗ, БУАУ)."
                session = _session_create(fallback_plan, fh, fr, fallback_comment)
                qid = _cache_put(fh, fr, fallback_plan, fallback_comment)
                page_size = max(50, min(int(req.page_size or 300), 2000))
                page_rows = fr[:page_size]
                return {
                    "columns": _labels(fh),
                    "rows": page_rows,
                    "ai_comment": fallback_comment,
                    "plan": fallback_plan,
                    "query_id": qid,
                    "session_id": session.session_id,
                    "table_version": session.version,
                    "total_rows": len(fr),
                    "offset": 0,
                    "page_size": page_size,
                    "has_more": len(fr) > len(page_rows),
                }
        except Exception:
            pass
    if not rows:
        raise HTTPException(404, "Такие данные не найдены. Попробуйте сформулировать запрос по-другому.")
    
    session = _session_create(plan, headers, rows, ai_comment)
    qid = _cache_put(headers, rows, plan, ai_comment)
    
    page_size = max(50, min(int(req.page_size or 300), 2000))
    page_rows = rows[:page_size]
    return {
        "columns":_labels(headers),
        "rows":page_rows,
        "ai_comment":ai_comment,
        "plan":plan,
        "query_id": qid,
        "session_id": session.session_id,
        "table_version": session.version,
        "total_rows": len(rows),
        "offset": 0,
        "page_size": page_size,
        "has_more": len(rows) > len(page_rows),
    }


@app.post("/api/query/page")
def api_query_page(req: QueryPageRequest):
    cached = _cache_get(req.query_id)
    if not cached:
        raise HTTPException(404, "Сессия результатов истекла. Выполните запрос заново.")
    rows = cached.get("rows", [])
    total = len(rows)
    offset = max(0, int(req.offset or 0))
    limit = max(1, min(int(req.limit or 300), 2000))
    if total > 0 and offset >= total:
        raise HTTPException(400, "Смещение страницы вне диапазона. Выполните запрос заново.")
    page_rows = rows[offset: offset + limit]
    return {
        "query_id": req.query_id,
        "offset": offset,
        "limit": limit,
        "rows": page_rows,
        "total_rows": total,
        "has_more": (offset + len(page_rows)) < total,
    }

@app.post("/api/chat")
def api_chat(req: ChatRequest):
    session = _session_get(req.session_id) if req.session_id else None
    cached = _cache_get(req.query_id)
    effective_table = req.current_table
    
    if session:
        effective_table = session.table_state
    elif cached:
        effective_table = {"headers": cached.get("headers", []), "rows": cached.get("rows", [])}

    # Гарантированно не дергаем LLM на приветствиях.
    if _is_greeting(req.message):
        response = {"action": "message", "table": req.current_table, "ai_message": "Привет! Готов помочь с таблицей.", "query_id": req.query_id}
        if session:
            response["session_id"] = session.session_id
            response["table_version"] = session.version
        return response

    effective_message = _resolve_followup_message(req.message, req.history)
    selection_clusters = req.selection_clusters or []
    
    # Try local_transform first (быстрые команды без LLM)
    local_result = _local_transform(effective_table, effective_message, selection_clusters)
    if local_result is not None:
        if local_result.get("action") in ("transform", "query"):
            tbl = local_result.get("table") or {}
            canonical_headers = _normalize_headers_to_canonical(tbl.get("headers", []))
            qid = _cache_put(
                canonical_headers,
                tbl.get("rows", []),
                {"source": "chat_local"},
                local_result.get("ai_message", "Готово")
            )
            local_result["query_id"] = qid
            
            if session:
                session.update_table(canonical_headers, tbl.get("rows", []))
                session.add_patch({"type": "local_transform", "message": req.message})
                local_result["session_id"] = session.session_id
                local_result["table_version"] = session.version
        else:
            local_result["query_id"] = req.query_id
            local_result["table"] = req.current_table
            if session:
                local_result["session_id"] = session.session_id
                local_result["table_version"] = session.version
        return local_result
    
    # NEW: Intent-based routing (если включена сессия)
    if session and session.version > 0:
        intent_result = _classify_intent(effective_message, has_session=True)
        intent = intent_result.get("intent", "clarify")
        confidence = intent_result.get("confidence", 0.0)
        
        if confidence < 0.5:
            return {
                "action": "message",
                "ai_message": f"Не уверен, что правильно понял: {intent_result.get('reason', 'уточните запрос')}. Можете переформулировать?",
                "query_id": req.query_id,
                "session_id": session.session_id,
                "table_version": session.version
            }
        
        if intent == "question":
            pass
        
        elif intent == "query_refine" or intent == "data_append":
            pass
        
        elif intent == "table_patch":
            pass

    # Спец-кейс: "посчитай сумму лимитов по всем данным" — считаем напрямую из БД.
    eff_l = (effective_message or "").lower()
    if ("сумм" in eff_l and "лимит" in eff_l and ("всем доступным данным" in eff_l or "по всем данным" in eff_l or "считай все" in eff_l)):
        try:
            h, r = database.execute_plan({
                "sources": ["mart_rchb"],
                "columns": ["limit_amount"],
                "limit": 200000,
            })
            ci = 0 if h else None
            total = 0.0
            cnt = 0
            if ci is not None:
                for row in r:
                    v = _to_num(row[ci] if ci < len(row) else "")
                    if v is not None:
                        total += v
                        cnt += 1
            pretty = f"{total:,.2f}".replace(",", " ").replace(".00", "")
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": f"Сумма по колонке лимитов по всем доступным данным: {pretty} руб. (учтено строк: {cnt}).",
                "query_id": req.query_id
            }
        except Exception:
            pass

    # Если это вопрос (а не команда редактирования) — отвечаем обычным текстом.
    if _is_question_text(effective_message) and not _is_table_edit_intent(effective_message):
        rows_n = len(req.current_table.get("rows", []))
        msg_l = (effective_message or "").strip().lower()
        if "это все" in msg_l or "все данные" in msg_l:
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": (
                    f"Сейчас в таблице {rows_n} строк(и): это текущая выборка по вашему запросу. "
                    "Если нужно шире, уточните период/территорию или напишите: "
                    "«покажи максимально полную выборку без дополнительных ограничений»."
                ),
                "query_id": req.query_id
            }
        schema = get_schema_context()
        try:
            txt = _plain_chat_answer(effective_message, effective_table, schema, req.history, selection_clusters)
            return {"action": "message", "table": req.current_table, "ai_message": txt, "query_id": req.query_id}
        except Exception as e:
            msg = str(e)
            if "429" in msg:
                return {
                    "action": "message",
                    "table": req.current_table,
                    "ai_message": "Сервис AI временно перегружен (лимит запросов). Повторите через 10-20 секунд."
                }
            if "Illegal header value" in msg or "Basic " in msg:
                return {
                    "action": "message",
                    "table": req.current_table,
                    "ai_message": "AI-сервис не настроен: проверьте ключ GigaChat в .env."
                }
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": "Не удалось получить ответ на вопрос. Попробуйте переформулировать."
            }

    schema=get_schema_context()
    cur_json=json.dumps({"headers":effective_table.get("headers",[]),"rows":effective_table.get("rows",[])[:20]},ensure_ascii=False)
    hist_txt = _history_to_text(req.history)
    ranges_txt = ", ".join([str(cl.get("range_label", "")) for cl in selection_clusters if isinstance(cl, dict) and cl.get("range_label")]) or "нет"
    sel_preview = _selection_preview(selection_clusters)
    user_msg=(
        "История диалога:\n" + hist_txt +
        "\n\nВыделенные диапазоны: " + ranges_txt +
        "\nВыделения preview:\n" + sel_preview +
        "\n\nТекущая таблица:\n" + cur_json +
        "\n\nЗапрос пользователя: " + effective_message
    )
    try:
        raw=gc.chat([{"role":"system","content":build_chat_prompt(schema)},{"role":"user","content":user_msg}])
    except Exception as e:
        msg = str(e)
        if "Illegal header value" in msg or "Basic " in msg:
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": "AI-сервис не настроен: проверьте ключ GigaChat в .env.",
                "query_id": req.query_id
            }
        if "429" in msg:
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": "Сервис AI временно перегружен (лимит запросов). Повторите через 10-20 секунд.",
                "query_id": req.query_id
            }
        raise HTTPException(502, "GigaChat: " + msg)
    try: result=_extract_json(raw)
    except Exception:
        # Если JSON сломан — делаем второй проход в текстовом режиме, чтобы не возвращать ошибку формата.
        try:
            txt = _plain_chat_answer(effective_message, effective_table, schema, req.history, selection_clusters)
            return {"action":"message","table":req.current_table,"ai_message":txt,"query_id": req.query_id}
        except Exception:
            raw_s = (raw or "").strip()
            return {
                "action":"message",
                "table":req.current_table,
                "ai_message": raw_s[:500] if raw_s else "Не удалось обработать ответ AI. Попробуйте уточнить запрос.",
                "query_id": req.query_id
            }
    action=result.get("action","transform"); ai_msg=result.get("ai_message","Готово")
    if action=="query":
        plan=result.get("plan",{}); merge_on=result.get("merge_on")
        try: nh,nr=database.execute_plan(plan)
        except Exception as e: return {"action":"message","table":req.current_table,"ai_message":"Ошибка: "+str(e), "query_id": req.query_id}
        cur_h = effective_table.get("headers",[])
        cur_r = effective_table.get("rows",[])
        cur_h_canon = _normalize_headers_to_canonical(cur_h)
        nh_canon = _normalize_headers_to_canonical(nh)
        if merge_on:
            merge_on = _to_canonical_header(merge_on)
        if merge_on and merge_on in cur_h_canon and merge_on in nh_canon:
            add_h = [nh[i] for i, hc in enumerate(nh_canon) if hc not in set(cur_h_canon)]
            nki = nh_canon.index(merge_on)
            cki = cur_h_canon.index(merge_on)
            nlookup={}
            for r in nr:
                if nki < len(r):
                    nlookup[r[nki]] = r
            merged=[]
            for row in cur_r:
                kv=row[cki] if cki<len(row) else ""
                extra=nlookup.get(kv,[""]*len(nh))
                merged.append(list(row)+[extra[nh.index(h)] for h in add_h])
            new_headers = cur_h + _labels(add_h)
            qid = _cache_put(_normalize_headers_to_canonical(new_headers), merged, {"source": "chat_query_merge"}, ai_msg)
            return {"action":"transform","table":{"headers":new_headers,"rows":merged},"ai_message":ai_msg, "query_id": qid}
        qid = _cache_put(nh, nr, {"source": "chat_query"}, ai_msg)
        return {"action":"query","table":{"headers":_labels(nh),"rows":nr},"ai_message":ai_msg, "query_id": qid}
    elif action=="transform":
        tbl = result.get("new_table", effective_table)
        qid = _cache_put(
            _normalize_headers_to_canonical(tbl.get("headers", [])),
            tbl.get("rows", []),
            {"source": "chat_transform"},
            ai_msg
        )
        return {"action":"transform","table":tbl,"ai_message":ai_msg, "query_id": qid}
    # action=="message" или другое — просто текст без изменения таблицы
    return {"action":"message","table":req.current_table,"ai_message":ai_msg, "query_id": req.query_id}

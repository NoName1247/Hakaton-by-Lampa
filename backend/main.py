import json, re, os, sys, math, statistics
from typing import Optional
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import gigachat as gc
import db as database
from schema_context import get_schema_context
from prompts import build_query_prompt, build_chat_prompt

app = FastAPI(title="Lampa Budget API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class QueryRequest(BaseModel):
    query: str
    current_table: Optional[dict] = None

class ChatRequest(BaseModel):
    message: str
    current_table: dict
    history: Optional[list[dict]] = None

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

def _labels(hdrs): return [COLUMN_LABELS.get(h,h) for h in hdrs]
def _norm(s): return re.sub(r"[^a-zа-я0-9]+","",str(s or "").lower().replace("ё","е"))
def _to_num(v):
    try: return float(str(v).replace("%","").replace(",",".").replace(" ","").replace("\xa0",""))
    except: return None

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
    try: return json.loads(raw)
    except:
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


def _plain_chat_answer(message: str, current_table: dict, schema: str, history: Optional[list[dict]] = None) -> str:
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


def _local_transform(current_table, message):
    """Локальная обработка команд без GigaChat. None = передать GigaChat."""
    headers = list(current_table.get("headers", []))
    rows    = [list(r) for r in current_table.get("rows", [])]
    msg  = (message or "").strip()
    msg_l = msg.lower()
    msg_n = _norm(msg)
    ncols = len(headers)
    nrows = len(rows)

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
def health(): return {"status":"ok"}

@app.get("/api/schema")
def get_schema(): return {"schema":get_schema_context()}

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
    if not rows:
        raise HTTPException(404, "Такие данные не найдены. Попробуйте сформулировать запрос по-другому.")
    return {"columns":_labels(headers),"rows":rows,"ai_comment":ai_comment,"plan":plan}

@app.post("/api/chat")
def api_chat(req: ChatRequest):
    # Гарантированно не дергаем LLM на приветствиях.
    if _is_greeting(req.message):
        return {"action": "message", "table": req.current_table, "ai_message": "Привет! Готов помочь с таблицей."}

    effective_message = _resolve_followup_message(req.message, req.history)
    local_result = _local_transform(req.current_table, effective_message)
    if local_result is not None:
        return local_result

    # Спец-кейс: "посчитай сумму лимитов по всем данным" — считаем напрямую из БД.
    eff_l = (effective_message or "").lower()
    if ("сумм" in eff_l and "лимит" in eff_l and ("всем доступным данным" in eff_l or "по всем данным" in eff_l or "считай все" in eff_l)):
        try:
            h, r = database.execute_plan({
                "sources": ["mart_rchb"],
                "columns": ["limit_amount"],
                "limit": 5000,
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
                "ai_message": f"Сумма по колонке лимитов по всем доступным данным: {pretty} руб. (учтено строк: {cnt})."
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
            }
        schema = get_schema_context()
        try:
            txt = _plain_chat_answer(effective_message, req.current_table, schema, req.history)
            return {"action": "message", "table": req.current_table, "ai_message": txt}
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
    cur_json=json.dumps({"headers":req.current_table.get("headers",[]),"rows":req.current_table.get("rows",[])[:20]},ensure_ascii=False)
    hist_txt = _history_to_text(req.history)
    user_msg="История диалога:\n"+hist_txt+"\n\nТекущая таблица:\n"+cur_json+"\n\nЗапрос пользователя: "+effective_message
    try:
        raw=gc.chat([{"role":"system","content":build_chat_prompt(schema)},{"role":"user","content":user_msg}])
    except Exception as e:
        msg = str(e)
        if "Illegal header value" in msg or "Basic " in msg:
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": "AI-сервис не настроен: проверьте ключ GigaChat в .env."
            }
        if "429" in msg:
            return {
                "action": "message",
                "table": req.current_table,
                "ai_message": "Сервис AI временно перегружен (лимит запросов). Повторите через 10-20 секунд."
            }
        raise HTTPException(502, "GigaChat: " + msg)
    try: result=_extract_json(raw)
    except Exception:
        # Если JSON сломан — делаем второй проход в текстовом режиме, чтобы не возвращать ошибку формата.
        try:
            txt = _plain_chat_answer(effective_message, req.current_table, schema, req.history)
            return {"action":"message","table":req.current_table,"ai_message":txt}
        except Exception:
            raw_s = (raw or "").strip()
            return {
                "action":"message",
                "table":req.current_table,
                "ai_message": raw_s[:500] if raw_s else "Не удалось обработать ответ AI. Попробуйте уточнить запрос."
            }
    action=result.get("action","transform"); ai_msg=result.get("ai_message","Готово")
    if action=="query":
        plan=result.get("plan",{}); merge_on=result.get("merge_on")
        try: nh,nr=database.execute_plan(plan)
        except Exception as e: return {"action":"message","table":req.current_table,"ai_message":"Ошибка: "+str(e)}
        cur_h=req.current_table.get("headers",[]); cur_r=req.current_table.get("rows",[])
        if merge_on and merge_on in cur_h and merge_on in nh:
            add_h=[h for h in nh if h not in cur_h]
            nki=nh.index(merge_on); cki=cur_h.index(merge_on)
            nlookup={r[nki]:r for r in nr}
            merged=[]
            for row in cur_r:
                kv=row[cki] if cki<len(row) else ""
                extra=nlookup.get(kv,[""]*len(nh))
                merged.append(list(row)+[extra[nh.index(h)] for h in add_h])
            return {"action":"transform","table":{"headers":cur_h+_labels(add_h),"rows":merged},"ai_message":ai_msg}
        return {"action":"query","table":{"headers":_labels(nh),"rows":nr},"ai_message":ai_msg}
    elif action=="transform":
        return {"action":"transform","table":result.get("new_table",req.current_table),"ai_message":ai_msg}
    # action=="message" или другое — просто текст без изменения таблицы
    return {"action":"message","table":req.current_table,"ai_message":ai_msg}

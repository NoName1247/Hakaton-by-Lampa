import json, re, os, sys
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
    "source_file":"Файл-источник",
}

def _labels(hdrs): return [COLUMN_LABELS.get(h,h) for h in hdrs]

def _norm_text(s):
    return re.sub(r"[^a-zа-я0-9]+", "", (s or "").lower().replace("ё", "е"))

def _extract_first_json_object(text):
    start = text.find("{")
    if start == -1:
        raise ValueError("no JSON object start")
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(text[start:], start=start):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    raise ValueError("no JSON object end")

def _extract_json(text):
    text = re.sub(r"```(?:json)?\s*","",text); text = re.sub(r"```","",text)
    raw = _extract_first_json_object(text)
    try:
        return json.loads(raw)
    except Exception:
        # Частая ошибка LLM: trailing comma перед ] или }
        fixed = re.sub(r",\s*([}\]])", r"\1", raw)
        return json.loads(fixed)

def _remove_columns_from_table(current_table, message):
    headers = list(current_table.get("headers", []))
    rows = list(current_table.get("rows", []))
    if not headers:
        return None

    message_norm = _norm_text(message)
    remove_words = ("убери", "удали", "скрой", "убрать", "удалить", "скрыть")
    if not any(w in (message or "").lower() for w in remove_words):
        return None

    aliases = {
        "наименованиекфср": ["Наименование КФСР", "kfsr_name"],
        "наименованиекцср": ["Наименование КЦСР", "kcsr_name"],
        "наименованиекцсо": ["Наименование КЦСР", "kcsr_name"],  # частая опечатка
        "кцсо": ["Наименование КЦСР", "kcsr_name"],
        "кцсрнаименование": ["Наименование КЦСР", "kcsr_name"],
    }

    to_remove = set()
    for key, candidates in aliases.items():
        if key in message_norm:
            for c in candidates:
                cn = _norm_text(c)
                for h in headers:
                    if _norm_text(h) == cn:
                        to_remove.add(h)

    # Дополнительное грубое совпадение по словам из команды
    for token in re.findall(r"[a-zа-я0-9]+", (message or "").lower()):
        if len(token) < 6:
            continue
        if token in {"поля", "поле", "колонки", "колонку", "столбцы", "столбец"}:
            continue
        for h in headers:
            hn = _norm_text(h)
            if token in hn:
                to_remove.add(h)

    if not to_remove:
        return None

    keep_indexes = [i for i, h in enumerate(headers) if h not in to_remove]
    if not keep_indexes:
        return {
            "action": "message",
            "table": current_table,
            "ai_message": "Нельзя удалить все колонки сразу. Уточни, какие оставить.",
        }

    new_headers = [headers[i] for i in keep_indexes]
    new_rows = [[row[i] if i < len(row) else "" for i in keep_indexes] for row in rows]
    return {
        "action": "transform",
        "table": {"headers": new_headers, "rows": new_rows},
        "ai_message": "Убрал поля: " + ", ".join(sorted(to_remove)),
    }

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
            except Exception: nr.append(list(row)+[""])
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
    except Exception as e: raise HTTPException(502,"GigaChat: "+str(e))
    try: plan=_extract_json(raw)
    except Exception as e: raise HTTPException(422,"Bad JSON: "+str(e))
    ai_comment=plan.pop("ai_comment","Данные получены")
    try: headers,rows=database.execute_plan(plan)
    except ValueError as e: raise HTTPException(400,str(e))
    except Exception as e: raise HTTPException(500,str(e))
    headers,rows=_add_pct(headers,rows)
    return {"columns":_labels(headers),"rows":rows,"ai_comment":ai_comment+("" if rows else " (не найдено)"),"plan":plan}

@app.post("/api/chat")
def api_chat(req: ChatRequest):
    local_result = _remove_columns_from_table(req.current_table, req.message)
    if local_result is not None:
        return local_result

    schema=get_schema_context()
    cur_json=json.dumps({"headers":req.current_table.get("headers",[]),"rows":req.current_table.get("rows",[])[:20]},ensure_ascii=False)
    user_msg="Текущая таблица:\n"+cur_json+"\n\nЗапрос: "+req.message
    try:
        raw=gc.chat([{"role":"system","content":build_chat_prompt(schema)},{"role":"user","content":user_msg}])
    except Exception as e: raise HTTPException(502,"GigaChat: "+str(e))
    try: result=_extract_json(raw)
    except Exception: return {"action":"message","table":req.current_table,"ai_message":raw[:800]}
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
    return {"action":"message","table":req.current_table,"ai_message":ai_msg}

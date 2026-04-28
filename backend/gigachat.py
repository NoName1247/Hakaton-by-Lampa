"""
Клиент GigaChat API: OAuth 2.0 + /chat/completions
Токен авто-обновляется за 60 секунд до истечения.
"""
import os
import time
import uuid
import httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

GIGACHAT_AUTH_KEY = os.getenv("GIGACHAT_AUTHORIZATION_KEY", "")
GIGACHAT_SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
GIGACHAT_OAUTH_URL = os.getenv("GIGACHAT_OAUTH_URL", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth")
GIGACHAT_API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

_token_cache: dict = {"access_token": "", "expires_at": 0}


def _get_token() -> str:
    now = time.time() * 1000  # ms
    if _token_cache["access_token"] and _token_cache["expires_at"] - 60_000 > now:
        return _token_cache["access_token"]

    resp = httpx.post(
        GIGACHAT_OAUTH_URL,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "RqUID": str(uuid.uuid4()),
            "Authorization": f"Basic {GIGACHAT_AUTH_KEY}",
        },
        data={"scope": GIGACHAT_SCOPE},
        verify=False,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = data.get("expires_at", int(time.time() * 1000) + 1_800_000)
    return _token_cache["access_token"]


def chat(messages: list[dict], model: str = "GigaChat", temperature: float = 0.2) -> str:
    """Отправить список сообщений, вернуть текст ответа ассистента."""
    token = _get_token()
    resp = httpx.post(
        GIGACHAT_API_URL,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json={
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
        },
        verify=False,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

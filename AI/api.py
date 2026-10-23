"""Proste API do modelu Bielik (przez Ollame).

GET  /health  czy Ollama dziala i czy model jest pobrany
POST /chat    {"prompt": "...", "system": "...", "temperature": 0.2, "max_tokens": 512}
              -> {"answer": "...", "model": "...", "latency_ms": 1234}
Jesli ustawiony API_KEY, /chat wymaga naglowka X-API-Key.
"""
import os
import time

import requests
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("MODEL", "hf.co/speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q4_K_M")
API_KEY = os.environ.get("API_KEY", "")
TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", "600"))  # model na CPU potrafi myslec dlugo

app = FastAPI(title="Bielik API")


class ChatRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=20000)
    system: str | None = None
    temperature: float = Field(0.2, ge=0, le=2)
    max_tokens: int = Field(512, ge=1, le=4096)


class ChatResponse(BaseModel):
    answer: str
    model: str
    latency_ms: int


def check_key(x_api_key: str | None = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="brak albo zly naglowek X-API-Key")


@app.get("/health")
def health():
    try:
        tags = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5).json()
    except requests.RequestException as error:
        raise HTTPException(status_code=503, detail=f"Ollama niedostepna: {error}")
    models = [model["name"] for model in tags.get("models", [])]
    return {"ollama": "ok", "model": MODEL, "model_pulled": MODEL in models, "models": models}


@app.post("/chat", response_model=ChatResponse, dependencies=[Depends(check_key)])
def chat(request: ChatRequest):
    messages = [{"role": "system", "content": request.system}] if request.system else []
    messages.append({"role": "user", "content": request.prompt})
    started = time.monotonic()
    try:
        response = requests.post(f"{OLLAMA_URL}/api/chat", json={
            "model": MODEL,
            "messages": messages,
            "stream": False,
            "options": {"temperature": request.temperature, "num_predict": request.max_tokens},
        }, timeout=TIMEOUT)
        response.raise_for_status()
        answer = response.json()["message"]["content"].strip()
    except (requests.RequestException, KeyError, ValueError) as error:
        raise HTTPException(status_code=502, detail=f"blad Ollamy: {error}")
    return ChatResponse(answer=answer, model=MODEL, latency_ms=int((time.monotonic() - started) * 1000))

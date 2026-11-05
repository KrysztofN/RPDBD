"""
Bielik (model językowy przez Ollamę) pisze komentarz analityka do biuletynu B1/B2 — ze sprawdzeniem odpowiedzi.

Co robi:
  facts_from_context(ctx)  -> liczby i statusy z biuletynu (jedyne, czego model może użyć)
  ask_bielik(facts)        -> {"streszczenie", "ocena", "przyczyna"} albo None (wtedy zostają teksty z reguł)
  validate(answer, facts)  -> lista problemów (pusta = odpowiedź przyjęta)

Trochę teorii:
  Model językowy pisze płynnie, ale może „dopowiadać” liczby i fakty. Dlatego:
    - dostaje wyłącznie liczby policzone przez B1, B2 i pogodę (JSON), z poleceniem, by nie dodawał innych,
    - odpowiada w ustalonym formacie JSON (Ollama `format: json`),
    - kod sprawdza odpowiedź: format, długość, przyczyna z listy, każda liczba w tekście musi wystąpić w danych.
  Odpowiedź niezgodna -> jedna ponowna próba -> jeśli dalej źle, biuletyn zostaje z tekstami z reguł.
  Ollama słucha tylko lokalnie (127.0.0.1:11434) — raporter działa na tej samej maszynie co model.
"""
from __future__ import annotations

import json
import os
import re
import time

import requests

from lib.log import get_logger

log = get_logger(__name__)

OLLAMA_URL = os.environ.get("RBD_OLLAMA_URL", "http://127.0.0.1:11434")
MODEL = os.environ.get("RBD_BIELIK_MODEL", "hf.co/speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q4_K_M")
CAUSES = ["susza", "upał", "przymrozek", "nadmiar wody", "chłodna wiosna", "przesunięcie sezonu",
          "przyczyna lokalna", "brak problemu"]

SYSTEM = (
    "Jesteś analitykiem rolnym piszącym biuletyn stanu upraw dla powiatu w Polsce. "
    "Piszesz po polsku, rzeczowo, dla analityka (nie dla rolnika). "
    "Korzystasz WYŁĄCZNIE z danych w JSON od użytkownika: nie dodawaj żadnych innych liczb, dat, nazw ani faktów. "
    "Liczby przepisuj dokładnie tak, jak są w danych. Jeśli czegoś nie ma w danych — nie pisz o tym. "
    "Odpowiedz tylko obiektem JSON z polami: "
    '"streszczenie" (3–4 zdania: stan upraw, przebieg sezonu, porównanie z województwem), '
    '"ocena" (2–3 zdania: najbardziej prawdopodobna przyczyna i na czym ją opierasz), '
    f'"przyczyna" (jedno z: {", ".join(CAUSES)}).'
)


def facts_from_context(ctx: dict) -> dict:
    """Liczby i statusy z biuletynu — dokładnie to, co widać w PDF."""
    ws = ctx.get("_weather_summary")
    stages = {r["stage"]: {"data": r["date"], "typowo": r["typical"], "przesunięcie": r["shift"]}
              for r in ctx.get("b2_rows", [])}
    facts = {
        "powiat": ctx["county_name"], "województwo": ctx["voiv_name"], "termin": ctx["period_label"],
        "zieloność_pól_proc_normy": ctx["pct_of_norm"], "stan_upraw": ctx["status_short"],
        "od_ilu_terminów": ctx["status_caption"], "faza_sezonu": ctx["phase_short"],
        "etapy_sezonu": stages, "województwo_gorzej_niż_zwykle": ctx["neighbours_text"],
        "przyczyny_z_reguł": ctx.get("causes", []),
    }
    if ws is not None:
        c = ws["cur"]
        facts["pogoda"] = {
            "opad_w_terminie_mm": round(float(c["precip_mm"])), "parowanie_w_terminie_mm": round(float(c["et0_mm"])),
            "bilans_60_dni_mm": round(ws["kbw_60d"]),
            "bilans_60_dni_typowo_mm": None if ws["kbw_60d_norm"] != ws["kbw_60d_norm"] else round(ws["kbw_60d_norm"]),
            "próg_suszy_mm": -150, "opad_od_kwietnia_mm": round(ws["season_precip"]),
            "dni_upalne_ostatnie_32_dni": ws["hot_recent"], "dni_z_przymrozkiem_od_kwietnia": ws["frost_spring"],
        }
    return facts


_NUM = re.compile(r"-?\d+(?:[.,]\d+)?")


def _numbers(text: str) -> set[float]:
    return {float(n.replace(",", ".")) for n in _NUM.findall(text)}


def validate(answer: dict, facts: dict) -> list[str]:
    """Problemy w odpowiedzi modelu (pusta lista = przyjęta)."""
    problems = []
    for key in ("streszczenie", "ocena", "przyczyna"):
        if not isinstance(answer.get(key), str) or not answer[key].strip():
            problems.append(f"brak pola {key}")
    if problems:
        return problems
    if answer["przyczyna"].strip().lower() not in CAUSES:
        problems.append(f"przyczyna spoza listy: {answer['przyczyna'][:40]}")
    for key, lo, hi in (("streszczenie", 120, 900), ("ocena", 60, 700)):
        if not lo <= len(answer[key]) <= hi:
            problems.append(f"{key}: długość {len(answer[key])} poza {lo}–{hi}")
    allowed = _numbers(json.dumps(facts, ensure_ascii=False))
    allowed |= {abs(a) for a in allowed} | set(range(0, 32))       # dni miesiąca i małe liczebniki są dopuszczalne
    unknown = [n for n in _numbers(answer["streszczenie"] + " " + answer["ocena"])
               if not any(abs(n - a) <= 1 for a in allowed)]
    if unknown:
        problems.append(f"liczby spoza danych: {sorted(unknown)[:5]}")
    return problems


def ask_bielik(facts: dict, retries: int = 1, timeout: int = 900) -> tuple[dict | None, str]:
    """Pyta model; zwraca (odpowiedź albo None, opis wyniku do logu/ClickHouse)."""
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": json.dumps(facts, ensure_ascii=False, indent=1)}]
    for attempt in range(retries + 1):
        t0 = time.time()
        try:
            r = requests.post(f"{OLLAMA_URL}/api/chat", timeout=timeout, json={
                "model": MODEL, "messages": messages, "stream": False, "format": "json",
                "options": {"temperature": 0.2, "num_predict": 700}})
            r.raise_for_status()
            answer = json.loads(r.json()["message"]["content"])
        except Exception as e:                                # brak modelu, timeout, zły JSON
            log.warning("Bielik: próba %d nieudana (%s: %s)", attempt + 1, type(e).__name__, str(e)[:150])
            continue
        problems = validate(answer, facts)
        if not problems:
            log.info("Bielik: odpowiedź przyjęta w %.0f s (przyczyna: %s)", time.time() - t0, answer["przyczyna"])
            return answer, f"ok ({time.time() - t0:.0f} s)"
        log.warning("Bielik: odpowiedź odrzucona (%s) — próba %d", "; ".join(problems), attempt + 1)
        messages += [{"role": "assistant", "content": json.dumps(answer, ensure_ascii=False)},
                     {"role": "user", "content": "Popraw odpowiedź: " + "; ".join(problems)
                      + ". Używaj tylko liczb z danych. Odpowiedz samym JSON."}]
    return None, "odrzucona — teksty z reguł"

# Bielik API

*Proste API do modelu Bielik 11B v3.0 (przez Ollamę) na jednym serwerze.*

```
klient (laptop, notatnik, inny program) ──HTTP──► api.py :8000 ──► Ollama :11434 ──► Bielik 11B v3.0
```

- **Ollama** trzyma model i go uruchamia. Nie ma hasła, więc **nie wystawiamy jej na zewnątrz**.
- **`api.py`** (FastAPI) to jedyne wejście z zewnątrz. Ma klucz dostępu (`X-API-Key`) i prosty format zapytań.

## Pliki

| Plik | Co to |
| --- | --- |
| `api.py` | API: `GET /health`, `POST /chat` |
| `Dockerfile` | obraz API (Python 3.13.12, fastapi 0.142.4, uvicorn 0.54.0, requests 2.34.2) |
| `docker-compose.yml` | Ollama + API w kontenerach (wariant B) |

## Model

Bielik 11B v3.0 z Hugging Face (`speakleash/Bielik-11B-v3.0-Instruct-GGUF`), wariant **Q4_K_M (6,7 GB)**, bo jest najmniejszy i najszybszy na CPU. Inne warianty: Q5_K_M 7,9 GB, Q8_0 11,9 GB. W razie potrzeby jest mniejszy model: `hf.co/speakleash/Bielik-4.5B-v3.0-Instruct-GGUF:Q8_0` (5,1 GB).

## Uruchomienie: wariant A (Ollama już zainstalowana na serwerze)

Najprostszy, skoro Ollama jest już na serwerze. Domyślnie słucha tylko na `127.0.0.1`, więc jest niewidoczna z zewnątrz i tak ma zostać.

```bash
cd ~/RPDBD && git pull

# 1. model (~6,7 GB) i test
ollama pull hf.co/speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q4_K_M
time ollama run hf.co/speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q4_K_M "Napisz jedno zdanie o przymrozkach."

# 2. API w tmux (żeby działało po rozłączeniu SSH)
python3 -m venv ~/venvs/bielik
~/venvs/bielik/bin/pip install fastapi==0.142.4 uvicorn==0.54.0 requests==2.34.2
tmux new -s bielik
cd ~/RPDBD/AI
API_KEY=<wymyśl_klucz> ~/venvs/bielik/bin/uvicorn api:app --host 0.0.0.0 --port 8000
```

## Uruchomienie: wariant B (wszystko w Dockerze)

Nie korzysta z ręcznie zainstalowanej Ollamy. Ma swoją w kontenerze, więc zainstalowaną warto zatrzymać, żeby nie zajmowała RAM-u.

```bash
cd ~/RPDBD/AI
API_KEY=<wymyśl_klucz> docker compose up -d --build
docker compose exec ollama ollama pull hf.co/speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q4_K_M
docker compose logs -f api
```

## Test

```bash
curl http://<IP_serwera>:8000/health
# {"ollama":"ok","model":"hf.co/...","model_pulled":true,...}

curl -X POST http://<IP_serwera>:8000/chat \
  -H "Content-Type: application/json" -H "X-API-Key: <klucz>" \
  -d '{"prompt": "Czym jest wymarzanie ozimin? Odpowiedz w 2 zdaniach."}'
# {"answer":"...","model":"hf.co/...","latency_ms":...}
```

Dokumentacja API w przeglądarce (generuje ją FastAPI): `http://<IP_serwera>:8000/docs`.

## Jak korzystać z innego programu (Python)

```python
import requests

response = requests.post(
    "http://<IP_serwera>:8000/chat",
    headers={"X-API-Key": "<klucz>"},
    json={"prompt": "Opisz w 2 zdaniach ryzyko przymrozku dla rzepaku.",
          "system": "Odpowiadasz po polsku, krótko i rzeczowo.",
          "temperature": 0.2},
    timeout=600,
)
print(response.json()["answer"])
```

Pola zapytania `/chat`:

| Pole | Wymagane | Domyślnie | Co to |
| --- | --- | --- | --- |
| `prompt` | tak | — | pytanie albo polecenie |
| `system` | nie | brak | instrukcja dla modelu (styl, rola) |
| `temperature` | nie | 0.2 | 0 = przewidywalnie, wyżej = bardziej kreatywnie |
| `max_tokens` | nie | 512 | maksymalna długość odpowiedzi |

## Uwagi

- **Klucz `API_KEY`** podajemy przy uruchomieniu (zmienna środowiskowa). Nie wpisujemy go do repo. Bez klucza API jest otwarte dla każdego w sieci uczelni.
- **Port 8000** musi być osiągalny z miejsca, z którego się łączycie (sieć uczelni, VPN). Jeśli nie jest, działa tunel SSH: `ssh -L 8000:localhost:8000 <login>@<serwer>`, a potem `http://localhost:8000` na laptopie.
- **Szybkość na CPU:** pierwsza odpowiedź jest wolniejsza (ładowanie modelu do RAM-u). Czas widać w `latency_ms`. Zapytania są obsługiwane po kolei, więc przy kilku osobach naraz trzeba czekać.
- **RAM:** model Q4_K_M potrzebuje ok. 7–8 GB RAM-u, plus zapas na kontekst.

## Do sprawdzenia przy pierwszym uruchomieniu

- Czas odpowiedzi na CPU serwera (`time ollama run …`). Jeśli jest za wolno, mniejszy Bielik 4.5B.
- Czy odpowiedzi są czyste, po polsku, bez dziwnych znaczników. Ollama bierze szablon czatu z pliku GGUF. Jeśli coś jest nie tak, potrzebny będzie `Modelfile` z szablonem.
- Czy nazwa modelu w `/health` (`model_pulled`) zgadza się z tą, pod którą Ollama zapisała model (`ollama list`).

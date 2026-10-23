# S1 — Potrzeba nawodnienia

## Co robi

W sezonie (21.03–30.09) codziennie sprawdza dla każdego powiatu, czy polom brakuje wody.
Wynik: mapa powiatów — zielony (OK), żółty (ostrzeżenie), czerwony (alarm) — i e-mail przy alarmie.

Brak wody poznajemy po dwóch sygnałach:
1. **Bilans wody z 60 dni** = suma opadu − suma parowania. Mocno ujemny = przez 2 miesiące więcej wyparowało, niż spadło.
2. **Wilgotność gleby** niższa niż zwykle w tym miejscu o tej porze roku.

## Dane

**Pogoda: ERA5-Land** (Copernicus CDS, konto potrzebne). To reanaliza — odtworzenie pogody z przeszłości: model pogodowy poprawiany prawdziwymi pomiarami, bez dziur, dla każdej godziny i kwadratu ~9 km. Wartości to średnie z kwadratu (ekstremy łagodniejsze niż na stacji), wilgotność gleby jest wyliczona, nie zmierzona.

| Zmienna | Po co | Uwagi |
| --- | --- | --- |
| `tp` opad | bilans | metry, narasta od 00 UTC |
| `pev` parowanie potencjalne | bilans (wersja prosta) | metry, ujemne, narasta |
| `t2m`, `d2m`, `u10`, `v10`, `ssrd` | parowanie liczone wzorem FAO-56 (wersja dokładna) | kelwiny, m/s, J/m² (narasta) |
| `swvl1`, `swvl2`, `swvl3` | wilgotność gleby 0–7, 7–28, 28–100 cm | m³/m³ |

- Zakres: 2010–2024, co godzinę, obszar `[55.0, 14.0, 49.0, 24.2]`, siatka 61 × 103.
- Plik: NetCDF, 1 miesiąc = 1 plik, ~67 MB → całość ~9–12 GB.
- Do S1 wystarczą miesiące I–IX (sezon + 60 dni rozgrzewki).

**Dodatkowo:**
- **Tabela punktów** (~4 100): powiat, województwo, % pól (granice GUGiK + WorldCover), wysokość (Open-Meteo, bez klucza).
- **Do sprawdzenia wyników:** raporty suszy IUNG (susza.iung.pulawy.pl).

## Jak działa (kroki)

1. **Pobranie** — `data/download.py`, miesiąc po miesiącu, wznawia po przerwie.
2. **Sprawdzenie pobrania** — czy są wszystkie pliki, otwierają się, mają 10 zmiennych.
3. **Zamiana na tabelę** — z „kostki” (godzina × siatka) robimy wiersze: punkt × godzina. Tylko zmiana kształtu i odwrócenie narastania (opad, parowanie, promieniowanie). **Bez czyszczenia** — to robi strumień.
4. **Normy wilgotności** — dla każdego punktu i każdej z 36 dekad roku: p10 i p20 z 15 lat.
5. **Prototyp w pandas** na 2018 — bilans, stany, mapa powiatów; porównanie z IUNG; dobór progów.
6. **Strumień** — replay wysyła godzinę po godzinie do Kafki → Spark Streaming:
   - **walidacja każdej wiadomości** (pola, zakresy, duble, spóźnienia) → złe do kolejki `weather.hourly.dlq`,
   - dobre → sumy z 60 dni na punkt → stan punktu → stan powiatu.
   - Replay celowo psuje ~0,1% wiadomości (ERA5-Land jest prawie bez błędów), żeby pokazać, że walidacja działa.
   - Wynik musi być taki sam jak w prototypie (kroku 5).
7. **Wyniki** → ClickHouse → mapa w Grafanie, e-mail przy alarmie.

## Reguły (wszystkie w konfiguracji)

- Liczymy tylko punkty z ≥ 20% pól.
- **Ostrzeżenie:** bilans 60 dni < −100 mm **lub** wilgotność < p20.
- **Alarm:** bilans 60 dni < −150 mm **i** wilgotność < p10.
- **Powiat** (główny poziom alertów): ostrzeżenie, gdy ≥ 30% punktów w ostrzeżeniu; alarm, gdy ≥ 50% w alarmie. Powiat musi mieć **≥ 3 punkty z polami** — inaczej na mapie szary „za mało pól” (90 z 364 powiatów, głównie miasta i tereny leśne/górskie).
- **Województwo:** ten sam wzór, jako widok zbiorczy.
- **Punkt** (kratka ~11 × 7 km): stan widoczny jako warstwa szczegółowa na mapie.

## Test

- 2018 i 2019 (susze): alarmy od maja–czerwca, najpierw zachód i centrum Polski.
- Rok mokry (2010 lub 2017): brak lub pojedyncze ostrzeżenia.
- Zgodność z mapami suszy IUNG w tych samych terminach.

## Rozproszenie (5 maszyn, 320 GB RAM)

- Kafka na 3 maszynach, temat podzielony na 6 części.
- Spark na 4 workerach. W strumieniu dzielą się **punktami**, nie czasem — jak 4 100 stacji, każdy worker obsługuje swoją grupę; czas płynie po kolei.
- Na demo: wyłączenie maszyny → system działa dalej; przyspieszenie replayu → dokładamy workery.

**Narzędzia:** Airflow (kroki 3–4 po kolei), Kafka UI (podgląd kolejki), Prometheus + cAdvisor (obciążenie), GitHub Actions (testy na małej próbce), lokalny model AI w Ollamie (treść e-maila z liczb).

## Stan

Kolejność uruchamiania (z katalogu `S1/data`; wspólne ustawienia w `config.py`):

| Skrypt | Co robi | Wynik |
| --- | --- | --- |
| `points.py` | tabela punktów: powiat, % pól, wysokość (~10 min) | `points.csv` — 4 117 punktów, 2 805 z ≥ 20% pól |
| `download.py` | pobranie ERA5-Land, 2010–2024, I–IX | `raw/era5land_RRRR_MM.nc` (135 plików) |
| `check.py` | czy pobieranie się udało (pliki, zmienne, godziny, obszar) | lista problemów |
| `to_table.py` | kostka → wiersze punkt × godzina, odwrócenie narastania | `table/era5land_RRRR_MM.parquet` |

Pakiety: `cdsapi>=0.7.7 xarray netCDF4 pandas pyarrow geopandas rasterio requests`.

- ✅ Wszystkie 4 skrypty napisane; `check.py` i `to_table.py` przetestowane na sztucznym pliku o układzie ERA5-Land.
- ⏳ Pobranie danych, potem normy wilgotności i prototyp S1.

## Do ustalenia

- Parowanie: gotowe `pev` czy wzór FAO-56 — porównać na 2018 i wybrać.
- Czy IUNG udostępnia liczby, czy tylko mapy.
- Ile rdzeni mają maszyny (od tego zależy liczba workerów).

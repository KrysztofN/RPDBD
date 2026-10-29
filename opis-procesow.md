# Procesy S1 i S2 — opis

*Strumieniowe procesy zagrożeń dla upraw. Ogólna specyfikacja: [README.md](../README.md), szczegóły S1: [S1/README.md](../S1/README.md).*

## 1. Co to jest

Oba procesy z godzinowej pogody z przeszłości (odtwarzanej replayem do Kafki jak strumień na żywo) liczą **dla każdego powiatu, codziennie, czy uprawy są zagrożone**. Wynik: kolor na mapie (zielony / żółty / czerwony) + e-mail przy alarmie. Różnią się zagrożeniem.

### S1. Susza — czy polom brakuje wody

- **Kiedy:** 21.03–30.09.
- **Pytanie:** czy przez ostatnie 2 miesiące wyparowało dużo więcej wody, niż spadło, i czy gleba jest suchsza niż zwykle o tej porze?
- **Sygnał 1 — bilans wodny (KBW):** suma opadu − suma parowania z 60 dni. < −100 mm ostrzeżenie, < −150 mm alarm (tak liczy oficjalny monitoring suszy IUNG).
- **Sygnał 2 — wilgotność gleby względem normy:** dla każdego punktu i każdej dekady roku z 15 lat liczymy, ile jest „zwykle”. < p20 ostrzeżenie, < p10 alarm.
- **Ostrzeżenie** = jeden z sygnałów, **alarm** = oba.
- **Powiat:** ostrzeżenie, gdy ≥ 30% punktów w ostrzeżeniu; alarm, gdy ≥ 50% w alarmie; < 3 punkty z polami → „za mało pól”.

### S2. Przymrozki wiosenne

- **Kiedy:** IV–VI, gdy rośliny już ruszyły z wegetacją.
- **Pytanie:** czy w nocy temperatura spadła na tyle i na tak długo, że uszkodzi uprawy?
- **Progi** (w konfiguracji): rzepak i zboża jare −4 °C, kukurydza i ziemniaki 0 °C. Przy gruncie jest ok. 1,6 °C zimniej niż na 2 m.
- **Jak:** godzina groźna = temperatura < progu. Dla każdej nocy: liczba groźnych godzin + minimum temperatury. Liczy się czas trwania — kilka godzin w −7 °C niszczy kwitnący rzepak, chwila w −3 °C zwykle nie.
- **Start wegetacji:** średnia dobowa ≥ 5 °C przez 5 dni z rzędu.
- **Powiat:** te same zasady 30% / 50% co S1.

## 2. Dane

Jedno źródło: **ERA5-Land (Copernicus CDS)** — godzinowa reanaliza, siatka 0,1° (~11 × 7 km), 2010–2024, ok. 2 800 punktów z polami w Polsce. IMGW nie jest potrzebne do liczenia.

| Zmienna | S1 | S2 | Po co |
| --- | --- | --- | --- |
| `tp` opad | ✔ | | bilans |
| `pev` lub `t2m`, `d2m`, `u10`, `v10`, `ssrd` | ✔ | | parowanie (gotowe albo FAO-56) |
| `swvl1–3` wilgotność gleby | ✔ | | sygnał 2 |
| `t2m` temperatura 2 m | ✔ | ✔ | przymrozek, start wegetacji |
| `skt` temperatura powierzchni | | ✔ | najlepsze przybliżenie temperatury przy gruncie — **brak w `S1/data/config.py`** |
| `stl1` temperatura gleby 0–7 cm | | ✔ | **brak w `S1/data/config.py`** |
| `d2m`, `u10`, `v10` | | opcjonalnie | bezchmurna, bezwietrzna, sucha noc = silny przymrozek |

`skt` i `stl1` trzeba dopisać do `config.py` **przed pobieraniem**. 12 zmiennych × 31 dni × 24 h = 8 928 pól, limit CDS to 12 000 — nadal 1 zapytanie = 1 miesiąc.

**Dodatkowo:**
- tabela punktów `S1/data/points.csv` (gotowa): powiat, województwo, % pól (GUGiK + WorldCover), wysokość;
- opcjonalnie dla S2: mapa typów upraw EUCROPMAP (zespół B) — wtedy próg zależy od tego, co rośnie w danym miejscu; bez niej alert per uprawa dla każdego punktu z polami.

**Weryfikacja S2 bez IMGW w obliczeniach:** ERA5 to model, nie pomiar, więc potrzebujemy niezależnego sprawdzenia. Propozycja: IMGW zostaje **tylko do porównania** (57 stacji, bez konta, minimum przy gruncie `TGMI`). Plan B: lista znanych przymrozków z mediów i raportów GUS (np. kwiecień 2017, maj 2020 — do potwierdzenia).

## 3. Technologie

Każda technologia musi odpowiadać na pytanie „po co?”.

| Warstwa | Technologia | Po co u nas |
| --- | --- | --- |
| Pobieranie | `cdsapi` + xarray/NetCDF | już jest |
| Orkiestracja | **Airflow** | DAG: pobranie → sprawdzenie → tabela → normy → replay; drugi DAG: raport |
| Magazyn | **HDFS + Parquet** | już jest |
| Magazyn | **Apache Iceberg** / Delta Lake | wersje tabeli, „time travel” |
| Kolejka | **Kafka** (3 węzły, KRaft) | już jest |
| Kolejka | **Schema Registry + Avro** | wiadomość niezgodna ze schematem nie przejdzie (obok DLQ) |
| Kolejka | **Kafka Connect** (ClickHouse Sink) | zapis do ClickHouse bez własnego kodu |
| Strumień S1 | **Spark Structured Streaming** + stan w RocksDB | długie okno 60 dni, dużo stanu |
| Strumień S2 | **Apache Flink + Flink CEP** | „N godzin z rzędu < progu” to wzorzec zdarzeń; dwa różne silniki strumieniowe do porównania |
| Szybkie filtry | **ksqlDB** (opcjonalnie) | walidacja / kierowanie do DLQ SQL-em |
| Wyniki | **ClickHouse** (silnik Kafka + materialized views) | sam czyta topic i agreguje dziennie / co 16 dni pod okresy zespołu B |
| Bieżący stan | **Redis** | aktualny kolor powiatu dla API i Bielika |
| API | **FastAPI** | `GET /powiat/{teryt}/stan` dla raportu i Bielika |
| Jakość danych | **pandera / Great Expectations** | test tabeli przed replayem |
| Monitoring | **Prometheus + cAdvisor + kafka-exporter** | opóźnienie konsumenta (lag), obciążenie |
| Logi | **Loki** | logi kontenerów w Grafanie |
| Wizualizacja | **Grafana** (mapa + alerting); opcjonalnie **Superset** | mapa powiatów, drugi BI do raportu |
| Powiadomienia | **n8n** | alarm → Bielik → e-mail, przepływ do pokazania |
| CI/CD | **GitHub Actions + pytest + Testcontainers** | test z prawdziwą Kafką („80 dni bez deszczu → alarm”) |
| Wdrożenie | **Docker Swarm** (+ Portainer) | już jest |

**Rdzeń:** Kafka + Spark (S1) + Flink (S2) + ClickHouse + Grafana + Airflow + Bielik. Iceberg, ksqlDB, Superset — jeśli starczy czasu.

## 4. Bielik i wspólny raport

Zasada: **model tylko opisuje, nic nie liczy.** API: `AI/api.py` (`POST /chat`), Bielik 11B przez Ollamę.

1. **Komunikat przy alarmie S1/S2 (na bieżąco):** n8n / Grafana Alerting wysyła JSON z liczbami, np. `{powiat, bilans_mm, wilgotnosc_vs_norma, dni_bez_deszczu}` albo `{powiat, min_temp, godziny_przymrozku, uprawa}` → 3–4 zdania po polsku do e-maila.
2. **Wspólny raport co 16 dni (rytm MODIS MOD13Q1):** DAG w Airflow:
   - z ClickHouse wyniki S1, S2, B1, B2 za okres, złączone po kodzie TERYT,
   - JSON → Bielik pisze sekcje: „Susza”, „Przymrozki”, „Stan roślinności (NDVI)”, „Fenologia / plony” + podsumowanie łączące sygnały (np. alarm suszy S1 zgodny ze spadkiem NDVI z B1),
   - Markdown → PDF (Quarto / WeasyPrint) → e-mail / folder.
3. **Zabezpieczenie:** liczby wstawiamy z bazy w szablonie albo po generacji skrypt sprawdza, czy każda liczba w tekście występuje w JSON-ie — model nie może zmyślić danych.

## 5. Do ustalenia z zespołem B

- **Wspólny klucz:** kod TERYT powiatu / województwa (`county_code`, `voiv_code` w `points.csv`) + data.
- **Wspólny okres:** 16-dniowe okresy MOD13Q1 (dni roku 1, 17, 33, …) — jak zrzucamy dzienny stan S1/S2 (najgorszy w okresie czy na koniec okresu).
- Co dokładnie robi B2 (w README tabela mówi „Fenologia”, a prognoza plonów jest pod G).

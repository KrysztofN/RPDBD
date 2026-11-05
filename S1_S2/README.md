# S1/S2 — ERA5-Land na istniejącym klastrze

**Wersja do testów integracyjnych: kod zweryfikowany składniowo, ale nie uruchomiony na Twoich VM.**

## Infrastruktura (bez tworzenia nowego klastra)

- Kafka `10.7.11.22–24:9092` (trzy istniejące brokery)
- Spark `spark://10.7.11.21:7077`, **Spark 4.0.4 / Scala 2.13**
- HDFS `hdfs://10.7.11.21:9000`, WebHDFS `http://10.7.11.21:9870`
- ClickHouse: istniejąca baza `rbd`; istniejący Grafana i bot Telegram
- Obraz `rbd-python:0.1`, `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4`

Wszystkie polecenia poniżej dla **Bigdata-wn212-04**; `compose.yaml` ma `spark.driver.host=10.7.11.24`. Kod powinien znajdować się w `~/rbd-build/S1_S2`, a obok niego `~/rbd-build/reporter-build` z oryginalnym `lib/era5_counties_spark.py`.

## Instalacja

```bash
cd ~/rbd-build/S1_S2
cp .env.example .env
nano .env              # hasło ClickHouse
chmod 600 .env
docker compose build
```

## Testy i inicjalizacja (automatycznie z Pythona)

```bash
docker compose run --rm init python3 -m unittest discover -s tests -v
docker compose run --rm init python3 kafka_setup.py
docker compose run --rm init python3 make_grid_map.py
docker compose run --rm init python3 create_tables.py
```

Mapowanie siatki ERA5 na powiaty trafia do `/rbd/ref/weather_grid_counties/map.csv` (uprawnienia HDFS są wymagane). Python tworzy temat `weather.hourly.v3` i `alerts.detected` z replikacją 3 oraz sześcioma partycjami. Nie zmienia tematów biuletynów.

## Uruchomienie

Terminal PuTTY nr 1:

```bash
docker compose run --rm s1
```

Terminal nr 2:

```bash
docker compose run --rm s2
```

Terminal nr 3, po uruchomieniu obu zapytań:

```bash
# krótki test S2 (3 dni)
docker compose run --rm replay python3 era5_replay.py \
  --start 2018-04-01T00:00 --end 2018-04-03T23:00 --speed 1
```

Do pełnego S1 odtwarzaj **co najmniej 63 kolejne dni**, najlepiej do 2018-06-10; przeprowadź to w oddzielnym teście z nowym tematem i checkpointami, aby uniknąć ponownego wysyłania kwietniowych danych. `--speed 1` to cel 1 godzina symulacji/s, nie gwarancja czasu wykonania.

```bash
docker compose run --rm replay python3 era5_replay.py \
  --start 2018-04-01T00:00 --end 2018-06-10T23:00 --speed 1
```

**Nie uruchamiaj obu przykładów kolejno na tym samym temacie.** Dla nowego doświadczenia ustaw `WEATHER_TOPIC` i `CHECKPOINT_ROOT` na nowe wartości (checkpoint zmienisz przez zmienną środowiskową Compose). Replay końca miesiąca używa akumulacji 00:00 następnego miesiąca; pełny grudzień 2018 wymaga stycznia 2019, którego obecnie nie masz. Dla 2018-12-31 kończ najpóźniej o 22:00.

## Zawartość

- `era5_replay.py`: odczyt NetCDF z HDFS miesiącami; dla godziny h opad i promieniowanie to przyrost między końcami godzin h i h+1; godzina 00 jest pierwszą akumulacją z 01:00, h=23 wykorzystuje 00:00 następnego dnia. Wysyłka z oryginalnym `event_time`, przybliżone tempo replay.
- `make_grid_map.py`: istniejące granice GUGiK → punkty ERA5 przypisane do powiatów; najbliższa komórka w powiatach bez punktu.
- `s1_stream.py`: osobne godzinowe wartości (`s1_hourly`), sygnał przesiewowy przy `swvl1 < 0.15` (umowny próg demonstracyjny, nie reguła agronomiczna) i dzienne ET₀ FAO-56, bilans 7/30/60 dni (`s1_daily`, `s1_balance`). Alert przy 60 kompletnych dniach i KBW60 < −150 mm. Wykorzystuje istniejące `et0_fao56` B1/B2; średnie per powiat mogą się różnić od sum ET0 obliczanych per punkt.
- `s2_stream.py`: minimum temperatury powietrza i wskaźnik przygruntu `t2m−1.6` dla pełnych godzin (`s2_hourly`), widoki `s2_frost_alerts` i `s2_rolling_24h`.
- `create_tables.py`: nie usuwa i nie aktualizuje dotychczasowych tabel B1/B2.
- `grafana/dashboards/s1s2.json`: dashboard do importu; zamień `REPLACE_WITH_CLICKHOUSE_UID` na UID istniejącego źródła ClickHouse.
- `alert_telegram.py`: odbiera `alerts.detected` (domyślnie **DRY RUN**), wykorzystuje istniejący kod Telegram i subskrypcje powiatów B1/B2.

**Alerty są symulacją archiwalnych wydarzeń z 2018 roku**, nie ostrzeżeniem na dziś. Nie używaj `--send --allow-historical` bez uzgodnienia z zespołem — istniejący bot może rozesłać bardzo dużo wiadomości do swoich subskrybentów. Na docelowym wdrożeniu należy dodać osobne subskrypcje `/alerty`. Wysyłka ma semantykę at-least-once, a nie 100% exactly-once.

## Kontrola

```bash
docker compose run --rm init python3 check_results.py
docker compose run --rm alert-preview  # tylko podgląd alertów, bez wysyłania
```

Przykładowe SQL w istniejącym ClickHouse:

```sql
SELECT * FROM rbd.s2_frost_alerts ORDER BY event_hour DESC LIMIT 20;
SELECT region_id,day,kbw_7d_mm,kbw_30d_mm,kbw_60d_mm,days_60d,alert
FROM rbd.s1_balance FINAL ORDER BY day DESC LIMIT 20;
```

## Ograniczenia i rozszerzenia

- **Bielik:** istniejąca Ollama działa na `127.0.0.1` workerów, więc ta paczka nie wywołuje jej z node04. Aby dodawać komentarze modelu, proces przygotowania alertów trzeba uruchomić obok istniejącego Bielika, z deterministycznym fallbackiem. Detekcją zagrożenia nadal sterują reguły.
- **Cassandra i Kafka Streams** nie są instalowane w tej wersji; ich wdrożenie wymaga ustalenia zasobów 4 workerów i uzgodnienia nowych usług z zespołem. Przetwarzanie strumieniowe odbywa się w Spark Structured Streaming.
- **Korekta S1:** sumę opadów `avg(tp_mm)*24` liczymy przy założeniu stabilnej liczby punktów siatki per godzina; dla jakości produkcyjnej należy zastosować agregację per punkt→godzina→powiat oraz porównać ET0 z istniejącymi tabelami weather_counties.
- **S2:** założona różnica 1,6°C nie jest rzeczywistym pomiarem przy gruncie. `stl1` to temp. gleby 0–7 cm; nie można jej utożsamiać z temperaturą tuż przy powierzchni. Progi określają potencjalne zagrożenie, nie stwierdzone straty.
- **Replay:** duplikaty Kafka mogą powstać po ponownym uruchomieniu. Spark stosuje ograniczoną czasowo deduplikację i checkpointy; ClickHouse używa `ReplacingMergeTree`. Nie gwarantujemy dokładnie raz dla kombinacji Kafka→ClickHouse→Telegram.
- Przed produkcyjnym użyciem trzeba wykonać end-to-end: stan Spark, zamykanie okien po watermarku, SQL widoków ClickHouse, wydajność oraz Telegram SOCKS proxy.

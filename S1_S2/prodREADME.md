# S1/S2: Spark Structured Streaming → incydenty → Kafka → Telegram `/powiat`

Wersja do wdrożenia na działającym klastrze (Spark **4.0.4 / Scala 2.13**).
**Nie była uruchomiona na Waszym klastrze** — przeprowadzono kontrolę składni i testy statyczne. Wdrażaj etapami.

## Architektura

```text
ERA5 → Kafka weather.hourly.v4 → Spark S1 hourly/daily ──→ CH s1_hourly/s1_daily (historia)
                                    │
                                    └── Kafka s1.daily.metrics.v1
                                          │
                                          └── Spark s1_kbw_state.py [stan 60 dni w Spark checkpoint/HDFS]
                                                ├── CH s1_balance (historia/Grafana)
                                                └── Kafka alerts.detected (TYLKO water_deficit)

ERA5 → Kafka weather.hourly.v4 → Spark S2 hourly ──→ CH s2_hourly (historia/Grafana)
                                    └── Kafka alerts.detected (godzinowe sygnały S2)

Kafka alerts.detected → Spark spark_incidents.py (session_window, stan w checkpoint/HDFS)
          ├── S2: według POWIATU, przerwa > 90 minut kończy epizod
          │        grupy upraw scalane w JEDEN epizod
          ├── S1: według POWIATU, przerwa > 36 h kończy serię dni z KBW60 < -150 mm
          ├── Kafka s1s2.incidents.v1 ──→ pojedynczy istniejący bot Telegram
          │                                TYLKO aktywne subskrypcje county:XXXX (/powiat)
          └── CH s1s2_incident_history (wyłącznie historia/Grafana)
```

**Zamknięte zdarzenia**: sesja pojawia się po przekroczeniu watermarku. S2 opóźnienie ~4,5 h czasu zdarzenia (90 min + 3 h); S1 ~3,5 dnia (36 h + 2 dni). To są progi czasu ZDARZEŃ, a nie minut zegarowych. Jeśli Kafka całkowicie przestanie dostarczać dane, watermark nie postępuje i ostatnia sesja może pozostać niewysłana. Konieczny jest monitoring ostatniego event_time / lag i okresowy sygnał postępu w realnym systemie.

**Brak opóźnionych korekt**, zgodnie z wymaganiem: dzienne S1 są niemutowalne po zamknięciu; wcześniejsze daty ignorujemy w stanie KBW. Nie mieszaj różnych wersji replay na tych samych checkpointach.

**S1**: próg `-150 mm` jest uproszczonym modelem, nie formalnym stwierdzeniem suszy IUNG. **S2**: minima przy gruncie są ESTYMACJĄ, a nie pomiarem/wiarygodną prognozą szkód. Jeden incydent S2 ma **jedną wiadomość z listą grup upraw**.

## Pliki

- `worker/spark_incidents.py` — Spark stateful session windows; dwa niezależne checkpointy S1/S2; zdarzenia do Kafka + kopia w ClickHouse.
- `worker/s1_kbw_state.py` — Spark stateful 60-dniowe KBW (applyInPandasWithState; stan ostatnich 60 dni na powiat); publikuje tylko S1 `water_deficit`, zapisuje S1 balance do CH.
- `worker/patch_s1_daily.py` — BEZPIECZNA modyfikacja lokalnego S1, aby publikował `s1.daily.metrics.v1` i **przestał czytać ClickHouse w obliczeniach KBW**. Robi kopię zapasową; wymaga zgodności bloku `save_daily` z wyjściowym kodem.
- `worker/patch_s2_simulation.py` — opcjonalna zmiana flagi historyczny/live; użyj przed przejściem na dane live.
- `worker/seed_s1_daily_once.py` — jednorazowy bootstrap (CH **tylko migracja starych danych** → Kafka), nie element produkcyjny.
- `worker/setup_incident_topics.py` — Kafka topic setup.
- `worker/compose.incidents.yaml` — dwie nowe usługi. Nie zmienia działających `s1/s2`.
- `master/telegram_bot.py` — nowy bot oparty na Waszej wersji 0.2, z obsługą `s1s2.incidents.v1` w tym samym procesie `getUpdates`. Tylko `/powiat`.
- `master/patch_bot.py` — patcher odtwarzający tę zmianę z wersji 0.2; nie uruchamiaj na już zmodyfikowanej wersji.

## ETAP A: wgraj pliki i utwórz tematy (worker 04)

Przez WinSCP skopiuj pliki z `worker/` do `~/rbd-build/S1_S2/` (bez zmiany `compose.yaml`, `.env`, czy bieżących skryptów). Zachowaj uruchomiony replay, jeśli jeszcze trwa. Dla testów najlepiej zacząć po jego zakończeniu, bo trzy aplikacje Spark potrzebują dodatkowych rdzeni i pamięci.

```bash
cd ~/rbd-build/S1_S2
python3 -m py_compile spark_incidents.py s1_kbw_state.py patch_s1_daily.py
# Wykorzystujemy konfigurację i dostęp do Kafka wewnątrz kontenera:
docker compose run --rm init python3 setup_incident_topics.py
```

W `.env` zachowaj `WEATHER_TOPIC` i `CHECKPOINT_ROOT` z obecnego testu i dodaj:

```dotenv
S1_DAILY_TOPIC=s1.daily.metrics.v1
INCIDENTS_INPUT_TOPIC=alerts.detected
INCIDENTS_OUTPUT_TOPIC=s1s2.incidents.v1
INCIDENTS_START_OFFSETS=earliest
INCIDENTS_HISTORY_ENABLED=1
INCIDENTS_SIMULATION=1
```

**Ważne:** po raz pierwszy użyj nowych, pustych checkpointów `incident_sessions_v1` i `s1_kbw_state_v1`. Nie usuwaj starych checkpointów S1/S2 ani stanu poprzedniego watchera.

## ETAP B: przygotuj S1 bez zależności od ClickHouse

Wykonaj na workerze 04 w katalogu, w którym jest obecny `s1_stream.py`:

```bash
python3 patch_s1_daily.py s1_stream.py
# Powinna powstać kopia s1_stream.py.pre_incidents_backup
python3 -m py_compile s1_stream.py
```

Patcher odmówi zmian, jeśli kod `save_daily` nie zgadza się z paczką. W takim przypadku **nie podmieniaj starego działającego S1 w ciemno**. Zobacz diff i dopiero potem dostosuj patch.

W `compose.yaml` w istniejącym `x-common.environment` dopisz:

```yaml
    S1_DAILY_TOPIC: ${S1_DAILY_TOPIC:-s1.daily.metrics.v1}
    INCIDENTS_SIMULATION: ${INCIDENTS_SIMULATION:-1}
```

Uruchom tylko `s1` ponownie, nie `s2` ani replay:

```bash
docker compose up -d --force-recreate s1
docker compose logs --tail=70 s1
```

**Obecne 66 dni** mogły już zostać przetworzone. Aby zainicjalizować KBW ze starych danych bez ponownego replay całego ERA5, wykonaj **RAZ** migrację przed startem `s1-kbw` (patrz niżej). To tylko bootstrap historyczny; wersja live ma dane z Kafki.

```bash
docker compose run --rm -e CONFIRM_S1_BACKFILL=YES init python3 seed_s1_daily_once.py
```

To może wygenerować wiele rekordów. **Nie uruchamiaj drugi raz na tym samym temacie**, a do testu historycznego pozostaw wysyłkę bota wyłączoną.

## ETAP C: uruchom 2 nowe aplikacje Spark (worker 04)

```bash
# Najpierw Kafka KBW 60d:
docker compose -f compose.yaml -f compose.incidents.yaml up -d s1-kbw
# Po potwierdzeniu działania:
docker compose -f compose.yaml -f compose.incidents.yaml up -d s1s2-incidents

docker compose -f compose.yaml -f compose.incidents.yaml ps
# Logi:
docker compose -f compose.yaml -f compose.incidents.yaml logs --tail=100 s1-kbw s1s2-incidents
```

Sprawdź Spark Master UI `http://10.7.11.21:8080`, aby upewnić się, że wszystkie aplikacje mają executory i nie ma restartów. Jeśli zasoby klastra są niewystarczające, uruchom najpierw tylko `s1s2-incidents` (S2 działa niezależnie).

## ETAP D: nowy bot w JEDNYM istniejącym kontenerze (master 01)

**Nigdy nie twórz nowej usługi `rbd-telegram_reporter`.** Istniejąca usługa nazywa się `rbd-telegram_bot`. Nowa wersja obrazu `rbd-reporter:0.3` ma zawierać jedynie nowy `telegram_bot.py`; inne moduły są identyczne jak w 0.2.

Na masterze, po skopiowaniu `master/telegram_bot.py` przez WinSCP do `~/telegram_bot.py`:

```bash
cd ~/rbd-build
cp reporter-build/B1_B2/reporter/telegram_bot.py reporter-build/B1_B2/reporter/telegram_bot.py.v02_backup
cp ~/telegram_bot.py reporter-build/B1_B2/reporter/telegram_bot.py
cp ~/telegram_bot.py app/B1_B2/reporter/telegram_bot.py
python3 -m py_compile reporter-build/B1_B2/reporter/telegram_bot.py
# Twój prawidłowy build context:
docker build -t rbd-reporter:0.3 -f reporter-build/Dockerfile reporter-build
# Kontrola:
docker run --rm --entrypoint grep rbd-reporter:0.3 -n 's1s2.incidents.v1' /app/B1_B2/reporter/telegram_bot.py
```

**Bezpieczny etap testowy:** bot nadal ma `getUpdates` tylko raz, ale **żadna nowa subskrypcja S1/S2 nie jest aktywowana**, dopóki nie podasz zmiennej.

Na masterze ustaw w `~/rbd-build/telegram/.env`:

```dotenv
S1S2_INCIDENTS_ENABLED=0
S1S2_INCIDENTS_TEST_ONLY=1
S1S2_TEST_CHAT_ID=TWÓJ_CHAT_ID
S1S2_ALLOW_HISTORICAL=0
```

Następnie zaktualizuj istniejącą usługę (to zmienia działający kod, zachowuje dotychczasowy token/proxy i B1/B2):

```bash
docker service update \
 --image rbd-reporter:0.3 \
 --env-add S1S2_INCIDENTS_ENABLED=0 \
 --env-add S1S2_INCIDENTS_TEST_ONLY=1 \
 --env-add S1S2_ALLOW_HISTORICAL=0 \
 rbd-telegram_bot
```

Po potwierdzeniu działania podłącz topic i włącz tryb testowy jednym `docker service update`:

```bash
docker service update \
 --env-add S1S2_INCIDENTS_ENABLED=1 \
 --env-add S1S2_INCIDENTS_TEST_ONLY=1 \
 --env-add S1S2_TEST_CHAT_ID=TWÓJ_CHAT_ID \
 --env-add S1S2_ALLOW_HISTORICAL=1 \
 rbd-telegram_bot
```

**Zanim aktywujesz test**, musisz mieć w Telegramie `/powiat 0206` (dla alertów tego powiatu) lub inny właściwy kod; `/woj` i `/wszystko` są **ignorowane** dla incydentów. W trybie testowym bot wysyła tylko do wskazanego chat ID, o ile ma on aktywną subskrypcję `/powiat`. Przy backfill wszystkich 314 powiatów testowa osoba z `/powiat` może otrzymać wiele historycznych incydentów — najbezpieczniej pozostawić `S1S2_INCIDENTS_ENABLED=0`, uruchomić stream, sprawdzić CH historię, i włączyć odbiór dopiero po zakończeniu backfill. Jeśli bot zacznie czytać temat od `latest`, przeszłe incydenty nie wyślą się automatycznie.

Test odbioru na żywo: produkuj nowe dane strumieniowo i obserwuj `docker service logs --tail=100 rbd-telegram_bot`. **Nie** uruchamiaj wcześniej użytego `s2-watcher`; mógłby produkować duplikaty innym schematem.

## ETAP E: polityka produkcyjna

- Wersja live: `INCIDENTS_SIMULATION=0` w obu procesach publikujących alerty, `S1S2_ALLOW_HISTORICAL=0` w bocie; S2 wymaga zastosowania `patch_s2_simulation.py` i przekazania zmiennej do kontenera.
- Zasilanie z bieżących, a nie replay, strumieni ERA5: musisz użyć osobnego topicu, osobnych checkpointów i wyłączyć testowy replay; nie wolno nazywać historycznych pomiarów bieżącą prognozą.
- Dopiero po testach zdejmij `S1S2_INCIDENTS_TEST_ONLY=1` ustawiając `0` w działającej usłudze. Incydenty trafią wtedy do **wszystkich aktywnych subskrypcji powiatowych**, nigdy `/woj` ani `/wszystko`.
- `INCIDENTS_START_OFFSETS=latest` dla nowego pustego checkpointu w systemie live, a stary checkpoint zostaje bez zmian. Uwaga: z `latest` uruchom najpierw konsumenta, potem źródła.
- Deduplikacja: po stronie Telegrama `rbd.tg_incident_delivery` (`event_id`,`chat_id`, `status`). Przy awarii pomiędzy skutecznym `sendMessage` a wpisem historii możliwa jest powtórka — gwarancja at-least-once, **nie exactly once**. Kafka → CH to wtórny sink; retry może dać powtórkę, a `ReplacingMergeTree` usuwa logiczne duplikaty przy `FINAL`.
- Stan sesji trzyma Spark/HDFS, a nie lokalny SQLite. Dla produkcji ustaw `spark.sql.streaming.stateStore.providerClass` RocksDB i monitoruj wielkość stanu oraz checkpointy, jeżeli jest dostępny i przetestowany na Twoim klastrze.
- S1 60-dniowe KBW wymaga 60 **kolejnych** dni kompletnych obserwacji; brak danych oznacza brak alarmu. Próg `-150` jest poglądowy.
- **Nie wspieramy rewizji/spóźnionych danych**, ale potrzebujemy watermarku do zamknięcia sesji. Musisz mieć sygnały postępu czasu zdarzenia nawet w okresach bez alertów, inaczej ostatnie otwarte sesje pozostaną bez wyjścia.
- Jeśli zechcesz zmienić luki sesji, progi lub schemat stanu, załóż nowe checkpointy i nową wersję tematu. Nie edytuj produkcyjnych checkpointów ręcznie.
- `rbd.tg_subscribers` z dawnego bota wciąż jest bazą subskrypcji (control plane), a `rbd.tg_incident_delivery` i `rbd.s1s2_incident_history` to logi historii. **Wykrywanie incydentów i KBW nie czyta ClickHouse.**

## Wycofanie

Master: `docker service update --image rbd-reporter:0.2 --env-add S1S2_INCIDENTS_ENABLED=0 rbd-telegram_bot`.
Worker: `docker compose -f compose.yaml -f compose.incidents.yaml stop s1-kbw s1s2-incidents`; S1 można przywrócić z `s1_stream.py.pre_incidents_backup` i uruchomić ponownie. Nie kasuj historycznych tabel, logów ani checkpointów.

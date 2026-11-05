# S2: automatyczny obserwator incydentów (bezpieczny test)

## Decyzja architektoniczna

**Docelowo**: `Kafka weather.hourly.v4 -> Spark (agregacja godzinowa S2) -> Kafka s2.hourly -> procesor stanowy (Kafka Streams lub Spark) -> Kafka s1s2.incidents -> bot -> Telegram`. ClickHouse jest sinkiem do analiz, audytu i Grafany.

**Teraz, bez zmiany działającego Sparka**: `S2 -> ClickHouse s2_frost_alerts -> watcher -> Kafka s1s2.incidents.test -> istniejący rbd-telegram_bot (0.2) -> pojedynczy czat testowy`. To jest celowo **przejściowe rozwiązanie pollingowe** i nie jest strumieniowaniem end-to-end. Działa bez przebudowy Spark S2 i bez drugiej instancji Telegram getUpdates.

## Pliki

- `s2_incident_watcher.py` — okresowo czyta maksymalnie 7 dni z widoku S2, scala godziny dla powiatu i grupy upraw (korzysta z `telegram_incidents.py`), utrzymuje w SQLite fazy `open`, `updated`, `closed`, publikuje **tylko zamknięty** incydent. Log audytowy `audit` w tym samym SQLite.
- `compose.watcher.yaml` — osobna usługa Compose, bez zmian S1/S2 i replay.
- `tests/test_watcher.py` — testy kontraktu wiadomości i cyklu życia.

## Przygotowanie na Bigdata-wn212-04

1. Skopiuj **oba** pliki `s2_incident_watcher.py` i `compose.watcher.yaml` obok obecnego `compose.yaml` w `~/rbd-build/S1_S2/`. Pozostaw dotychczasowe `telegram_incidents.py` (z wcześniejszej paczki) w tym samym katalogu.
2. Utwórz trwały katalog stanu: `mkdir -p ~/rbd-build/S1_S2/state`. SQLite musi mieć uprawnienia zapisu dla użytkownika z obrazu. Jeśli jest problem z uprawnieniami, sprawdź `id` w kontenerze i dostosuj właściciela katalogu.
3. W istniejącym `~/rbd-build/S1_S2/.env` ustaw `S2_AUTOPUBLISH=0` — domyślnie **żadna wiadomość Kafka nie zostanie opublikowana**.
4. Upewnij się, że bot 0.2 działa na masterze i ma `S1S2_TEST_ALERTS_ENABLED=1`, ustawiony `S1S2_TEST_CHAT_ID` oraz widzi temat `s1s2.incidents.test`. Żadnego tokenu Telegram nie trzeba przekazywać watcherowi.

## Dry run

```bash
cd ~/rbd-build/S1_S2
mkdir -p state
docker compose -f compose.yaml -f compose.watcher.yaml run --rm s2-watcher \
  python3 -u /app/s2_incident_watcher.py --once --start 2018-04-01T00:00:00Z \
  --end 2018-04-08T00:00:00Z --regions 0206 --max-publish 1
```

Wynik: `DRY RUN closed incident...`, fazy incydentów zapisane w `state/s2_watcher.sqlite3`, **zero wysyłek**.

## Włączenie automatyzacji: tylko JEDEN testowy alert łącznie

W `.env`: `S2_AUTOPUBLISH=1`. Włącz usługę:

```bash
docker compose -f compose.yaml -f compose.watcher.yaml up -d s2-watcher
docker compose -f compose.yaml -f compose.watcher.yaml logs -f --tail=40 s2-watcher
```

Watcher ponawia odczyt co 60 s, ale zapisuje do Kafki **maksymalnie jeden incydent w całym czasie życia pliku SQLite** (`--max-publish 1`), tylko dla `0206` z 1–7 kwietnia 2018. Ten incydent może być taki sam jak wcześniej testowany — w takim przypadku bot już go zarejestrował i **nie wyśle ponownie**, co jest poprawną deduplikacją. Aby zobaczyć nową wiadomość, świadomie wybierz inny powiat/okno i odrębny plik stanu; nadal obowiązuje limit wysyłek do jednego czatu.

Wyłączenie:

```bash
docker compose -f compose.yaml -f compose.watcher.yaml stop s2-watcher
```

## Gdzie jest stan

`state/s2_watcher.sqlite3`:
- `incident_state`: `event_id`, powiat, uprawa, początek i ostatnia godzina, liczba godzin, minimum temperatury, faza, `published`.
- `audit`: przejścia faz i fakt publikacji do Kafki.

Kontrola bez modyfikacji bazy:

```bash
python3 - <<'PY'
import sqlite3
c=sqlite3.connect('state/s2_watcher.sqlite3')
print(c.execute('SELECT phase,count(*) FROM incident_state GROUP BY phase').fetchall())
print(c.execute('SELECT event_id,published FROM incident_state WHERE published=1').fetchall())
PY
```

## Ważne ograniczenia

- `closed` jest określane heurystycznie z **maksymalnej godziny obecnej w ClickHouse** i bufora 3 godzin. Przy opóźnieniach/out-of-order po tym buforze może dojść do błędnej finalizacji; w produkcji użyj watermarka i stanowego procesora strumieniowego.
- Obecny test zapisuje fazy `open`, `updated`, `closed` w SQLite, ale wysyła do Telegrama tylko **zamknięte** incydenty (bez serii wiadomości start/update/koniec). To celowe ograniczenie powiadomień testowych.
- Odczyt `s2_frost_alerts` korzysta z `s2_hourly FINAL`; polling jest droższy niż zdarzenia Kafka, więc nie zwiększaj skali bez zmiany architektury.
- SQLite jest lokalne i **nie jest odporne na utratę węzła**. Nie usuwaj folderu `state` ani nie uruchamiaj dwóch instancji watchera ze wspólnym stanem.
- Kafka i ClickHouse nie zapewniają transakcji łączonej. Awaria pomiędzy publikacją a wpisem do SQLite może opublikować ponownie; bot deduplikuje `event_id` po udanej wysyłce, choć jego zapis logu też nie jest atomowy z Telegram API.
- `event_id` bazuje na **czasie początku** ciągłego incydentu; zmiana już zamkniętego incydentu wymaga osobnej obsługi korekt.
- `max-publish` jest limitowany per baza stanu SQLite; usunięcie bazy resetuje limit. Nie ustawiaj `S2_AUTOPUBLISH=1` przy szerokim zakresie/powiatach bez świadomej decyzji.
- `S2_AUTOPUBLISH` to kontrola publikacji Kafka, a `S1S2_TEST_ALERTS_ENABLED` na masterze to osobna kontrola wysyłki Telegram.

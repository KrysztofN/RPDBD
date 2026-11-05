# Telegram — bezpieczny test incydentów przymrozkowych S2

**Nie zastępuje istniejącego bota B1/B2.** Nie wywołuje `getUpdates` i nie subskrybuje Kafki. Czyta tylko historyczne wyniki `rbd.s2_frost_alerts` z ClickHouse. Łączy kolejne godziny zagrożenia dla jednego powiatu i grupy upraw w jeden incydent; ignoruje pojedyncze godziny (`--min-hours 2`). Wybiera najwyżej 3 pierwsze incydenty (`--limit 3`) z maksymalnie 7 dni. Domyślnie **wyłącznie wypisuje podgląd**, nie wysyła nic do Telegrama ani nie modyfikuje bazy.

## Instalacja (na Bigdata-wn212-04)

Wgraj `telegram_incidents.py` do istniejącego `~/rbd-build/S1_S2`. Ponieważ `/app` jest zamontowane jako katalog projektu, **nie trzeba przebudowywać obrazu ani restartować Spark S1/S2/replay**.

Podgląd na danych 1–8 kwietnia 2018:

```bash
cd ~/rbd-build/S1_S2
docker compose run --rm init python3 telegram_incidents.py \
  --start 2018-04-01T00:00:00Z --end 2018-04-08T00:00:00Z \
  --min-hours 2 --limit 3
```

Jeśli 0 incydentów: użyj `--min-hours 1` lub innego tygodnia. Opcjonalnie `--region 3011`, ale nie każdy powiat ma przymrozki.

## Wyślij JEDEN alert na swój czat testowy

1. We własnym koncie Telegram wyślij `/start` do **istniejącego bota zespołu** (nie twórz nowego). Potrzebujesz numerycznego `chat_id` swojego czatu; poproś administratora bota o bezpieczne ustalenie ID lub uzyskaj je z autoryzowanych logów. **Nie uruchamiaj osobnego procesu getUpdates**, gdy istniejący bot już go używa.
2. W istniejącym `.env` na maszynie 04 wstaw `TELEGRAM_BOT_TOKEN` (ten sam token co w działającym bocie, bez wklejania go do czatu/Git) oraz `TELEGRAM_TEST_CHAT_ID=<twoj_chat_id>`. Sieć może wymagać `TELEGRAM_PROXY` podobnie jak istniejący bot.
3. Obecny `compose.yaml` przekazuje token, ale **nie przekazuje `TELEGRAM_TEST_CHAT_ID`**. Dodaj tę jedną linię pod `environment` w `x-common`:

```yaml
    TELEGRAM_TEST_CHAT_ID: ${TELEGRAM_TEST_CHAT_ID:-}
```

4. Uruchom wysyłkę tylko jednego zdarzenia:

```bash
docker compose run --rm init python3 telegram_incidents.py \
  --start 2018-04-01T00:00:00Z --end 2018-04-08T00:00:00Z \
  --min-hours 2 --limit 1 --send --allow-historical
```

Ponowne uruchomienie tego samego zakresu nie powinno wysłać zdarzenia z tym samym `event_id` do tego samego `chat_id` — wysyłki testowe zapisuje osobna tabela `rbd.s1s2_incident_test_log`. Nie jest to atomowa gwarancja dokładnie jednej dostawy w razie awarii pomiędzy Telegramem a zapisem logu; uruchamiaj **jeden** proces testowy naraz.

## Bielik (opcjonalnie)

Jeśli istniejąca instancja Ollama jest osiągalna z kontenera `init`, dodaj do `x-common.environment`:

```yaml
    BIELIK_OLLAMA_URL: ${BIELIK_OLLAMA_URL:-}
    BIELIK_MODEL: ${BIELIK_MODEL:-}
```

W `.env` podaj **rzeczywisty** wewnętrzny URL Ollama i zainstalowaną nazwę modelu (nie zgaduj nazwy). Użyj `--use-bielik`; odpowiedź będzie jedynie tekstowym komentarzem. W razie błędu Ollama proces wyśle komunikat regułowy. Bez flagi ani endpointu Bielik nie jest używany.

## Ograniczenia i zasady

- Progi 0 °C i −4 °C są **modelowymi wskaźnikami ekspozycji**, nie dowodem uszkodzenia upraw.
- Dane ERA5-Land są historyczne i przygruntowa temperatura jest oszacowana jako `t2m - 1.6 °C`.
- Dwie niezależne grupy upraw mogą wygenerować dwa incydenty w tym samym powiecie i okresie. Docelowo można scalić grupy w jeden komunikat.
- To jest **test wsadowy z ClickHouse**, a nie pełne produkcyjne przetwarzanie live incydentów z Kafka Streams. Taki proces powinien mieć osobny stan i obsługę końca incydentu oraz określony czas oczekiwania na spóźnione zdarzenia.
- Nie podłączaj `alerts.detected` bezpośrednio do istniejącego bota: Spark wysyła tam osobną wiadomość na każdą godzinę i grupę upraw.

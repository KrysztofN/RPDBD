# Monitorowanie zagrożeń dla upraw — specyfikacja

*Rozproszone Przetwarzanie Danych w Big Data · plan pracy: [ROADMAP.md](ROADMAP.md)*

## 1. Projekt

Z historycznych danych pogodowych i satelitarnych liczymy zagrożenia dla upraw w regionach Polski. Część procesów to **strumień** (alerty co godzinę), część **batch** (całe sezony). Strumień symulujemy: archiwalne dane odtwarzamy do Kafki, jakby przychodziły na żywo. Dane pobieramy raz, więc demo zawsze działa tak samo. Wyniki: ClickHouse + Grafana.

## 2. Rolnictwo w pigułce

- **Oziminy** (pszenica, rzepak) — siane jesienią, zimują w polu, zbiór w lipcu. **Jare** (jęczmień, kukurydza, ziemniaki, buraki) — siane wiosną.
- **Plon** — dt/ha (1 dt = 100 kg); pszenica ok. 45–60 dt/ha.
- **NDVI** — „zieloność” ze zdjęć satelitarnych (0,1 goła ziemia … 0,9 pełny wzrost). W roku ma kształt górki ze szczytem w maju–czerwcu.
- **Bilans wodny (KBW)** = opad − ET₀ (parowanie). Długo ujemny = susza.

| Okres | Zagrożenie | Próg (w konfiguracji) | Proces |
| --- | --- | --- | --- |
| XII–II | **wymarzanie ozimin** | < −18 °C bez śniegu; ≥ 10 cm śniegu chroni do ok. −23 °C | D |
| IV–VI | **przymrozki** | rzepak −4 °C, zboża jare −4 °C, kukurydza/ziemniaki 0 °C; przy gruncie jest o ok. 1,6 °C zimniej niż na 2 m | S2 |
| III–IX | **susza** | KBW z 60 dni < −150 mm (uproszczenie definicji IUNG) | S1, H |
| VII–X | wynik roku: **plon** | — | B2 |

Testujemy na: zimie **2011/12** (do −30 °C bez śniegu, duże straty ozimin) i suszach **2018, 2019**.
Źródła progów: [farmer.pl](https://www.farmer.pl/produkcja-roslinna/rzepak/kiedy-wymarza-rzepak,76958.html), [agrofakt.pl](https://www.agrofakt.pl/wymarzanie-zboz-rzepaku-moze-byc-realnym-zagrozeniem/), [farmer.pl — przymrozki](https://www.farmer.pl/produkcja-roslinna/ochrona-roslin/w-jakiej-temperaturze-przymrozki-uszkadzaja-rosliny-uprawne-wiosna-to-zalezy-od-gatunku,145219.html), [IUNG — KBW](https://www.iung.pl/2024/06/05/klimatyczny-bilans-wodny-od-1-kwietnia-do-31-maja-2024-roku/), [GUS — ocena upraw 2012](https://stat.gov.pl:443/cps/rde/xbcr/gus/RL_wiosenna_ocena_stanu_upraw_2012.pdf).

## 3. Dane

| Zbiór | Źródło | Zakres | Procesy | Notatnik |
| --- | --- | --- | --- | --- |
| Pogoda godzinowa | ERA5-Land (Copernicus CDS) | 2010–2024, siatka 0,1° (~4 100 punktów), **~0,5 mld wierszy** | S1, S2, D, H, B2 | 01 |
| NDVI | MODIS MOD13Q1 (250 m, co 16 dni) z [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/dataset/modis-13Q1-061) | 2001–2024, **~2,8 mld pikseli** Polski (~100 GB) | B1, B2, G, H | 02 |
| Mapa pól | [ESA WorldCover](https://planetarycomputer.microsoft.com/dataset/esa-worldcover) (Planetary Computer); typy upraw: EUCROPMAP (JRC) | 2021 / 2018 | maska pól | 02 |
| Pomiary stacji | IMGW (dane publiczne) | 2010–2024, 57 stacji | kalibracja S2, sprawdzanie ERA5-Land | 03 |
| Granice | GUGiK: WFS (wszystkie naraz) + [ULDK](https://uldk.gugik.gov.pl/opis.html) (jednostka po kodzie TERYT lub punkcie) | województwa, powiaty | wszystkie | 05 |
| Plony | [GUS BDL API](https://api.stat.gov.pl/Home/BdlApi) | 2010–2024 × 16 województw (**brak danych dla powiatów**) | B2 | 04 |

Najważniejsze uwagi (szczegóły w notatnikach):
- ERA5-Land: temperatura w kelwinach, śnieg w metrach, opad **narasta od 00 UTC** (trzeba odejmować kolejne godziny). ET₀ liczymy sami (FAO-56).
- NDVI: Polskę pokrywają 4 kafelki MODIS (h18–h19, v03–v04), ok. 44 MB każdy na 16 dni. Link do pliku podpisujemy anonimowo przez API Planetary Computer (`/api/sas/v1/sign`). Chmury maskujemy warstwą `pixel_reliability`. Jeden produkt na całe 2001–2024, więc norma i bieżące lata są porównywalne.
- GUS: „pszenica” bez podziału na ozimą/jarą; brak uprawy zapisany jako 0. Limit bez klucza: 5 zapytań/s, 100/15 min, 1000/12 h — nam wystarczy.
- ULDK: `GetCountyByXY` przypisuje punkt (np. stację IMGW) do powiatu jednym zapytaniem; geometrie od razu w EPSG:4326 (`srid=4326`).

**Dostępy — jedno konto:**
- **Copernicus CDS** (pogoda, [instrukcja](https://cds.climate.copernicus.eu/how-to-api)): zaakceptować licencję ERA5-Land, plik `~/.cdsapirc` z tokenem (nie commitujemy), `pip install "cdsapi>=0.7.7"`.
- Bez kont: Planetary Computer (NDVI, WorldCover), IMGW, GUS (opcjonalny klucz `X-ClientId` podnosi limity 5×), GUGiK.

## 4. Architektura

```mermaid
flowchart LR
    API[ERA5-Land / NDVI / IMGW / GUS / GUGiK] -->|pobranie raz| RAW[(HDFS raw)]
    RAW --> ETL[Spark: obróbka] --> CLEAN[(HDFS Parquet)]
    CLEAN --> RP[Replay] --> K[[Kafka]] --> SS[Spark Streaming: S1, S2, D] --> CH[(ClickHouse)]
    CLEAN --> SB[Spark batch: B1, B2, G, H] --> CH
    CH --> GR[Grafana]
```

**Replay:** zima 2011/12 wysyłana do Kafki w tempie 1 h danych = 1 s, z oryginalnym czasem zdarzenia (event time, watermark).

## 5. Procesy

| Id | Proces | Tor | Wejście → wynik |
| --- | --- | --- | --- |
| S1 | Potrzeba nawodnienia | strumień | opad, ET₀, wilgotność gleby → sygnał „brak wody” |
| S2 | Przymrozki | strumień | temperatura powietrza i gleby → alert |
| **D** | Wymarzanie ozimin | strumień | temperatura + śnieg → ryzyko dla województwa |
| B1 | Stan upraw | batch | NDVI → regiony powyżej/poniżej normy 2001–2017 |
| B2 | Prognoza plonów | batch | NDVI + pogoda + plony GUS (2010–2024, 240 obserwacji na uprawę) → plon |
| **G** | Fenologia | batch | krzywa NDVI → start, szczyt, koniec sezonu |
| **H** | Anomalie NDVI | batch | NDVI + bilans wodny → regiony i daty do kontroli |

- **D:** godzina groźna = temperatura < progu (tabela w sekcji 2). Okno 72 h; alarm, gdy ≥ 50% punktów z oziminami w województwie ma ≥ 6 groźnych godzin (ostrzeżenie ≥ 30%).
- **G:** dla regionu i roku: uzupełnienie luk → wygładzenie (Savitzky–Golay) → start/koniec = 50% wysokości górki, szczyt = maksimum.
- **H:** (1) nagły spadek NDVI poniżej wygładzonej krzywej z G; (2) sezon poniżej mediany z lat 2001–2017. Normy nie liczymy z lat 2018–2024, bo zawierają testowane susze.

S2, B1, B2 rozpisują ich autorzy według tego samego wzoru co S1 poniżej.

### S1. Potrzeba nawodnienia (strumień)

**Co robi:** w sezonie (21.03–30.09) codziennie sprawdza dla każdego powiatu, czy polom brakuje wody. Wynik to mapa: zielony = OK, żółty = ostrzeżenie, czerwony = alarm, a przy alarmie e-mail.

**Po czym poznajemy brak wody (2 sygnały):**
1. Przez ostatnie 60 dni wyparowało dużo więcej wody, niż spadło deszczu (bilans: < −100 mm ostrzeżenie, < −150 mm alarm).
2. Gleba jest bardziej sucha niż zwykle o tej porze roku.

**Jakie dane:** tylko pogoda ERA5-Land, 9 zmiennych:
- opad,
- temperatura, punkt rosy, wiatr (2 składowe), promieniowanie słoneczne, ciśnienie — z nich liczymy parowanie,
- wilgotność gleby na 3 głębokościach.

Do tego wspólne tabele od reszty zespołu: granice powiatów i mapa pól (liczymy tylko punkty, gdzie są pola).

**Jak zdobyć dane:** pobieramy **raz, masowo** — nie odpytujemy API w trakcie działania.
1. Konto Copernicus CDS (sekcja 3).
2. Skrypt pobiera 1 plik = 1 miesiąc całej Polski ze wszystkimi zmiennymi; 2010–2024 to 180 plików. Ten sam plik służy D i S2, więc pobiera go jedna osoba dla wszystkich.
3. Pliki trafiają do HDFS, Spark zamienia je na tabelę (godzina × punkt).
4. Replay czyta tę tabelę i wysyła do Kafki godzina po godzinie, jakby dane przychodziły na żywo. S1 czyta z Kafki.

**Test:** susza 2018 → alarmy od maja–czerwca na zachodzie i w centrum Polski; mokry rok (2010 lub 2017) → brak alarmów; zgodność z raportami suszy IUNG.

**Co pokazujemy profesorowi (rozproszenie):**
- Kafka na 3 węzłach — dane podzielone na części, awaria jednego węzła nie zatrzymuje strumienia.
- Spark na kilku węzłach — w trakcie demo dokładamy węzły i obliczenia przyspieszają.
- Po awarii joba wznawia od miejsca przerwania, bez utraty danych.
- Złe rekordy trafiają do osobnej kolejki, reszta liczy się dalej.
- Grafana pokazuje mapę, obciążenie strumienia i wysyła e-mail przy alarmie.

**Narzędzia w S1:**
- **Airflow** — jednym przyciskiem przygotowuje dane po kolei: pliki pogody → tabela → parowanie → normy wilgotności → start replayu.
- **Kafka UI** — podgląd w przeglądarce: płynące wiadomości, podział na części, czy S1 nadąża czytać.
- **Prometheus + cAdvisor** — obciążenie kontenerów i tempo strumienia na wykresach w Grafanie.
- **GitHub Actions** — po każdym pushu testy S1 na małej próbce (np. 80 dni bez deszczu → musi wyjść alarm).
- **Lokalny model AI (Ollama)** — przy alarmie dostaje liczby z bazy (bilans, wilgotność, dni bez deszczu) i pisze krótki komunikat po polsku do e-maila. Działa na naszym serwerze, bez płatnego API; mały model (np. polski Bielik albo Qwen 2.5 3B — wybór po teście na serwerze). Model tylko opisuje, nic nie liczy.

## 6. Narzędzia

- **Rdzeń:** Docker Swarm, HDFS + Parquet, Kafka, Spark (batch + Structured Streaming), ClickHouse, Grafana.
- **Dodatki:** Airflow, Kafka UI, Schema Registry (Avro), Prometheus + cAdvisor, Grafana Alerting, kontrola jakości danych (pandera), GitHub Actions.
- **AI:** opis alertów po polsku przez lokalny model (Ollama).
- **Jeśli starczy czasu:** MLflow (B2).

## 7. Jak sprawdzamy wyniki

Porównujemy ze źródłami, których **nie używamy w obliczeniach**. Cele to propozycja na start.

| Proces | Porównujemy z | Cel |
| --- | --- | --- |
| Dane ERA5-Land | stacje IMGW | błąd temperatury ≤ 2 °C |
| D | [GUS — Wiosenna ocena stanu upraw](https://stat.gov.pl/obszary-tematyczne/rolnictwo-lesnictwo/uprawy-rolne-i-ogrodnicze/wiosenna-ocena-stanu-upraw-rolnych-i-ogrodniczych-w-2024-roku,10,21.html) (co roku); spadek plonu rzepaku 2012 | alarmy w latach i województwach ze stratami |
| S2 | IMGW: minimum przy gruncie | ≥ 70% przymrozków złapanych, ≤ 30% fałszywych alarmów |
| S1 | [IUNG — monitoring suszy](https://susza.iung.pulawy.pl) | te same okresy suszy |
| H, B1 | [European Drought Observatory](https://drought.emergency.copernicus.eu) (wskaźnik suszy, stan roślinności) | anomalie 2018–2019 tam, gdzie EDO |
| G | [Copernicus HR-VPP](https://collections.sentinel-hub.com/vegetation-phenology-and-productivity-parameters-season-1) (wymaga konta Copernicus Data Space — tylko do porównania) | ta sama kolejność lat (wcześniejszy/późniejszy sezon) |
| B2 | plony GUS z lat pominiętych w uczeniu; [JRC MARS](https://joint-research-centre.ec.europa.eu/scientific-activities/crop-monitoring-and-yield-forecasting_en) | lepiej niż „średnia z 3 poprzednich lat” |

## 8. Ryzyka i plan B

- **Kolejka CDS za wolna** — pogoda z Open-Meteo (`lib/data_downloader.py`, bez konta).
- **Planetary Computer niedostępny** — ten sam MODIS z NASA LP DAAC (darmowe konto Earthdata).
- **Serwery bez Dockera lub miejsca** (NDVI ~100 GB + pogoda) — Swarm na laptopach; mniej lat.
- **EUCROPMAP niedostępny** — sama maska pól z WorldCover (bez typów upraw).
- Do ustalenia: współrzędne stacji IMGW, straty zimowe GUS per województwo, pliki z European Drought Observatory.

**Minimum na zaliczenie:** dane w HDFS + G + D z replayem zimy 2011/12, w Swarmie, wyniki w Grafanie.

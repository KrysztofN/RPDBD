# S1 — wiedza do prezentacji

Krótko i po kolei: co robimy, na jakich danych i dlaczego tak.

## 1. Problem

Rolnik i doradca chcą wiedzieć **wcześnie**, gdzie polom zaczyna brakować wody — zanim susza zniszczy plon. S1 co dzień w sezonie (21.03–30.09) pokazuje mapę powiatów: zielony / żółty (ostrzeżenie) / czerwony (alarm).

## 2. Skąd wiemy, że brakuje wody

- **Bilans wody (KBW)** = opad − parowanie, suma z ostatnich **60 dni**. Ujemny = więcej wody uciekło, niż spadło. Tak liczy oficjalny monitoring suszy IUNG (60 dni, 21.03–30.09); próg suszy dla gleb lekkich to ok. −150 mm.
- **Wilgotność gleby względem normy.** Ta sama wilgotność bywa normalna w sierpniu, a w kwietniu oznacza suszę — dlatego porównujemy z tym, co zwykle jest **w tym miejscu o tej porze roku** (percentyle z 15 lat).
- Dwa sygnały razem dają mniej fałszywych alarmów niż jeden.

## 3. Dane: ERA5-Land

- **Reanaliza** = odtworzenie pogody z przeszłości. Model pogodowy (jak do prognoz) liczy przeszłe lata i jest na bieżąco poprawiany prawdziwymi pomiarami (stacje, satelity). Wynik: dane dla **każdej godziny i każdego miejsca**, bez dziur.
- To **nie prognoza** (mówi, jak było) i **nie czysta symulacja** (trzyma się pomiarów).
- Wady: wartość to średnia z kratki ~9 km, więc ekstremy są łagodniejsze niż na stacji; wilgotność gleby jest wyliczona, nie zmierzona — dobra do porównań z normą.
- Dostawca: Copernicus Climate Data Store (UE/ECMWF), darmowe konto.

## 4. Siatka co 0,1°

- Dane są w punktach co 0,1° szerokości i długości geograficznej.
- 0,1° szerokości ≈ 11 km, 0,1° długości w Polsce ≈ 7 km → punkt = kratka ~11 × 7 km.
- Pobieramy prostokąt 49–55° N, 14–24,2° E: 61 × 103 = 6 283 punkty, z czego **4 117 w Polsce**.

## 5. Zmienne i pułapki

| Zmienna | Co to | Pułapka |
| --- | --- | --- |
| `tp` | opad | w metrach i **narasta przez dobę** (od 01:00, o 00:00 suma całej doby) — godzinowy = różnica z poprzednią godziną |
| `pev` | parowanie potencjalne (gotowe) | ujemne, narasta; podobno zaniżone nad lasami — porównujemy ze wzorem |
| `t2m`, `d2m`, `u10`, `v10`, `ssrd` | temperatura, punkt rosy, wiatr, promieniowanie | do wzoru FAO-56 na parowanie; temperatura w kelwinach |
| `swvl1–3` | wilgotność gleby 0–7, 7–28, 28–100 cm | średnia ważona grubością warstw |

Ciśnienia nie pobieramy — wzór FAO-56 pozwala policzyć je z wysokości punktu.

## 6. Pobieranie

- Limit CDS: **12 000 „pól” na zapytanie** (pole = zmienna × dzień × godzina). Miesiąc z 10 zmiennymi = 10 × 31 × 24 = 7 440 → jedno zapytanie = jeden miesiąc.
- 2010–2024, tylko miesiące I–IX (sezon 21.03–30.09 + 60 dni wstecz): 135 plików NetCDF po ≈ 67 MB → ~9 GB.
- Pobieramy **raz, masowo**; w trakcie działania systemu nie pytamy żadnego API.

## 7. Tabela punktów

Dla każdego z 4 117 punktów: powiat, województwo (granice GUGiK), **udział pól** w kratce (mapa ESA WorldCover, klasa 40 = pola) i wysokość (Open-Meteo). S1 liczy tylko punkty z ≥ 20% pól — las czy miasto nie powinny wywoływać alarmu „polom brakuje wody”.

Wynik (`data/points.csv`): 4 117 punktów w 364 powiatach; **2 805 punktów ma ≥ 20% pól**. Najwięcej pól: opolskie, kujawsko-pomorskie, wielkopolskie (~50% kratki); najmniej: lubuskie, śląskie, podkarpackie (~22–24%) — zgadza się z rzeczywistością (lasy, góry, miasta). 16 małych powiatów (głównie miasta na prawach powiatu) nie ma żadnego punktu siatki — dla S1 bez znaczenia, bo to nie tereny rolnicze.

## 8. Dla jakiego obszaru alerty

| Poziom | Ile | Punktów z polami na jednostkę | Ocena |
| --- | --- | --- | --- |
| województwo | 16 | ~175 | za duże — mokry i suchy region uśredniają się |
| **powiat** | 380 | średnio 8 | **główny poziom** — konkretny dla doradcy, wystarczająco punktów |
| gmina (jak IUNG) | ~2 480 | ~1 | za mało — jedna kratka ERA5-Land jest większa niż gmina |
| punkt | 2 805 | 1 | najdrobniej, jak pozwalają dane (kratka ~11 × 7 km) |

Mniej niż kratka 9 km nie ma sensu — dane nie mają większej szczegółowości. Powiat z < 3 punktami z polami dostaje „za mało pól” zamiast alarmu z 1–2 punktów.

## 9. Strumień, czyli „na żywo” z historii

- **Replay** czyta tabelę i wysyła do Kafki godzinę po godzinie, jakby dane szły z 4 117 stacji na żywo.
- **Walidacja jest w strumieniu**: każda wiadomość sprawdzana (pola, zakresy, duble, spóźnienia), złe idą do osobnej kolejki. ERA5-Land prawie nie ma błędów, więc replay **celowo psuje ~0,1%** wiadomości — symulacja awarii czujników.
- Sprawdzenie plików po pobraniu to co innego: kontrola, czy pobieranie się udało.

## 10. Rozproszenie

- Przygotowanie danych (batch) dzielimy **po miesiącach** — kolejność nie ma znaczenia.
- Strumień dzielimy **po punktach**, nie po czasie: każdy worker obsługuje swoją grupę „stacji”, a czas płynie po kolei. Tak robi się w prawdziwych systemach (np. czujniki według regionów).
- Kafka na 3 maszynach (kopie danych), Spark na 4 workerach; awaria maszyny nie zatrzymuje systemu.

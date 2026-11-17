# System parkingów – klient równoległy

Rozwiązanie projektu 1 (system parkingów miejskich) oparte na `multiprocessing.Process`.

## Architektura rozwiązania ([main.py](main.py))

- Zgłoszenia dla różnych parkingów (`P1`–`P5`) są od siebie niezależne (stan jednego parkingu nie wpływa na pozostałe), natomiast w ramach jednego parkingu muszą być przetwarzane w kolejności chronologicznej zgłoszeń (auta nocujące wpływają na kolejne dni).
- Przy starcie klienta uruchamianych jest **5 trwałych procesów roboczych** (`multiprocessing.Process` komunikujących się przez `multiprocessing.Pipe`), po jednym dla każdego parkingu.
- Po odebraniu listy zgłoszeń na `POST /parking-data` klient natychmiast odsyła `201`, parsuje JSON, rozdziela zgłoszenia na 5 buforów (`array('i')`) i wysyła je równolegle do procesów roboczych.
- Każdy proces symuluje swój parking z użyciem kopca (`heapq`) czasów wyjazdu oraz wylicza dzienne statystyki (`ENTRIES`, `REJECTED`, `MAX_OCCUPANCY`, `REVENUE`, `AVG_STAY`, `MEDIAN_STAY`) również dla dni końcowych, w których dany parking nie miał nowych wjazdów.

## Uruchomienie

1. Instalacja zależności:
   ```bash
   pip install -r requirements.txt
   ```
2. Serwer (bez zmian, w katalogu `lab-server`):
   ```bash
   cd lab-server
   python -m uvicorn main:app --port 5678
   ```
3. Klient równoległy (w katalogu głównym):
   ```bash
   python main.py
   ```
   Opcjonalnie można podać parametry z linii poleceń: `--server-ip`, `--server-port`, `--client-ip`, `--client-port`, `--indeks`.

## Testy i benchmark

- Testy jednostkowe i zgodność z zapisanymi payloadami serwera:
  ```bash
  python -m unittest test_client.py -v
  ```
- Benchmark porównawczy na żywo przeciwko `lab-server` i `lab-client-oneThread`:
  ```bash
  python benchmark.py --variants S1 S3 M2
  ```

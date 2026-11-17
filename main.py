import argparse
import array
import heapq
import json
import logging
import multiprocessing
import threading
import time
from contextlib import asynccontextmanager

import requests
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Request

logging.basicConfig(
    handlers=[logging.StreamHandler()],
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

# Parkingi i ich parametry (takie same jak w treści zadania i na serwerze)
PARKINGS = ['P1', 'P2', 'P3', 'P4', 'P5']
CAPACITY = {'P1': 20, 'P2': 35, 'P3': 15, 'P4': 50, 'P5': 10}   # liczba miejsc
RATE = {'P1': 0.10, 'P2': 0.08, 'P3': 0.15, 'P4': 0.05, 'P5': 0.25}  # zł za minutę postoju
MINUTES_PER_DAY = 24 * 60

AGGREGATE_TYPES = ['ENTRIES', 'REJECTED', 'MAX_OCCUPANCY', 'REVENUE', 'AVG_STAY', 'MEDIAN_STAY']
PARKING_FIELDS = [
    (p, f'{p}_ENTRIES', f'{p}_REJECTED', f'{p}_MAX_OCCUPANCY',
     f'{p}_REVENUE', f'{p}_AVG_STAY', f'{p}_MEDIAN_STAY')
    for p in PARKINGS
]

SERVER_IP = 'localhost'
SERVER_PORT = 5678

CLIENT_IP = 'localhost'
CLIENT_PORT = 6780

INDEKS = 12

_workers = {}
_pool_lock = threading.Lock()


def simulate_parking(parking: str, flat_buf: bytes, n_days: int):
    """Symuluje pojedynczy parking w kolejności chronologicznej zgłoszeń."""
    if n_days <= 0:
        return [], [], [], [], [], []

    cap = CAPACITY[parking]
    rate = RATE[parking]
    events = array.array('i')
    events.frombytes(flat_buf)
    n = len(events)

    heap = []
    stays = []
    day = -1
    rev = 0.0
    cur_max = 0

    entries = [0] * n_days
    rejected = [0] * n_days
    max_occ = [0] * n_days
    revenue = [0.0] * n_days
    avg_stay = [0.0] * n_days
    median_stay = [0.0] * n_days

    heappush = heapq.heappush
    heappop = heapq.heappop

    def close_day(d: int):
        nonlocal rev
        revenue[d] = round(rev, 2)
        if stays:
            stays.sort()
            cnt = len(stays)
            avg_stay[d] = round(sum(stays) / cnt, 2)
            mid = cnt // 2
            median_stay[d] = stays[mid] if (cnt & 1) else (stays[mid - 1] + stays[mid]) / 2
            stays.clear()
        rev = 0.0

    def open_day(d: int):
        nonlocal cur_max
        midnight = d * MINUTES_PER_DAY
        while heap and heap[0] <= midnight:
            heappop(heap)
        cur_max = len(heap)
        max_occ[d] = cur_max

    for i in range(0, n, 3):
        d = events[i]
        enter = events[i + 1]
        exit_ = events[i + 2]

        while day < d:
            if day >= 0:
                close_day(day)
            day += 1
            open_day(day)

        entries[d] += 1
        now = d * MINUTES_PER_DAY + enter
        while heap and heap[0] <= now:
            heappop(heap)

        if len(heap) < cap:
            heappush(heap, d * MINUTES_PER_DAY + exit_)
            stay = exit_ - enter
            rev += stay * rate
            stays.append(stay)
            occ = len(heap)
            if occ > cur_max:
                cur_max = occ
                max_occ[d] = occ
        else:
            rejected[d] += 1

    while day < n_days - 1:
        if day >= 0:
            close_day(day)
        day += 1
        open_day(day)

    if day >= 0:
        close_day(day)

    return entries, rejected, max_occ, revenue, avg_stay, median_stay


def _worker_loop(parking: str, conn):
    """Pętla robocza procesu obsługującego jeden niezależny parking."""
    while True:
        msg = conn.recv()
        if msg is None:
            break
        flat_buf, n_days = msg
        conn.send(simulate_parking(parking, flat_buf, n_days))


def start_workers():
    """Uruchamia po jednym trwałym procesie roboczym dla każdego parkingu (P1..P5)."""
    for p in PARKINGS:
        parent_conn, child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=_worker_loop, args=(p, child_conn), daemon=True)
        proc.start()
        child_conn.close()
        _workers[p] = (proc, parent_conn)


def stop_workers():
    """Zamyka procesy robocze przy wyłączaniu aplikacji."""
    for p, (proc, conn) in list(_workers.items()):
        try:
            conn.send(None)
        except OSError:
            pass
        proc.join(timeout=2)
        if proc.is_alive():
            proc.terminate()
    _workers.clear()


def compute_aggregates(raw: bytes):
    """Parsuje dane wejściowe, rozdziela je po parkingach i liczy wyniki równolegle."""
    data = json.loads(raw)
    if not data:
        return [], 0, 0

    buckets = {p: [] for p in PARKINGS}
    max_day = -1
    for e in data:
        d = e['day']
        if d > max_day:
            max_day = d
        buckets[e['parking']].extend((d, e['enter'], e['exit']))

    n_days = max_day + 1
    with _pool_lock:
        for p in PARKINGS:
            buf = array.array('i', buckets[p]).tobytes()
            _workers[p][1].send((buf, n_days))
        per_parking = [_workers[p][1].recv() for p in PARKINGS]

    aggregates = []
    for d in range(n_days):
        row = {'day': d}
        for idx_p, (_, k_en, k_rj, k_mo, k_rv, k_av, k_md) in enumerate(PARKING_FIELDS):
            en, rj, mo, rv, av, md = per_parking[idx_p]
            row[k_en] = en[d]
            row[k_rj] = rj[d]
            row[k_mo] = mo[d]
            row[k_rv] = rv[d]
            row[k_av] = av[d]
            row[k_md] = md[d]
        aggregates.append(row)

    return aggregates, len(data), n_days


def process_and_send(raw: bytes):
    start = time.perf_counter()
    aggregates, n_events, n_days = compute_aggregates(raw)
    elapsed = time.perf_counter() - start
    logging.info(f'Przetworzono {n_events} zdarzeń w {elapsed:.3f} s, wysyłam wynik ({n_days} dni)')
    res = requests.post(
        f"http://{SERVER_IP}:{SERVER_PORT}/results",
        json={"ip_addr": CLIENT_IP, "port": CLIENT_PORT, "indeks": INDEKS, "aggregates": aggregates},
        timeout=60,
    )
    print_verdict(res)


# ---------------------------------------------------------------------------
# Komunikacja z serwerem
# ---------------------------------------------------------------------------

def print_verdict(res):
    if res.status_code != 200:
        logging.error(f'Serwer odrzucił wynik ({res.status_code}): {res.text[:300]}')
        return
    v = res.json()
    speedup = f", przyspieszenie względem wzorca x{v['speedup']:.2f}" if v.get('speedup') else ''
    processing = f" (przetwarzanie {v['processing']:.3f} s)" if v.get('processing') is not None else ''
    logging.info(
        f"WERDYKT SERWERA: {v['score']}/{v['total']} poprawnych, "
        f"czas {v['duration']:.3f} s{processing}{speedup}"
    )
    if v['missing_days']:
        logging.warning(f"Brakujące dni w wyniku: {v['missing_days'][:20]}")
    if v['extra_days']:
        logging.warning(f"Nadmiarowe dni w wyniku: {v['extra_days'][:20]}")
    if v['differences_count']:
        logging.warning(f"Różnice względem wzorca ({v['differences_count']}, pokazano {len(v['differences'])}):")
        for d in v['differences']:
            logging.warning(
                f"  dzień {d['day']:>4} {d['field']:<17} klient {d['client']:>10} "
                f"wzorzec {d['expected']:>10} różnica {d['diff']:+}"
            )


def register():
    """Rejestracja na serwerze - ponawiana, dopóki serwer nie odpowie."""
    while True:
        try:
            res = requests.post(
                f"http://{SERVER_IP}:{SERVER_PORT}/clients/handshake",
                json={"ip_addr": CLIENT_IP, "port": CLIENT_PORT, "indeks": INDEKS},
                timeout=5,
            )
            if res.status_code == 201:
                logging.info(f'Zarejestrowano na serwerze {SERVER_IP}:{SERVER_PORT} jako {INDEKS}. Czekam na dane...')
                return
            logging.error(f'Serwer odrzucił rejestrację ({res.status_code}): {res.text}')
        except requests.RequestException:
            logging.warning(f'Serwer {SERVER_IP}:{SERVER_PORT} niedostępny - ponowna próba za 3 s')
        time.sleep(3)


@asynccontextmanager
async def lifespan(app):
    start_workers()
    threading.Thread(target=register, daemon=True).start()
    try:
        yield
    finally:
        stop_workers()


app = FastAPI(lifespan=lifespan)


@app.post("/parking-data", status_code=201)
async def create_parking_data(request: Request, background_tasks: BackgroundTasks):
    raw = await request.body()
    logging.info(f'Otrzymano dane ({len(raw)} B) - przetwarzanie')
    background_tasks.add_task(process_and_send, raw)
    return {"status": "accepted"}


def parse_args():
    parser = argparse.ArgumentParser(description='Równoległy klient systemu parkingów')
    parser.add_argument('--server-ip', default=SERVER_IP)
    parser.add_argument('--server-port', type=int, default=SERVER_PORT)
    parser.add_argument('--client-ip', default=CLIENT_IP)
    parser.add_argument('--client-port', type=int, default=CLIENT_PORT)
    parser.add_argument('--indeks', type=int, default=INDEKS)
    return parser.parse_args()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    args = parse_args()
    SERVER_IP = args.server_ip
    SERVER_PORT = args.server_port
    CLIENT_IP = args.client_ip
    CLIENT_PORT = args.client_port
    INDEKS = args.indeks
    uvicorn.run(app, host=CLIENT_IP, port=CLIENT_PORT, log_level='warning')

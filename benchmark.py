"""Skrypt weryfikacyjno-benchmarkowy: uruchamia serwer oraz obu klientów (oneThread i równoległego)
i porównuje poprawność oraz czasy wykonania dla wybranych wariantów eksperymentów.
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
SERVER_URL = 'http://localhost:5678'


def wait_for_clients(expected_ids, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f'{SERVER_URL}/api/state', timeout=2)
            if r.status_code == 200:
                clients = {c['indeks']: c for c in r.json().get('clients', [])}
                if all(cid in clients for cid in expected_ids):
                    return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise TimeoutError(f'Klienci {expected_ids} nie zarejestrowali się w {timeout}s')


def run_variant(variant: str, expected_ids, timeout=180.0):
    r = requests.post(f'{SERVER_URL}/api/start', json={'variant': variant}, timeout=5)
    if r.status_code != 200:
        raise RuntimeError(f'Nie udało się uruchomić wariantu {variant}: {r.status_code} {r.text}')

    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = requests.get(f'{SERVER_URL}/api/state', timeout=5).json()
        clients = {c['indeks']: c for c in snap['clients']}
        runs = snap['runs']
        if runs and runs[-1]['variant'] == variant:
            run = runs[-1]
            if (
                run.get('reference_status') == 'done'
                and all(clients[cid].get('run') == run['id'] and clients[cid].get('status') == 'done'
                        for cid in expected_ids)
            ):
                return run, clients
            for cid in expected_ids:
                if clients[cid].get('status') == 'error':
                    raise RuntimeError(f'Błąd klienta {cid}: {clients[cid].get("error")}')
        time.sleep(0.25)
    raise TimeoutError(f'Timeout podczas oczekiwania na wariant {variant}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--variants', nargs='+', default=['S1', 'S3', 'M2'])
    parser.add_argument('--only-parallel', action='store_true',
                        help='Uruchom tylko klienta równoległego (pomija oneThread)')
    args = parser.parse_args()

    procs = []
    try:
        server_proc = subprocess.Popen(
            [sys.executable, '-m', 'uvicorn', 'main:app', '--port', '5678', '--log-level', 'warning'],
            cwd=ROOT / 'lab-server',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(server_proc)

        expected_ids = [100]
        if not args.only_parallel:
            seq_proc = subprocess.Popen(
                [sys.executable, 'main.py'],
                cwd=ROOT / 'lab-client-oneThread',
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            procs.append(seq_proc)
            expected_ids.insert(0, 12)

        par_proc = subprocess.Popen(
            [sys.executable, 'main.py', '--client-port', '6781', '--indeks', '100'],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(par_proc)

        wait_for_clients(expected_ids)
        print(f'{"Wariant":<7} {"Zdarzenia":>10} {"Wzorzec(s)":>11} {"1T proc(s)":>11} {"MP proc(s)":>11} {"MP tot(s)":>10} {"Speedup":>8} {"Popr.":>12} {"Warn":>5}')
        print('-' * 95)

        for v in args.variants:
            run, clients = run_variant(v, expected_ids)
            c_par = clients[100]
            ref_p = run.get('reference_processing') or 0.0
            seq_str = f'{clients[12].get("processing") or 0.0:>11.3f}' if 12 in clients else f'{"-":>11}'
            par_p = c_par.get('processing') or 0.0
            par_d = c_par.get('duration') or 0.0
            sp = c_par.get('speedup') or 0.0
            ok_str = f"{c_par.get('score')}/{c_par.get('total')}"
            warn = '!' if c_par.get('result_before_ack') else 'OK'
            print(f'{v:<7} {run["events"]:>10} {ref_p:>11.3f} {seq_str} {par_p:>11.3f} {par_d:>10.3f} {sp:>7.2f}x {ok_str:>12} {warn:>5}')
            if c_par.get('score') != c_par.get('total'):
                raise AssertionError(f'Błędny wynik dla {v}: {ok_str}')
            if c_par.get('result_before_ack'):
                raise AssertionError(f'Wynik dotarł przed 201 dla {v}')

    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        for p in reversed(procs):
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == '__main__':
    main()

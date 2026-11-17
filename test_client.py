import json
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import main as client


class TestParallelParkingClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        client.start_workers()

    @classmethod
    def tearDownClass(cls):
        client.stop_workers()

    def test_pdf_example_day_by_day_and_exact_minute(self):
        # Z przykładu na str. 3 PDF (dla P1 o pojemności 20 nie będzie odrzuceń,
        # ale sprawdzamy wyjazd w minucie wjazdu i nocowanie dla P5 o pojemności 10).
        events = []
        # Zapełniamy P5 (CAPACITY=10): 10 aut stoi od 480 do 500, 11. auto o 490 odrzucone,
        # 12. auto o 500 wjeżdża bo pierwsze 10 wyjeżdża w minucie 500.
        for i in range(10):
            events.append({'parking': 'P5', 'ticket': i + 1, 'day': 0, 'enter': 480, 'exit': 500})
        events.append({'parking': 'P5', 'ticket': 11, 'day': 0, 'enter': 490, 'exit': 530})
        events.append({'parking': 'P5', 'ticket': 12, 'day': 0, 'enter': 500, 'exit': 1560})
        # Dzień 1: P5 puste zdarzenia dla P5, zdarzenie tylko dla P1 -> P5 musi zachować nocujące auto
        events.append({'parking': 'P1', 'ticket': 13, 'day': 1, 'enter': 60, 'exit': 90})

        aggs, n_ev, n_days = client.compute_aggregates(json.dumps(events).encode())
        self.assertEqual(n_ev, 13)
        self.assertEqual(n_days, 2)

        d0, d1 = aggs[0], aggs[1]
        self.assertEqual(d0['P5_ENTRIES'], 12)
        self.assertEqual(d0['P5_REJECTED'], 1)
        self.assertEqual(d0['P5_MAX_OCCUPANCY'], 10)
        # 10 * 20 min * 0.25 + 1 * 1060 min * 0.25 = 50 + 265 = 315.0
        self.assertAlmostEqual(d0['P5_REVENUE'], 315.0, places=2)
        self.assertEqual(d1['P5_ENTRIES'], 0)
        self.assertEqual(d1['P5_MAX_OCCUPANCY'], 1)
        self.assertEqual(d1['P5_AVG_STAY'], 0)
        self.assertEqual(d1['P5_MEDIAN_STAY'], 0)
        self.assertEqual(d1['P1_ENTRIES'], 1)
        self.assertEqual(d1['P1_MAX_OCCUPANCY'], 1)

    def test_empty_payload(self):
        aggs, n_ev, n_days = client.compute_aggregates(b'[]')
        self.assertEqual(aggs, [])
        self.assertEqual(n_ev, 0)
        self.assertEqual(n_days, 0)

    def test_matches_all_server_payloads(self):
        payloads_dir = ROOT / 'lab-server' / 'app' / 'payloads'
        sys.path.insert(0, str(ROOT / 'lab-server'))
        from app import compare

        payload_files = sorted(payloads_dir.glob('run_[0-9]*.json'))
        payload_files = [p for p in payload_files if not p.name.endswith('_aggregates.json')]
        self.assertGreater(len(payload_files), 0)

        for p_file in payload_files:
            agg_file = p_file.with_name(p_file.stem + '_aggregates.json')
            with open(p_file, 'rb') as f:
                raw = f.read()
            with open(agg_file, 'r') as f:
                ref = json.load(f)

            aggs, _, _ = client.compute_aggregates(raw)
            ok, total, _, extra, diffs = compare.compare(ref, aggs)
            with self.subTest(payload=p_file.name):
                self.assertEqual(extra, [])
                self.assertEqual(diffs, [])
                self.assertEqual(ok, total)


if __name__ == '__main__':
    unittest.main()

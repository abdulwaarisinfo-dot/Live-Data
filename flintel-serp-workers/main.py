"""flintel-serp-workers — run:  python main.py   (or  python main.py --once  for a single round)"""
import argparse
import logging
import signal
import threading

from dotenv import load_dotenv

load_dotenv()

from serp_workers.board import StatusBoard          # noqa: E402
from serp_workers.config import load_settings       # noqa: E402
from serp_workers.orchestrator import Orchestrator  # noqa: E402
from serp_workers.serp_client import SerpClient     # noqa: E402
from serp_workers.status_server import start_status_server  # noqa: E402
from serp_workers.store import Store                # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run one round and exit")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    s = load_settings()
    if not s.mongodb_jobs_uri:
        raise SystemExit("MONGODB3 (jobs database URI) is not set")
    store = Store.from_settings(s)
    store.ensure_indexes()
    board = StatusBoard(store, s.workers)
    serp = SerpClient(s)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    if s.status_server_enabled:
        start_status_server(board, s.port)
    orch = Orchestrator(s, store, serp, board, stop)
    if args.once:
        orch.run_round()
    else:
        orch.run_forever()


if __name__ == "__main__":
    main()

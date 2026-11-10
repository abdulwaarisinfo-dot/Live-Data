"""ROLLING scheduler. WORKERS threads (W001..W100) live forever. The moment a worker is free it takes the NEXT
unclaimed unit (oldest job first) — there are no rounds/barriers, so one slow job never blocks the other workers.
Big jobs are split into parts (KEYWORDS_PER_PART keywords each) that different workers run in parallel."""
import logging
import threading
import time
from collections import deque
from datetime import timedelta

from .store import now_utc
from .units import build_units
from .worker import process_unit

log = logging.getLogger("flintel.orchestrator")


class Scheduler:
    """Hands out units. Thread-safe. Re-scans flintel_search_jobs at most once per POLL_SECONDS."""

    def __init__(self, settings, store):
        self.s, self.store = settings, store
        self._lock = threading.Lock()
        self._units = deque()
        self._last_scan = 0.0

    def _window_start(self):
        return now_utc() - timedelta(hours=self.s.job_max_age_hours) if self.s.job_max_age_hours > 0 else None

    def scan(self):
        """Queue every not-yet-done unit of the recent jobs, oldest job first."""
        ws = self._window_start()
        done = self.store.done_keys(ws)
        fresh = deque()
        for job in self.store.candidate_jobs(ws):
            if not job.get("topic_key") or not job.get("keywords"):
                continue
            for unit in build_units(job, self.s):
                if unit.key not in done:
                    fresh.append(unit)
        self._units = fresh
        self._last_scan = time.monotonic()
        return len(fresh)

    def next_unit(self, slot: int, allow_scan: bool = True):
        with self._lock:
            if not self._units and allow_scan and time.monotonic() - self._last_scan >= self.s.poll_seconds:
                self.scan()
            while self._units:
                unit = self._units.popleft()
                if self.store.claim(unit, slot):
                    return unit
            return None

    def pending(self) -> int:
        return len(self._units)


class Orchestrator:
    def __init__(self, settings, store, serp, board, stop_event=None):
        self.s, self.store, self.serp, self.board = settings, store, serp, board
        self.stop = stop_event or threading.Event()
        self.sched = Scheduler(settings, store)

    def _worker_loop(self, slot: int, once: bool = False):
        wid = self.board.worker_id(slot)
        while not self.stop.is_set():
            unit = self.sched.next_unit(slot, allow_scan=not once)
            if unit is None:
                if self.board.state_of(slot) != "idle":
                    self.board.update(slot, state="idle", topic_key=None, part=None, keywords=[], error=None)
                if once:
                    return
                self.stop.wait(self.s.poll_seconds)
                continue
            log.info(f"  [{wid}] → job={unit.topic_key} part {unit.part}/{unit.parts} | "
                     f"{len(unit.keywords)} keyword(s): {list(unit.keywords)[:4]}")
            try:
                process_unit(slot, unit, self.store, self.serp, self.board, self.s)
            except Exception as exc:        # one broken unit must never stop the other workers (or this one)
                log.exception(f"[{wid}] crashed: {exc}")
                self.store.finish(unit.key, False, {}, f"crash: {exc}")
                self.board.update(slot, state="failed", error=str(exc)[:200])
                self.board.add_totals(jobs_failed=1)

    def _reporter(self):
        while not self.stop.wait(self.s.report_seconds):
            snap = self.board.snapshot()
            log.info(f"STATUS | busy:{snap['busy']}/{self.s.workers} | queued units:{self.sched.pending()} | totals:{snap['totals']}")

    def run_once(self):
        """Process everything currently pending, then return (for --once and tests)."""
        self.board.persist_all()
        n = self.sched.scan()
        log.info(f"{n} unit(s) queued | workers:{self.s.workers}")
        threads = [threading.Thread(target=self._worker_loop, args=(slot, True), daemon=True, name=f"serp-{slot}")
                   for slot in range(1, self.s.workers + 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return self.board.snapshot()["totals"]

    def run_forever(self):
        log.info(f"Service started | WORKERS={self.s.workers} (rolling, no rounds) | tbs={self.s.recent_tbs or '-'} | "
                 f"JOB_MAX_AGE_HOURS={self.s.job_max_age_hours} | platforms={self.s.job_platforms} | "
                 f"keywords/call={self.s.keywords_per_call} | keywords/part={self.s.keywords_per_part} | rps={self.s.rapidapi_max_rps}")
        self.board.persist_all()
        threads = [threading.Thread(target=self._worker_loop, args=(slot,), daemon=True, name=f"serp-{slot}")
                   for slot in range(1, self.s.workers + 1)]
        threads.append(threading.Thread(target=self._reporter, daemon=True, name="reporter"))
        for t in threads:
            t.start()
        while not self.stop.wait(1.0):
            pass
        log.info("Stopping … waiting for running units to finish")
        for t in threads:
            t.join(timeout=self.s.serp_timeout_seconds + 5)

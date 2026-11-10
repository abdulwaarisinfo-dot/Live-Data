"""Rounds of WORKERS jobs: jobs 1..100 -> W001..W100; when ALL finish, jobs 101..200 -> W001..W100; and so on."""
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from .store import now_utc
from .worker import process_job

log = logging.getLogger("flintel.orchestrator")


class Orchestrator:
    def __init__(self, settings, store, serp, board, stop_event=None):
        self.s, self.store, self.serp, self.board = settings, store, serp, board
        self.stop = stop_event or threading.Event()

    def _window_start(self):
        return now_utc() - timedelta(hours=self.s.job_max_age_hours) if self.s.job_max_age_hours > 0 else None

    def claim_next_batch(self, round_no: int) -> list:
        """Claim up to WORKERS unprocessed jobs, oldest first. Returns [(slot, job), ...] with slot = 1..N."""
        ws = self._window_start()
        done = self.store.done_keys(ws)
        from .store import job_key
        batch = []
        for job in self.store.candidate_jobs(ws):
            if len(batch) >= self.s.workers:
                break
            if not job.get("topic_key") or not job.get("keywords") or job_key(job) in done:
                continue
            slot = len(batch) + 1
            if self.store.claim(job, slot, round_no):
                batch.append((slot, job))
        return batch

    def run_round(self) -> int:
        self.board.round_no += 1
        rno = self.board.round_no
        batch = self.claim_next_batch(rno)
        if not batch:
            for slot in range(1, self.s.workers + 1):
                self.board.update(slot, state="idle", topic_key=None, keywords=[], error=None)
            return 0
        log.info(f"ROUND {rno} | {len(batch)} job(s) claimed → workers W001..W{len(batch):03d} "
                 f"({self.s.workers - len(batch)} idle)")
        for slot, job in batch:      # roster: everyone knows who does what
            log.info(f"  [{self.board.worker_id(slot)}] → job={job.get('topic_key')} | "
                     f"keywords={(job.get('keywords') or [])[:4]}")
        used = {slot for slot, _ in batch}
        for slot in range(1, self.s.workers + 1):
            if slot not in used:
                self.board.update(slot, state="idle", topic_key=None, keywords=[], error=None)

        def _run(item):
            slot, job = item
            try:
                return process_job(slot, job, self.store, self.serp, self.board, self.s)
            except Exception as exc:      # one broken job must never stop the other 99
                log.exception(f"[{self.board.worker_id(slot)}] crashed: {exc}")
                from .store import job_key
                self.store.finish(job_key(job), False, {}, f"crash: {exc}")
                self.board.update(slot, state="failed", error=str(exc)[:200])
                self.board.add_totals(jobs_failed=1)
                return {}

        with ThreadPoolExecutor(max_workers=self.s.workers, thread_name_prefix="serp") as pool:
            list(pool.map(_run, batch))
        t = self.board.snapshot()["totals"]
        log.info(f"ROUND {rno} finished | totals: {t}")
        return len(batch)

    def run_forever(self):
        log.info(f"Service started | WORKERS={self.s.workers} | RECENT_HOURS={self.s.recent_hours} | "
                 f"JOB_MAX_AGE_HOURS={self.s.job_max_age_hours} | platforms={self.s.job_platforms} | "
                 f"keywords/call={self.s.keywords_per_call} | rps={self.s.rapidapi_max_rps}")
        while not self.stop.is_set():
            try:
                n = self.run_round()
            except Exception as exc:
                log.exception(f"round failed: {exc}")
                n = 0
            if n == 0:
                self.stop.wait(self.s.poll_seconds)

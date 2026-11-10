"""Shared status board: every worker (employee) reports what it is doing; everybody can see everybody."""
import threading
from datetime import datetime, timezone


class StatusBoard:
    def __init__(self, store=None, workers: int = 100):
        self.store, self.workers = store, workers
        self._lock = threading.Lock()
        self.round_no = 0
        self.totals = {"jobs_done": 0, "jobs_failed": 0, "serp_calls": 0, "results": 0, "new_posts": 0}
        self.slots = {i: {"slot": i, "worker": f"W{i:03d}", "state": "idle"} for i in range(1, workers + 1)}

    @staticmethod
    def worker_id(slot: int) -> str:
        return f"W{slot:03d}"

    def update(self, slot: int, **fields):
        with self._lock:
            row = self.slots.setdefault(slot, {"slot": slot, "worker": self.worker_id(slot), "state": "idle"})
            row.update(fields)
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
            snap = dict(row)
        if self.store is not None:
            self.store.save_worker_status(self.worker_id(slot), snap)

    def add_totals(self, **inc):
        with self._lock:
            for k, v in inc.items():
                self.totals[k] = self.totals.get(k, 0) + v

    def snapshot(self) -> dict:
        with self._lock:
            return {"round": self.round_no, "totals": dict(self.totals),
                    "workers": [dict(v) for _, v in sorted(self.slots.items())]}

    def render_text(self) -> str:
        snap = self.snapshot()
        lines = [f"ROUND {snap['round']} | totals {snap['totals']}", ""]
        for w in snap["workers"]:
            kws = ", ".join((w.get("keywords") or [])[:3])
            lines.append(f"{w['worker']} {w.get('state', ''):<10} job={w.get('topic_key', '-')} "
                         f"calls={w.get('calls', 0)} results={w.get('results', 0)} new={w.get('new_posts', 0)} "
                         f"kw=[{kws}] {w.get('error') or ''}")
        return "\n".join(lines)

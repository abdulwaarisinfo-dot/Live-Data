"""Shared status board: every worker (employee) reports what it is doing; everybody can see everybody."""
import threading
from datetime import datetime, timezone


class StatusBoard:
    def __init__(self, store=None, workers: int = 100):
        self.store, self.workers = store, workers
        self._lock = threading.Lock()
        self.totals = {"jobs_done": 0, "jobs_failed": 0, "serp_calls": 0, "results": 0, "new_posts": 0}
        self.slots = {i: {"slot": i, "worker": f"W{i:03d}", "state": "idle", "handled": 0}
                      for i in range(1, workers + 1)}

    @staticmethod
    def worker_id(slot: int) -> str:
        return f"W{slot:03d}"

    def state_of(self, slot: int) -> str:
        with self._lock:
            return self.slots.get(slot, {}).get("state", "idle")

    def update(self, slot: int, **fields):
        with self._lock:
            row = self.slots.setdefault(slot, {"slot": slot, "worker": self.worker_id(slot), "state": "idle", "handled": 0})
            row.update(fields)
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
            snap = dict(row)
        if self.store is not None:
            self.store.save_worker_status(self.worker_id(slot), snap)

    def persist_all(self):
        """Write every worker's row once at start, so the status collection lists all of them (even idle ones)."""
        if self.store is None:
            return
        with self._lock:
            rows = [dict(v) for v in self.slots.values()]
        for row in rows:
            self.store.save_worker_status(self.worker_id(row["slot"]), row)

    def bump_handled(self, slot: int):
        with self._lock:
            row = self.slots.setdefault(slot, {"slot": slot, "worker": self.worker_id(slot), "state": "idle", "handled": 0})
            row["handled"] = row.get("handled", 0) + 1

    def add_totals(self, **inc):
        with self._lock:
            for k, v in inc.items():
                self.totals[k] = self.totals.get(k, 0) + v

    def snapshot(self) -> dict:
        with self._lock:
            workers = [dict(v) for _, v in sorted(self.slots.items())]
            busy = sum(1 for w in workers if w.get("state") in ("searching", "saving"))
            return {"busy": busy, "idle": len(workers) - busy, "totals": dict(self.totals), "workers": workers}

    def render_text(self) -> str:
        snap = self.snapshot()
        lines = [f"busy {snap['busy']} / idle {snap['idle']} | totals {snap['totals']}", ""]
        for w in snap["workers"]:
            kws = ", ".join((w.get("keywords") or [])[:3])
            lines.append(f"{w['worker']} {w.get('state', ''):<9} handled={w.get('handled', 0)} job={w.get('topic_key') or '-'} "
                         f"part={w.get('part') or '-'} calls={w.get('calls', 0)} results={w.get('results', 0)} "
                         f"new={w.get('new_posts', 0)} kw=[{kws}] {w.get('error') or ''}")
        return "\n".join(lines)

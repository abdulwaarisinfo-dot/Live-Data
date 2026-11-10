"""MongoDB access.
  * flintel_search_jobs          -> READ ONLY (never modified by this service)
  * flintel_google_posts         -> insert-only, SAME schema as flintel-commerce-agent.save_google_post()
  * flintel_serp_job_runs        -> our own claim/progress ledger (one row per job request)
  * flintel_serp_worker_status   -> one row per worker slot (what is each employee doing right now)
"""
import logging
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from .utils import normalize_reddit_post_url

log = logging.getLogger("flintel.store")


def now_utc():
    return datetime.now(timezone.utc)


def job_key(job: dict) -> str:
    ra = job.get("requested_at")
    ra = ra.isoformat() if hasattr(ra, "isoformat") else str(ra)
    return f"{job.get('topic_key')}|{ra}"


class Store:
    def __init__(self, jobs_col, posts_col, runs_col, status_col, settings):
        self.jobs, self.posts, self.runs, self.status = jobs_col, posts_col, runs_col, status_col
        self.s = settings

    @classmethod
    def from_settings(cls, s):
        kw = dict(serverSelectionTimeoutMS=15000, retryWrites=True)
        jobs_client = MongoClient(s.mongodb_jobs_uri, **kw)
        posts_client = jobs_client if s.mongodb_posts_uri == s.mongodb_jobs_uri else MongoClient(s.mongodb_posts_uri, **kw)
        jdb, pdb = jobs_client[s.mongodb_jobs_db], posts_client[s.mongodb_posts_db]
        return cls(jdb[s.jobs_collection], pdb[s.posts_collection], pdb[s.runs_collection], pdb[s.status_collection], s)

    def ensure_indexes(self):
        self.runs.create_index("job_key", unique=True)
        self.runs.create_index([("status", 1), ("requested_at", 1)])

    # ── jobs (read only) ─────────────────────────────────────────────────
    def candidate_jobs(self, window_start=None):
        """Oldest first, so jobs 1..100 go first, then 101..200 ... Python-side keyword check."""
        q = {"targeting_platform": {"$in": list(self.s.job_platforms) + [None]}}
        if window_start is not None:
            q["requested_at"] = {"$gte": window_start}
        return self.jobs.find(q).sort("requested_at", 1)

    def done_keys(self, window_start=None) -> set:
        q = {"status": "done"}
        if window_start is not None:
            q["requested_at"] = {"$gte": window_start}
        return {d["job_key"] for d in self.runs.find(q, {"job_key": 1})}

    # ── claims ───────────────────────────────────────────────────────────
    def claim(self, job: dict, slot: int, round_no: int) -> bool:
        """Atomic: only ONE worker/instance ever owns a job request. Also re-takes failed (attempts left) and stale runs."""
        key, now = job_key(job), now_utc()
        doc = {"job_key": key, "topic_key": job.get("topic_key"), "requested_at": job.get("requested_at"),
               "status": "claimed", "slot": slot, "round": round_no, "attempts": 1,
               "claimed_at": now, "heartbeat_at": now, "finished_at": None,
               "serp_calls": 0, "results": 0, "new_posts": 0, "error": None}
        try:
            self.runs.insert_one(doc)
            return True
        except DuplicateKeyError:
            pass
        stale = now - timedelta(seconds=self.s.stale_seconds)
        res = self.runs.find_one_and_update(
            {"job_key": key, "$or": [
                {"status": "failed", "attempts": {"$lt": self.s.max_attempts}},
                {"status": {"$in": ["claimed", "processing"]}, "heartbeat_at": {"$lt": stale},
                 "attempts": {"$lt": self.s.max_attempts}},
            ]},
            {"$set": {"status": "claimed", "slot": slot, "round": round_no, "claimed_at": now,
                      "heartbeat_at": now, "error": None},
             "$inc": {"attempts": 1}})
        return res is not None

    def heartbeat(self, key: str, status: str = "processing"):
        self.runs.update_one({"job_key": key}, {"$set": {"status": status, "heartbeat_at": now_utc()}})

    def finish(self, key: str, ok: bool, stats: dict, error: str = None):
        self.runs.update_one({"job_key": key}, {"$set": {
            "status": "done" if ok else "failed", "finished_at": now_utc(), "heartbeat_at": now_utc(),
            "serp_calls": stats.get("calls", 0), "results": stats.get("results", 0),
            "new_posts": stats.get("new_posts", 0), "error": error}})

    # ── flintel_google_posts — EXACT same document flintel-chatbot/google.py writes ──
    def save_google_post(self, post_url, google_rank, search_keyword, subreddit, fuzzy_keywords) -> bool:
        """$setOnInsert keyed by post_url (an existing post — maybe already reddit_fetched=True — is NEVER reset),
        and the job's keywords are MERGED into fuzzy_keywords with $addToSet on every call.
        Returns True only when this call inserted a brand-new post."""
        canonical = normalize_reddit_post_url(post_url)
        if canonical is None:
            return False
        try:
            res = self.posts.update_one(
                {"post_url": canonical},
                {"$setOnInsert": {
                    "post_url": canonical,
                    "discovered_at": now_utc(),
                    "fetched_at": None,
                    "fuzzy_matched": True,
                    "google_rank": google_rank,
                    "next_retry_at": None,
                    "reddit_fetched": False,
                    "search_keyword": search_keyword,
                    "subreddit": subreddit},
                 "$addToSet": {"fuzzy_keywords": {"$each": list(fuzzy_keywords)}}},
                upsert=True)
            return res.upserted_id is not None
        except Exception as exc:
            log.error(f"save_google_post failed for {canonical}: {exc}")
            return False

    # ── worker status board persistence ─────────────────────────────────
    def save_worker_status(self, slot_id: str, doc: dict):
        try:
            self.status.update_one({"_id": slot_id}, {"$set": {**doc, "updated_at": now_utc()}}, upsert=True)
        except Exception as exc:     # board is best-effort, must never break a job
            log.debug(f"status write failed {slot_id}: {exc}")

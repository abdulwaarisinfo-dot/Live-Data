from datetime import datetime, timedelta, timezone
from unittest import mock

import mongomock

from serp_workers.board import StatusBoard
from serp_workers.config import Settings
from serp_workers.orchestrator import Orchestrator
from serp_workers.serp_client import SerpClient
from serp_workers.store import Store, job_key
from serp_workers.utils import generate_fuzzy_keywords, normalize_reddit_post_url


def make_settings(**kw):
    base = dict(mongodb_jobs_uri="x", mongodb_jobs_db="d", mongodb_posts_uri="x", mongodb_posts_db="d",
                rapidapi_key="k", rapidapi_max_rps=0, workers=100, recent_hours=24, job_max_age_hours=24,
                keywords_per_call=5, max_attempts=3, stale_seconds=600)
    base.update(kw)
    return Settings(**base)


def make_store(s):
    db = mongomock.MongoClient()["d"]
    st = Store(db["flintel_search_jobs"], db["flintel_google_posts"], db["flintel_serp_job_runs"],
               db["flintel_serp_worker_status"], s)
    st.ensure_indexes()
    return st


def add_jobs(st, n, platform="all", start=0):
    now = datetime.now(timezone.utc)
    for i in range(start, start + n):
        st.jobs.insert_one({"topic_key": f"topic{i:04d}", "keywords": [f"kw{i} alpha", f"kw{i} beta"],
                            "targeting_platform": platform, "status": "pending",
                            "requested_at": now - timedelta(minutes=n + start - i)})


class FakeResp:
    def __init__(self, data, code=200):
        self._d, self.status_code, self.headers = data, code, {}

    def json(self):
        return self._d


def fake_http(calls, params_log=None):
    def _get(url, headers=None, params=None, timeout=None):
        calls.append(params["query"])
        if params_log is not None:
            params_log.append(dict(params))
        q = params["query"]
        # one reddit post per keyword that appears in the query, slug contains the keyword
        import re
        kws = re.findall(r'"([^"]+)"', q)
        items = [{"url": f"https://old.reddit.com/r/test/comments/{abs(hash(k)) % 10**6:06x}/{k.replace(' ', '_')}/?utm=1",
                  "title": k, "position": i + 1} for i, k in enumerate(kws)]
        items.append({"url": "https://www.reddit.com/r/test/", "title": "sub root"})   # must be dropped
        return FakeResp({"results": items})
    return _get


def build(s, st, calls):
    return Orchestrator(s, st, SerpClient(s, http_get=fake_http(calls), sleep=lambda *_: None), StatusBoard(st, s.workers))


def test_url_and_fuzzy_ported():
    assert normalize_reddit_post_url("https://old.reddit.com/r/a/comments/abc/t/?x=1") == "https://www.reddit.com/r/a/comments/abc/t/"
    assert normalize_reddit_post_url("https://www.reddit.com/r/a/") is None
    assert "cart recovery" in generate_fuzzy_keywords("cart recovery")


def test_query_or_and_tbs_param_like_chatbot():
    s = make_settings()
    c = SerpClient(s)
    assert c.build_query(["a b", "c d"]) == 'site:reddit.com ("a b" OR "c d")'
    assert c.build_query(["x"]) == 'site:reddit.com "x"'
    assert "after:" not in c.build_query(["a", "b"])
    assert s.recent_tbs == "qdr:d"                                     # last 24 hours, same as chatbot google.py
    assert make_settings(recent_hours=1).recent_tbs == "qdr:h"
    assert make_settings(recent_hours=72).recent_tbs == "qdr:w"
    assert make_settings(recent_hours=0).recent_tbs == ""
    assert make_settings(recent_tbs_override="qdr:m").recent_tbs == "qdr:m"


def test_tbs_is_sent_to_api():
    s = make_settings(workers=1)
    st = make_store(s)
    add_jobs(st, 1)
    seen = []
    o = Orchestrator(s, st, SerpClient(s, http_get=fake_http([], seen), sleep=lambda *_: None), StatusBoard(st, 1))
    o.run_round()
    assert seen and all(p["tbs"] == "qdr:d" for p in seen)
    seen2 = []
    s2 = make_settings(workers=1, recent_hours=0)
    st2 = make_store(s2)
    add_jobs(st2, 1)
    Orchestrator(s2, st2, SerpClient(s2, http_get=fake_http([], seen2), sleep=lambda *_: None), StatusBoard(st2, 1)).run_round()
    assert seen2 and all("tbs" not in p for p in seen2)


def test_rounds_100_100_50_and_slots():
    s = make_settings()
    st = make_store(s)
    add_jobs(st, 250)
    calls = []
    o = build(s, st, calls)

    n1 = o.run_round()
    assert n1 == 100
    runs = {r["topic_key"]: r for r in st.runs.find({})}
    assert sorted(runs) == [f"topic{i:04d}" for i in range(100)]            # jobs 1..100 (oldest first)
    assert runs["topic0000"]["slot"] == 1 and runs["topic0099"]["slot"] == 100
    assert all(r["status"] == "done" for r in runs.values())

    n2 = o.run_round()
    assert n2 == 100
    assert st.runs.count_documents({"slot": 1, "round": 2}) == 1
    assert st.runs.find_one({"topic_key": "topic0100"})["slot"] == 1        # 101st job -> worker 1 again

    assert o.run_round() == 50
    assert o.run_round() == 0                                              # nothing left
    assert st.runs.count_documents({"status": "done"}) == 250
    assert len(calls) == 250                                               # 2 keywords/job -> ONE call per job


def test_posts_saved_exactly_like_chatbot_google_py():
    s = make_settings(workers=3)
    st = make_store(s)
    add_jobs(st, 3)
    o = build(s, st, [])
    o.run_round()
    docs = list(st.posts.find({}, {"_id": 0}))
    assert len(docs) == 6                                                  # 3 jobs x 2 keywords
    d = docs[0]
    # the SAME 10 fields flintel-chatbot/google.py writes — nothing more, nothing less
    assert set(d) == {"post_url", "discovered_at", "fetched_at", "fuzzy_keywords", "fuzzy_matched",
                      "google_rank", "next_retry_at", "reddit_fetched", "search_keyword", "subreddit"}
    assert d["reddit_fetched"] is False and d["fetched_at"] is None and d["next_retry_at"] is None
    assert d["fuzzy_matched"] is True
    assert d["post_url"].startswith("https://www.reddit.com/r/test/comments/") and d["subreddit"] == "test"
    # search_keyword = job's FIRST keyword, fuzzy_keywords = ALL of the job's keywords
    one = st.posts.find_one({"search_keyword": "kw0 alpha"}, {"_id": 0})
    assert one["fuzzy_keywords"] == ["kw0 alpha", "kw0 beta"]
    # job requested again later (new requested_at) -> processed again, posts NOT duplicated
    for j in st.jobs.find({}):
        st.jobs.update_one({"_id": j["_id"]}, {"$set": {"requested_at": datetime.now(timezone.utc)}})
    o.run_round()
    assert st.posts.count_documents({}) == 6


def test_existing_post_never_reset_and_keywords_merged():
    s = make_settings(workers=1)
    st = make_store(s)
    url = "https://www.reddit.com/r/test/comments/abc123/hello/"
    st.posts.insert_one({"post_url": url, "reddit_fetched": True, "fetched_at": "kept", "fuzzy_keywords": ["old"]})
    assert st.save_google_post(url, 1, "new kw", "test", ["new kw", "other"]) is False      # not a new insert
    got = st.posts.find_one({"post_url": url})
    assert got["reddit_fetched"] is True and got["fetched_at"] == "kept"                    # never reset to False
    assert set(got["fuzzy_keywords"]) == {"old", "new kw", "other"}                         # keywords merged


def test_platform_and_keywords_filter():
    s = make_settings(workers=10)
    st = make_store(s)
    add_jobs(st, 2, platform="all")
    add_jobs(st, 2, platform="x_twitter", start=10)
    st.jobs.insert_one({"topic_key": "nokw", "keywords": [], "targeting_platform": "reddit",
                        "requested_at": datetime.now(timezone.utc)})
    o = build(s, st, [])
    assert o.run_round() == 2


def test_old_jobs_ignored_and_claim_is_exclusive():
    s = make_settings(workers=5, job_max_age_hours=24)
    st = make_store(s)
    st.jobs.insert_one({"topic_key": "old", "keywords": ["a"], "targeting_platform": "all",
                        "requested_at": datetime.now(timezone.utc) - timedelta(hours=60)})
    add_jobs(st, 1)
    o = build(s, st, [])
    assert o.run_round() == 1
    job = st.jobs.find_one({"topic_key": "topic0000"})
    assert st.claim(job, 9, 9) is False                                    # already done -> cannot be claimed again


def test_failed_job_retried_then_stops_at_max_attempts():
    s = make_settings(workers=2, max_attempts=2, serp_max_retries=0)
    st = make_store(s)
    add_jobs(st, 1)
    bad = SerpClient(s, http_get=lambda *a, **k: FakeResp({"message": "quota"}), sleep=lambda *_: None)
    o = Orchestrator(s, st, bad, StatusBoard(st, 2))
    assert o.run_round() == 1
    assert st.runs.find_one({})["status"] == "failed"
    assert o.run_round() == 1                                              # attempt 2
    assert st.runs.find_one({})["attempts"] == 2
    assert o.run_round() == 0                                              # attempts exhausted
    assert st.posts.count_documents({}) == 0


def test_stale_claim_is_recovered():
    s = make_settings(workers=2, stale_seconds=60)
    st = make_store(s)
    add_jobs(st, 1)
    job = st.jobs.find_one({})
    assert st.claim(job, 1, 1)
    assert st.claim(job, 2, 2) is False                                    # fresh claim, someone is working
    st.runs.update_one({}, {"$set": {"heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=300)}})
    assert st.claim(job, 2, 2) is True                                     # worker died -> taken over


def test_status_board_shows_every_worker():
    s = make_settings(workers=4)
    st = make_store(s)
    add_jobs(st, 2)
    o = build(s, st, [])
    o.run_round()
    snap = o.board.snapshot()
    assert len(snap["workers"]) == 4
    states = [w["state"] for w in snap["workers"]]
    assert states == ["done", "done", "idle", "idle"]
    assert snap["totals"]["jobs_done"] == 2 and snap["totals"]["new_posts"] > 0
    assert st.status.count_documents({}) == 4                              # persisted too
    assert "W001" in o.board.render_text()

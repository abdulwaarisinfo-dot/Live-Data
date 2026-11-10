"""One worker = one job. Reads the job's keywords, calls the SERP API, stores reddit URLs with reddit_fetched=False."""
import logging

from .utils import clean_keywords, extract_subreddit

log = logging.getLogger("flintel.worker")


def process_job(slot: int, job: dict, store, serp, board, settings) -> dict:
    wid = board.worker_id(slot)
    tag = f"[{wid}] "
    key_topic = job.get("topic_key")
    keywords = clean_keywords(job.get("keywords"), settings.max_keywords_per_job)
    stats = {"calls": 0, "results": 0, "new_posts": 0, "failed_calls": 0}
    from .store import job_key
    jk = job_key(job)

    board.update(slot, state="searching", topic_key=key_topic, keywords=keywords, calls=0, results=0,
                 new_posts=0, error=None)
    log.info(f"{tag}START job={key_topic} | {len(keywords)} keyword(s): {keywords[:5]}")
    if not keywords:
        store.finish(jk, True, stats)
        board.update(slot, state="done")
        return stats

    chunks = [keywords[i:i + settings.keywords_per_call] for i in range(0, len(keywords), settings.keywords_per_call)]
    for n, chunk in enumerate(chunks, start=1):
        board.update(slot, state="searching", call=f"{n}/{len(chunks)}")
        res = serp.search(chunk, tag=tag)
        stats["calls"] += 1
        if res is None:
            stats["failed_calls"] += 1
            board.update(slot, calls=stats["calls"], error="serp call failed")
            continue
        board.update(slot, state="saving")
        cap = settings.results_per_keyword * len(chunk)
        for r in res[:cap]:
            stats["results"] += 1
            # same as flintel-chatbot/google.py: search_keyword = job's first keyword, fuzzy_keywords = ALL job keywords
            if store.save_google_post(r["url"], r["rank"], keywords[0], extract_subreddit(r["url"]), keywords):
                stats["new_posts"] += 1
        store.heartbeat(jk)
        board.update(slot, calls=stats["calls"], results=stats["results"], new_posts=stats["new_posts"])

    ok = stats["failed_calls"] == 0
    err = None if ok else f"{stats['failed_calls']}/{stats['calls']} SERP call(s) failed"
    store.finish(jk, ok, stats, err)
    board.update(slot, state="done" if ok else "failed", error=err)
    board.add_totals(jobs_done=1 if ok else 0, jobs_failed=0 if ok else 1, serp_calls=stats["calls"],
                     results=stats["results"], new_posts=stats["new_posts"])
    log.info(f"{tag}{'DONE' if ok else 'FAILED'} job={key_topic} | calls:{stats['calls']} results:{stats['results']} "
             f"new_posts:{stats['new_posts']} (reddit_fetched=False)")
    return stats

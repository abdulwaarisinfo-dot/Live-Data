"""One worker processes one UNIT (a job, or one part of a big job): its keywords -> SERP calls -> flintel_google_posts."""
import logging

from .utils import extract_subreddit

log = logging.getLogger("flintel.worker")


def process_unit(slot: int, unit, store, serp, board, settings) -> dict:
    wid = board.worker_id(slot)
    tag = f"[{wid}] "
    keywords = list(unit.keywords)
    stats = {"calls": 0, "results": 0, "new_posts": 0, "failed_calls": 0}
    part = f"{unit.part}/{unit.parts}"

    board.update(slot, state="searching", topic_key=unit.topic_key, part=part, keywords=keywords,
                 calls=0, results=0, new_posts=0, error=None)
    log.info(f"{tag}START job={unit.topic_key} part {part} | {len(keywords)} keyword(s): {keywords[:4]}")

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
            # like flintel-chatbot/google.py: search_keyword = first keyword, fuzzy_keywords = this unit's keywords
            if store.save_google_post(r["url"], r["rank"], keywords[0], extract_subreddit(r["url"]), keywords):
                stats["new_posts"] += 1
        store.heartbeat(unit.key)
        board.update(slot, calls=stats["calls"], results=stats["results"], new_posts=stats["new_posts"])

    ok = stats["failed_calls"] == 0
    err = None if ok else f"{stats['failed_calls']}/{stats['calls']} SERP call(s) failed"
    store.finish(unit.key, ok, stats, err)
    board.update(slot, state="done" if ok else "failed", error=err)
    board.add_totals(jobs_done=1 if ok else 0, jobs_failed=0 if ok else 1, serp_calls=stats["calls"],
                     results=stats["results"], new_posts=stats["new_posts"])
    board.bump_handled(slot)
    log.info(f"{tag}{'DONE' if ok else 'FAILED'} job={unit.topic_key} part {part} | calls:{stats['calls']} "
             f"results:{stats['results']} new_posts:{stats['new_posts']} (reddit_fetched=False)")
    return stats

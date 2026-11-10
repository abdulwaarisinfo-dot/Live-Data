"""RapidAPI google-search116 client. One call = one OR'd `site:reddit.com (...)` query + Google tbs time filter."""
import logging
import random
import threading
import time

import requests

from .utils import (RANK_FIELD_CANDIDATES, RESULT_LIST_KEY_CANDIDATES, dig_list, dig_value,
                    normalize_reddit_post_url)

log = logging.getLogger("flintel.serp")


class RateLimiter:
    """Global pacing: at most `rps` call STARTS per second across all 100 workers (0 = unlimited)."""

    def __init__(self, rps: float):
        self.interval = 1.0 / rps if rps and rps > 0 else 0.0
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self):
        if not self.interval:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        delay = start - now
        if delay > 0:
            time.sleep(delay)


class SerpClient:
    def __init__(self, settings, http_get=None, sleep=time.sleep):
        self.s = settings
        self._get = http_get or requests.get
        self._sleep = sleep
        self.limiter = RateLimiter(settings.rapidapi_max_rps)

    # ── query ────────────────────────────────────────────────────────────
    def build_query(self, keywords: list) -> str:
        terms = " OR ".join(f'"{k}"' for k in keywords)
        core = f"({terms})" if len(keywords) > 1 else terms
        return f"site:reddit.com {core}"

    def time_filter(self):
        """Google's own time filter, sent as the `tbs` request param — the SAME way flintel-chatbot/google.py
        does it for its last-24-hours searches (tbs=qdr:d)."""
        return self.s.recent_tbs

    # ── one API call ─────────────────────────────────────────────────────
    def search(self, keywords: list, tag: str = ""):
        """Returns a list of results (possibly empty = genuine zero results) or None on REAL failure."""
        if not self.s.rapidapi_key:
            log.error(f"{tag}[SERP] RAPIDAPI_KEY not set")
            return None
        if not keywords:
            return []
        query = self.build_query(keywords)
        limit = self.s.results_per_keyword * len(keywords)
        headers = {"x-rapidapi-key": self.s.rapidapi_key, "x-rapidapi-host": self.s.rapidapi_host}
        params = {"query": query, "limit": str(limit), "page": "0"}
        tbs = self.time_filter()
        if tbs:
            params["tbs"] = tbs

        data = None
        for attempt in range(self.s.serp_max_retries + 1):
            self.limiter.wait()
            try:
                r = self._get(f"https://{self.s.rapidapi_host}/", headers=headers, params=params,
                              timeout=self.s.serp_timeout_seconds)
            except Exception as exc:
                log.warning(f"{tag}[SERP] request error ({exc}) attempt {attempt + 1}")
                r = None
            if r is not None and r.status_code == 200:
                try:
                    data = r.json()
                    break
                except ValueError:
                    log.warning(f"{tag}[SERP] non-JSON 200 response")
            elif r is not None and r.status_code in (429, 500, 502, 503, 504):
                try:
                    wait = float(r.headers.get("Retry-After", 0))
                except (TypeError, ValueError):
                    wait = 0.0
                wait = max(wait, 2 ** attempt) + random.random()
                log.warning(f"{tag}[SERP] HTTP {r.status_code} — retry in {wait:.1f}s")
                if attempt < self.s.serp_max_retries:
                    self._sleep(wait)
                continue
            elif r is not None:
                log.error(f"{tag}[SERP] HTTP {r.status_code} — not retrying")
                return None
            elif attempt < self.s.serp_max_retries:
                self._sleep(2 ** attempt)
        if data is None:
            return None

        raw_items = dig_list(data, RESULT_LIST_KEY_CANDIDATES)
        if (isinstance(data, dict) and ("message" in data or "error" in data) and not raw_items
                and not any(isinstance(data.get(k), list) for k in RESULT_LIST_KEY_CANDIDATES)):
            log.error(f"{tag}[SERP] API error payload: {str(data.get('message') or data.get('error'))[:200]!r}")
            return None   # quota/subscription error is a FAILURE, not "zero results"

        results, seen = [], set()
        for pos, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            url = normalize_reddit_post_url(item.get("url", "") or item.get("link", ""))
            if url is None or url in seen:
                continue
            title = item.get("title", "") or ""
            rank = dig_value(item, RANK_FIELD_CANDIDATES)
            seen.add(url)
            results.append({
                "url": url,
                "rank": rank if rank is not None else pos,
                "title": title,
            })
        log.info(f"{tag}[SERP] {len(keywords)} kw | raw:{len(raw_items)} | reddit posts:{len(results)} | tbs={tbs or '-'} | q={query[:110]!r}")
        return results[:limit]

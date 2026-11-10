"""Helpers ported 1:1 from flintel-commerce-agent/index.py so flintel_google_posts rows come out identical."""
import re
from urllib.parse import urlparse

_REDDIT_ALLOWED_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "np.reddit.com"}
_REDDIT_POST_PATH_RE = re.compile(r"^/r/([A-Za-z0-9_]+)/comments/([A-Za-z0-9]+)(?:/([^/]+))?(?:/.*)?$")

RANK_FIELD_CANDIDATES = ["rank_absolute", "rank", "position", "google_rank", "serp_position", "rank_group", "index", "pos"]
RESULT_LIST_KEY_CANDIDATES = ["results", "organic_results", "organic", "items", "data", "response", "hits"]

_FUZZY_STOPWORDS = {"a", "an", "the", "to", "for", "of", "in", "on", "my", "our", "is", "are", "and", "or",
                    "with", "from", "at", "by", "your", "their"}


def normalize_reddit_post_url(url):
    """Canonical https://www.reddit.com/r/<sub>/comments/<id>/<slug>/ or None if not a Reddit POST url."""
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    if (parsed.hostname or "").lower() not in _REDDIT_ALLOWED_HOSTS:
        return None
    m = _REDDIT_POST_PATH_RE.match(parsed.path or "")
    if not m:
        return None
    sub, post_id, slug = m.groups()
    base = f"https://www.reddit.com/r/{sub}/comments/{post_id}/"
    return f"{base}{slug}/" if slug else base


def extract_subreddit(post_url: str) -> str:
    m = re.search(r"reddit\.com/r/([^/]+)/", post_url or "")
    return m.group(1) if m else ""


def generate_fuzzy_keywords(search_keyword: str) -> list:
    kw = (search_keyword or "").lower().strip()
    words = re.findall(r"[a-z0-9']+", kw)
    variants = set()
    if kw:
        variants.add(kw)
    sig = [w for w in words if w not in _FUZZY_STOPWORDS and len(w) > 2]
    if sig:
        variants.add(" ".join(sig))
    for w in sig:
        variants.add(w)
    for i in range(len(sig) - 1):
        variants.add(f"{sig[i]} {sig[i + 1]}")
    plurals = set()
    for v in variants:
        plurals.add(v[:-1] if (v.endswith("s") and len(v) > 3) else v + "s")
    variants |= plurals
    variants.discard("")
    return sorted(variants)


def find_best_matching_keyword(text: str, keywords: list):
    """Which keyword of a (batched/OR'd) call does this result belong to? None = cannot tell -> skip."""
    t = (text or "").lower()
    for kw in keywords:
        if kw and kw.lower() in t:
            return kw
    for kw in keywords:
        for fkw in generate_fuzzy_keywords(kw):
            if fkw and " " in fkw and fkw in t:
                return kw
    return None


def dig_value(obj, candidate_keys):
    if obj is None:
        return None

    def _try(d):
        if not isinstance(d, dict):
            return None
        for k in candidate_keys:
            if k in d and d[k] is not None:
                return d[k]
        return None

    if isinstance(obj, dict):
        v = _try(obj)
        if v is not None:
            return v
        for val in obj.values():
            if isinstance(val, dict):
                v = _try(val)
                if v is not None:
                    return v
            elif isinstance(val, list) and val and isinstance(val[0], dict):
                v = _try(val[0])
                if v is not None:
                    return v
    elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return _try(obj[0])
    return None


def dig_list(obj, candidate_list_keys):
    if isinstance(obj, list):
        return obj
    if not isinstance(obj, dict):
        return []
    for key in candidate_list_keys:
        val = obj.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            for inner in candidate_list_keys:
                iv = val.get(inner)
                if isinstance(iv, list):
                    return iv
    return []


def clean_keywords(raw, cap: int) -> list:
    """Job `keywords` -> unique, trimmed, non-empty strings (order kept), capped."""
    if isinstance(raw, str):
        raw = [raw]
    out, seen = [], set()
    for k in raw or []:
        if not isinstance(k, str):
            continue
        k = " ".join(k.split()).replace('"', "")
        if k and k.lower() not in seen:
            seen.add(k.lower())
            out.append(k)
        if len(out) >= cap:
            break
    return out

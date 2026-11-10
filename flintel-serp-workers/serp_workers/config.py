"""All settings come from environment variables (see .env.example)."""
import os
from dataclasses import dataclass


def _s(name, default=""):
    return (os.getenv(name, default) or default).strip()


def _i(name, default):
    try:
        return int(_s(name, str(default)))
    except ValueError:
        return default


def _f(name, default):
    try:
        return float(_s(name, str(default)))
    except ValueError:
        return default


def _b(name, default):
    return _s(name, "true" if default else "false").lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    mongodb_jobs_uri: str
    mongodb_jobs_db: str
    mongodb_posts_uri: str
    mongodb_posts_db: str
    rapidapi_key: str
    rapidapi_host: str = "google-search116.p.rapidapi.com"
    rapidapi_max_rps: float = 10.0
    workers: int = 100
    poll_seconds: float = 10.0
    recent_hours: float = 24.0
    recent_tbs_override: str = ""
    job_max_age_hours: float = 24.0
    job_platforms: tuple = ("all", "reddit")
    keywords_per_call: int = 2
    max_keywords_per_job: int = 20
    results_per_keyword: int = 10
    max_attempts: int = 3
    stale_seconds: int = 600
    serp_timeout_seconds: float = 30.0
    serp_max_retries: int = 3
    status_server_enabled: bool = True
    port: int = 8080
    jobs_collection: str = "flintel_search_jobs"
    posts_collection: str = "flintel_google_posts"
    runs_collection: str = "flintel_serp_job_runs"
    status_collection: str = "flintel_serp_worker_status"

    @property
    def recent_tbs(self) -> str:
        """Google time filter for RECENT_HOURS: <=1h -> qdr:h, <=24h -> qdr:d, <=7d -> qdr:w, else none."""
        if self.recent_tbs_override:
            return self.recent_tbs_override
        h = self.recent_hours
        if h <= 0:
            return ""
        if h <= 1:
            return "qdr:h"
        if h <= 24:
            return "qdr:d"
        if h <= 168:
            return "qdr:w"
        return ""


def load_settings() -> Settings:
    jobs_uri = _s("MONGODB3") or _s("MONGODB_URI")
    jobs_db = _s("MONGODB_DB", "flintel")
    posts_uri = _s("MONGODB2_URI") or jobs_uri
    posts_db = _s("MONGODB2_DB") or jobs_db
    platforms = tuple(p.strip().lower() for p in _s("JOB_PLATFORMS", "all,reddit").split(",") if p.strip())
    return Settings(
        mongodb_jobs_uri=jobs_uri,
        mongodb_jobs_db=jobs_db,
        mongodb_posts_uri=posts_uri,
        mongodb_posts_db=posts_db,
        rapidapi_key=_s("RAPIDAPI_KEY"),
        rapidapi_max_rps=_f("RAPIDAPI_MAX_RPS", 10),
        workers=max(1, _i("WORKERS", 100)),
        poll_seconds=_f("POLL_SECONDS", 10),
        recent_hours=_f("RECENT_HOURS", 24),
        recent_tbs_override=_s("RECENT_TBS"),
        job_max_age_hours=_f("JOB_MAX_AGE_HOURS", 24),
        job_platforms=platforms or ("all", "reddit"),
        keywords_per_call=max(1, _i("KEYWORDS_PER_CALL", 2)),
        max_keywords_per_job=max(1, _i("MAX_KEYWORDS_PER_JOB", 20)),
        results_per_keyword=max(1, _i("RESULTS_PER_KEYWORD", 10)),
        max_attempts=max(1, _i("MAX_ATTEMPTS", 3)),
        stale_seconds=max(30, _i("STALE_SECONDS", 600)),
        serp_timeout_seconds=_f("SERP_TIMEOUT_SECONDS", 30),
        serp_max_retries=max(0, _i("SERP_MAX_RETRIES", 3)),
        status_server_enabled=_b("STATUS_SERVER_ENABLED", True),
        port=_i("PORT", 8080),
        jobs_collection=_s("JOBS_COLLECTION", "flintel_search_jobs"),
        posts_collection=_s("POSTS_COLLECTION", "flintel_google_posts"),
        runs_collection=_s("RUNS_COLLECTION", "flintel_serp_job_runs"),
        status_collection=_s("STATUS_COLLECTION", "flintel_serp_worker_status"),
    )

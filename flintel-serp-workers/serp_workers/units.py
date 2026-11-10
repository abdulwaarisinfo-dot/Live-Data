"""A job is split into UNITS (parts) so a big job (e.g. 200 keywords) is shared by many workers instead of
hogging one. Small jobs (<= KEYWORDS_PER_PART keywords) are exactly ONE unit = one worker."""
from dataclasses import dataclass

from .store import job_key
from .utils import clean_keywords


@dataclass(frozen=True)
class Unit:
    job: dict
    key: str            # unique claim key (job_key for a 1-part job, job_key#pN for a multi-part job)
    part: int
    parts: int
    keywords: tuple

    @property
    def topic_key(self):
        return self.job.get("topic_key")


def build_units(job: dict, settings) -> list:
    kws = clean_keywords(job.get("keywords"), settings.max_keywords_per_job)
    if not kws:
        return []
    size = settings.keywords_per_part
    chunks = [kws[i:i + size] for i in range(0, len(kws), size)]
    base = job_key(job)
    return [Unit(job, base if len(chunks) == 1 else f"{base}#p{n}", n, len(chunks), tuple(c))
            for n, c in enumerate(chunks, start=1)]

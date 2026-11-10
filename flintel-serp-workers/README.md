# flintel-serp-workers

Alag background service. `flintel_search_jobs` se keywords padhta hai, **100 workers (employees)** parallel mein
Google SERP (RapidAPI `google-search116`) se **last 24 ghante ke Reddit posts** nikalta hai, aur unke URLs
`flintel_google_posts` mein **`reddit_fetched=False`** ke saath save karta hai — **bilkul `flintel-chatbot/google.py` wala document**
(`post_url, discovered_at, fetched_at=None, fuzzy_matched=True, google_rank, next_retry_at=None, reddit_fetched=False, search_keyword, subreddit, fuzzy_keywords`).
`$setOnInsert` + `$addToSet`: pehle se maujood post kabhi reset nahi hota (reddit_fetched=True hi rehta hai), sirf keywords merge hote hain.
Baaki kaam wahi purana fetch loop karta hai.

## Kaise kaam karta hai

```
flintel_search_jobs (sirf PADHA jata hai, kabhi modify nahi hota)
        │  oldest first
        ▼
 Round 1:  job 1..100   →  W001 … W100   (1 job = 1 worker, parallel)
           ── sab 100 poore hone ka intezar ──
 Round 2:  job 101..200 →  W001 … W100
 Round 3:  job 201..300 →  …   (jobs khatam → POLL_SECONDS ka wait, phir naye jobs)
        │
        ▼  har worker:  job ke keywords → (KEYWORDS_PER_CALL=2 ke chunk) → site:reddit.com ("kw1" OR "kw2")  + tbs=qdr:d (last 24h)
 flintel_google_posts  (flintel-chatbot/google.py wala hi document, reddit_fetched=False)
```

* **Ek job = ek worker.** Job ke keywords `KEYWORDS_PER_CALL=2` ke chunks mein, har chunk ek OR-query. Google ~32 words tak hi padhta hai,
  is liye 2 (chatbot aur commerce-agent bhi 2 use karte hain). 2 se zyada keywords wali job ke liye usi worker ki 2+ calls hoti hain.
* **Live / last 24h:** har request mein `tbs=qdr:d` (Google ka "past 24 hours" filter) jata hai (`RECENT_HOURS=24`) — wahi tareeqa jo
  `flintel-chatbot/google.py` apni last-24-hours searches mein use karta hai.
* **Sirf naye jobs:** `JOB_MAX_AGE_HOURS=24` — pichle 24 ghante mein `requested_at` wale jobs. Sirf `targeting_platform` = `all`/`reddit` wale jobs.
* **Har job ek hi baar:** `flintel_serp_job_runs` mein `topic_key|requested_at` par unique claim lagta hai. Do instances chalayein
  to bhi ek job do baar nahi chalega. Job dobara enqueue ho (naya `requested_at`) to dobara chalega, lekin posts duplicate nahi hote.
* **Fail/crash:** fail job `MAX_ATTEMPTS` (3) tak dobara try hota hai; worker mar jaye to `STALE_SECONDS` baad koi aur utha leta hai.
  Ek job ka crash baaki 99 ko nahi rokta.

## Sab 100 employees ko pata hota hai kaun kya kar raha hai

* Har log line par `[W017]` jaisa tag. Har round ke shuru mein **roster** log hota hai: `[W017] → job=topic… | keywords=[…]`.
* **Status page:** `http://<host>:PORT/` (sab workers ki table), `/status` (JSON), `/health`.
* **MongoDB:** `flintel_serp_worker_status` (har worker ka state: idle / searching / saving / done / failed, kaun si job, calls, results, new_posts)
  aur `flintel_serp_job_runs` (har job ka hisaab).

## Chalana

```bash
pip install -r requirements.txt
cp .env.example .env      # keys bharein
python main.py            # hamesha chalta rahega
python main.py --once     # sirf ek round (test ke liye)
```
Render par: **Background Worker**, Start Command `python main.py` (Web Service rakhein to `PORT` par status page chalta hai).

## Zaroori env

| Variable | Matlab |
|---|---|
| `MONGODB3`, `MONGODB_DB` | jahan `flintel_search_jobs` hai (chatbot wala) |
| `MONGODB2_URI`, `MONGODB2_DB` | jahan `flintel_google_posts` hai (commerce-agent wala); khali = upar wala |
| `RAPIDAPI_KEY` | RapidAPI key |
| `WORKERS` | 100 |
| `RAPIDAPI_MAX_RPS` | ek second mein max kitni calls **shuru** hon (default 10). 100 workers ek saath rehte hain, bas calls is raftaar se nikalti hain taake 429 na aaye. Apne plan ke hisaab se barhayein (0 = bina limit) |
| `RECENT_HOURS` | 24 |

Baaki sab `.env.example` mein.

## Test

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q
```

## Dhyan dein

* Ye service `flintel_search_jobs` ka `status` **nahi badalti** (sirf padhti hai), kyunke kaun si status ko chatbot/doosra collector kaise use karta hai, ye is repo se verify nahi ho saka.
* Jobs ki `keywords` field list of strings honi chahiye (jaisa chatbot ka `enqueue_search_job` likhta hai).
* Live RapidAPI par `tbs` ka asar aap pehli run mein check kar lein (`python main.py --once`, logs mein `[SERP] ... tbs=qdr:d | q='site:reddit.com (...)'` dikhega). Chatbot ka `google.py` bhi isi param par chalta hai.

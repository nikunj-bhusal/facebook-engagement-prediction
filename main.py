"""
Nepal Election Candidate News Scraper
======================================
- Select candidates by number (1,3,5 / 2-10 / all)
- All sources fire in parallel per candidate (Round 1)
- Article texts fetched in parallel (Round 2)
- TF-IDF-style relevance scoring with name + constituency boosting
- Removed dead sources (Setopati 404, EC timeout)
- Added: Ratopati, Nagarik News, Nepali OnlineKhabar
- 4 DDG query variants (en full, en bare, ne full, ne bare)

Install:
    pip install requests beautifulsoup4 pandas lxml

Run:
    python election_scraper.py
"""

import re
import time
import random
import urllib.parse
from math import log
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE = "candidates_list.csv"
OUTPUT_DIR = "data"
MAX_RESULTS = 8  # top results saved per candidate
SOURCE_WORKERS = 11  # parallel source threads (one per source)
ARTICLE_WORKERS = 8  # parallel article fetch threads
REQUEST_TIMEOUT = 12  # seconds per HTTP request
INTER_CANDIDATE_DELAY = (1, 3)  # sleep between candidates (min, max seconds)
# ─────────────────────────────────────────────────────────────────────────────

HEADERS_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ne-NP,ne;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://duckduckgo.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.3 Safari/605.1.15",
        "Accept-Language": "en-GB,en;q=0.9,ne;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.bing.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0",
        "Accept-Language": "ne,en-US;q=0.7,en;q=0.3",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    },
]


def _new_session():
    s = requests.Session()
    s.headers.update(random.choice(HEADERS_POOL))
    return s


def safe_get(url, session=None, timeout=REQUEST_TIMEOUT):
    sess = session or _new_session()
    try:
        resp = sess.get(url, headers=random.choice(HEADERS_POOL), timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        msg = str(e)
        # Suppress expected misses to keep output clean
        if (
            "404" not in msg
            and "timed out" not in msg.lower()
            and "ConnectTimeout" not in msg
        ):
            print(f"      ⚠ {url[:65]} — {msg[:70]}")
        return None


# ── Relevance scoring ─────────────────────────────────────────────────────────
ELECTION_TERMS = {
    "निर्वाचन": 3,
    "उम्मेदवार": 3,
    "विजयी": 3,
    "नतिजा": 3,
    "election": 3,
    "candidate": 3,
    "winner": 3,
    "result": 3,
    "मत": 2,
    "मतदान": 2,
    "vote": 2,
    "polling": 2,
    "constituency": 2,
    "क्षेत्र": 2,
    "parliament": 2,
    "संसद": 2,
    "campaign": 2,
    "चुनाव": 2,
    "प्रतिनिधि": 2,
    "party": 1,
    "दल": 1,
    "seat": 1,
    "MP": 1,
    "defeated": 1,
    "सभा": 1,
}


def relevance_score(item, name_en, name_ne, const):
    text = " ".join(
        [
            item.get("title", ""),
            item.get("snippet", ""),
            item.get("content", ""),
        ]
    ).lower()

    score = 0.0
    for term, weight in ELECTION_TERMS.items():
        if term.lower() in text:
            score += weight

    # Name frequency boost (log-dampened)
    for name, boost in [(name_en.lower(), 4), (name_ne, 4)]:
        hits = text.count(name.lower())
        if hits:
            score += boost * (1 + log(hits))

    # Constituency number present
    if str(const) in text:
        score += 2

    # Penalise if name absent from title (likely off-topic)
    title = item.get("title", "").lower()
    if name_en.lower() not in title and name_ne.lower() not in title:
        score *= 0.5

    return round(score, 2)


# ── Sources ───────────────────────────────────────────────────────────────────


def ddg_search(query, source_label, max_results=6):
    """DuckDuckGo HTML — scraper-friendly, no bot detection."""
    sess = _new_session()
    encoded = urllib.parse.quote_plus(query)
    resp = safe_get(f"https://html.duckduckgo.com/html/?q={encoded}", sess)
    if not resp:
        return []

    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for r in soup.select(".result__body")[:max_results]:
        title_el = r.select_one(".result__title a")
        snippet_el = r.select_one(".result__snippet")
        if not title_el:
            continue
        href = title_el.get("href", "")
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            href = urllib.parse.unquote(m.group(1)) if m else href
        title = title_el.get_text(strip=True)
        if title and href:
            results.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                    "source": source_label,
                }
            )
    return results


def bing_news_rss(query, max_results=6):
    """Bing News RSS — reliable XML, no JS."""
    sess = _new_session()
    encoded = urllib.parse.quote_plus(query)
    resp = safe_get(f"https://www.bing.com/news/search?q={encoded}&format=rss", sess)
    if not resp:
        return []

    soup, results = BeautifulSoup(resp.text, "xml"), []
    for item in soup.find_all("item")[:max_results]:
        title_el = item.find("title")
        link_el = item.find("link") or item.find("guid")
        desc_el = item.find("description")
        title = title_el.get_text(strip=True) if title_el else ""
        link = link_el.get_text(strip=True) if link_el else ""
        snippet = (
            BeautifulSoup(desc_el.get_text(), "lxml").get_text(strip=True)[:400]
            if desc_el
            else ""
        )
        if title and link:
            results.append(
                {"title": title, "url": link, "snippet": snippet, "source": "Bing News"}
            )
    return results


def nepali_times(name_en, district_en):
    sess = _new_session()
    q = f"{name_en} {district_en} election"
    resp = safe_get(
        f"https://www.nepalitimes.com/?s={urllib.parse.quote_plus(q)}", sess
    )
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for a in soup.select("article h2 a, article h3 a, .entry-title a")[:4]:
        if a.get_text(strip=True) and a.get("href"):
            results.append(
                {
                    "title": a.get_text(strip=True),
                    "url": a["href"],
                    "snippet": "",
                    "source": "Nepali Times",
                }
            )
    return results


def kathmandu_post(name_en, district_en):
    sess = _new_session()
    q = f"{name_en} {district_en} election"
    resp = safe_get(
        f"https://kathmandupost.com/search?query={urllib.parse.quote_plus(q)}", sess
    )
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for article in soup.select("article, .article-item")[:4]:
        a = article.select_one("h2 a, h3 a, .title a")
        if not a:
            continue
        href = a.get("href", "")
        if not href.startswith("http"):
            href = "https://kathmandupost.com" + href
        results.append(
            {
                "title": a.get_text(strip=True),
                "url": href,
                "snippet": "",
                "source": "Kathmandu Post",
            }
        )
    return results


def onlinekhabar_en(name_en):
    sess = _new_session()
    resp = safe_get(
        f"https://english.onlinekhabar.com/?s={urllib.parse.quote_plus(name_en)}", sess
    )
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for a in soup.select(".ok-news-post h2 a, article h2 a, .entry-title a")[:4]:
        if a.get_text(strip=True):
            results.append(
                {
                    "title": a.get_text(strip=True),
                    "url": a.get("href", ""),
                    "snippet": "",
                    "source": "OnlineKhabar EN",
                }
            )
    return results


def onlinekhabar_ne(name_ne):
    """Nepali OnlineKhabar — far better coverage of local candidates."""
    sess = _new_session()
    resp = safe_get(
        f"https://www.onlinekhabar.com/?s={urllib.parse.quote_plus(name_ne)}", sess
    )
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for a in soup.select(".ok-news-post h2 a, article h2 a, .entry-title a")[:5]:
        if a.get_text(strip=True):
            results.append(
                {
                    "title": a.get_text(strip=True),
                    "url": a.get("href", ""),
                    "snippet": "",
                    "source": "OnlineKhabar NE",
                }
            )
    return results


def ratopati(name_ne, name_en):
    """Ratopati — strong Nepali election coverage."""
    sess = _new_session()
    for q in [name_ne, name_en]:
        resp = safe_get(
            f"https://www.ratopati.com/search?q={urllib.parse.quote_plus(q)}", sess
        )
        if not resp:
            continue
        soup, results = BeautifulSoup(resp.text, "lxml"), []
        for a in soup.select("article h2 a, .news-item a, a[href*='/story/']")[:4]:
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.ratopati.com" + href
            if a.get_text(strip=True):
                results.append(
                    {
                        "title": a.get_text(strip=True),
                        "url": href,
                        "snippet": "",
                        "source": "Ratopati",
                    }
                )
        if results:
            return results
    return []


def nagarik_news(name_ne):
    """Nagarik News — strong Nepali political coverage."""
    sess = _new_session()
    resp = safe_get(
        f"https://nagariknews.nagariknetwork.com/search?query={urllib.parse.quote_plus(name_ne)}",
        sess,
    )
    if not resp:
        return []
    soup, results = BeautifulSoup(resp.text, "lxml"), []
    for article in soup.select("article, .news-item")[:4]:
        a = article.select_one("h2 a, h3 a, .title a")
        if not a:
            continue
        href = a.get("href", "")
        if not href.startswith("http"):
            href = "https://nagariknews.nagariknetwork.com" + href
        results.append(
            {
                "title": a.get_text(strip=True),
                "url": href,
                "snippet": "",
                "source": "Nagarik News",
            }
        )
    return results


# ── Article full-text ─────────────────────────────────────────────────────────
CONTENT_SELECTORS = [
    "article",
    ".article-content",
    ".entry-content",
    ".post-content",
    ".story-content",
    ".news-content",
    ".content-body",
    "main",
]


def fetch_article(item):
    url = item.get("url", "")
    if not url or item.get("content"):
        return item
    sess = _new_session()
    resp = safe_get(url, sess, timeout=15)
    if not resp:
        item["content"] = ""
        return item

    soup = BeautifulSoup(resp.text, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()

    for sel in CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(" ", strip=True)
            if len(text) > 150:
                item["content"] = text[:5000]
                return item

    paras = [
        p.get_text(strip=True)
        for p in soup.find_all("p")
        if len(p.get_text(strip=True)) > 40
    ]
    item["content"] = " ".join(paras)[:5000]
    return item


# ── Per-candidate parallel search ────────────────────────────────────────────
def search_candidate(row):
    name_en = row["EnglishCandidateName"]
    name_ne = row["CandidateName"]
    dist_en = row["EnglishDistrictName"]
    dist_ne = row["DistrictName"]
    const = str(row["ConstName"])

    q_en_full = f'"{name_en}" {dist_en} constituency {const} Nepal election'
    q_en_bare = f"{name_en} Nepal election candidate 2079"
    q_ne_full = f'"{name_ne}" {dist_ne} निर्वाचन क्षेत्र {const}'
    q_ne_bare = f"{name_ne} उम्मेदवार निर्वाचन"
    q_news = f"{name_en} {dist_en} Nepal vote result"

    # (display_label, callable, *args)
    tasks = [
        ("DDG en-full", ddg_search, q_en_full, "DDG (en)", 5),
        ("DDG en-bare", ddg_search, q_en_bare, "DDG (en2)", 5),
        ("DDG ne-full", ddg_search, q_ne_full, "DDG (ne)", 6),
        ("DDG ne-bare", ddg_search, q_ne_bare, "DDG (ne2)", 5),
        ("Bing News", bing_news_rss, q_news, 6),
        ("Nepali Times", nepali_times, name_en, dist_en),
        ("Kathmandu Post", kathmandu_post, name_en, dist_en),
        ("OKhabar EN", onlinekhabar_en, name_en),
        ("OKhabar NE", onlinekhabar_ne, name_ne),
        ("Ratopati", ratopati, name_ne, name_en),
        ("Nagarik News", nagarik_news, name_ne),
    ]

    # ── Round 1: all sources simultaneously ──────────────────────────────────
    all_raw = []
    with ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as pool:
        futures = {pool.submit(fn, *args): label for label, fn, *args in tasks}
        for future in as_completed(futures):
            label = futures[future]
            try:
                res = future.result()
                icon = "✓" if res else "·"
                print(f"      {icon} {label:<16} → {len(res)}")
                all_raw.extend(res)
            except Exception as e:
                print(f"      ✗ {label:<16} → {e}")

    # ── Deduplicate (normalise URLs) ──────────────────────────────────────────
    seen, unique = set(), []
    for item in all_raw:
        key = re.sub(r"[?#].*$", "", item.get("url", "")).rstrip("/")
        if key and key not in seen:
            seen.add(key)
            unique.append(item)

    # ── Round 2: fetch article texts simultaneously ───────────────────────────
    need_fetch = [i for i in unique if not i.get("snippet") and not i.get("content")]
    have_text = [i for i in unique if i.get("snippet") or i.get("content")]

    if need_fetch:
        print(f"   ⚡ Round 2 — fetching {len(need_fetch)} article(s)...")
        with ThreadPoolExecutor(max_workers=ARTICLE_WORKERS) as pool:
            fetched = list(pool.map(fetch_article, need_fetch))
    else:
        fetched = []

    all_items = have_text + fetched

    # ── Score, rank, trim ─────────────────────────────────────────────────────
    for item in all_items:
        item["_score"] = relevance_score(item, name_en, name_ne, const)
    all_items.sort(key=lambda x: x["_score"], reverse=True)

    rows = []
    for item in all_items[:MAX_RESULTS]:
        snippet = item.get("snippet", "")
        content = item.get("content", snippet)
        rows.append(
            {
                "Constituency": const,
                "DistrictEnglish": dist_en,
                "DistrictNepali": dist_ne,
                "CandidateEnglish": name_en,
                "CandidateNepali": name_ne,
                "Source": item.get("source", ""),
                "Title": item.get("title", ""),
                "URL": item.get("url", ""),
                "Snippet": snippet[:500],
                "Content": content[:5000],
                "RelevanceScore": item["_score"],
                "ScrapedAt": datetime.now().isoformat(),
            }
        )
    return rows


# ── File helpers ──────────────────────────────────────────────────────────────
def slugify(text):
    text = str(text).lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s\-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def candidate_path(dist_en, const, name_en):
    folder = slugify(f"{dist_en}_{const}")
    return Path(OUTPUT_DIR) / folder / (slugify(name_en) + ".csv")


def already_scraped(path):
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path)
        return not df.empty and df["Title"].fillna("").str.strip().ne("").any()
    except Exception:
        return False


# ── Constituency picker ───────────────────────────────────────────────────────
def pick_candidates(df):
    # Build unique constituency list
    combos = (
        df[["EnglishDistrictName", "DistrictName", "ConstName"]]
        .drop_duplicates()
        .sort_values(["EnglishDistrictName", "ConstName"])
        .reset_index(drop=True)
    )
    total = len(combos)

    print("\n┌────┬──────────────────────────────┬──────┬──────────────────────┐")
    print("│  # │ Constituency                 │ Cand │ Progress             │")
    print("├────┼──────────────────────────────┼──────┼──────────────────────┤")

    for i, row in combos.iterrows():
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        mask = (df["EnglishDistrictName"] == dist_en) & (df["ConstName"] == const)
        cands = df[mask]
        n = len(cands)
        done = sum(
            already_scraped(candidate_path(dist_en, const, r["EnglishCandidateName"]))
            for _, r in cands.iterrows()
        )
        pct = int(done / n * 10) if n else 0
        bar = "█" * pct + "░" * (10 - pct)
        label = f"{dist_en.title()} - {const}"
        print(f"│{i + 1:>3} │ {label:<28} │ {n:>4} │ [{bar}] {done}/{n}      │")

    print("└────┴──────────────────────────────┴──────┴──────────────────────┘")
    print(f"\n  {total} constituencies total")
    print("  Select:  1,3,5  |  2-10  |  1,5-8,12  |  all\n")

    while True:
        raw = input("  → ").strip().lower()
        if not raw:
            continue
        if raw == "all":
            selected = list(range(total))
            break
        try:
            selected = []
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    a, b = part.split("-", 1)
                    selected.extend(range(int(a) - 1, int(b)))
                else:
                    selected.append(int(part) - 1)
            oob = [i for i in selected if i < 0 or i >= total]
            if oob:
                print(f"  ⚠ Out of range: {[i + 1 for i in oob]}. Max is {total}.")
                continue
            selected = sorted(set(selected))
            break
        except ValueError:
            print("  ⚠ Invalid. Try:  1,3,5  or  2-10  or  all")

    chosen_combos = combos.iloc[selected]
    print(f"\n  ✔ {len(chosen_combos)} constituency/ies selected:")
    for _, row in chosen_combos.iterrows():
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        n = len(df[(df["EnglishDistrictName"] == dist_en) & (df["ConstName"] == const)])
        print(f"    • {dist_en.title()} - {const}  ({n} candidates)")
    print()

    # Return all candidates belonging to selected constituencies
    mask = df.apply(
        lambda r: any(
            r["EnglishDistrictName"] == sc["EnglishDistrictName"]
            and r["ConstName"] == sc["ConstName"]
            for _, sc in chosen_combos.iterrows()
        ),
        axis=1,
    )
    return df[mask].reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    df = pd.read_csv(INPUT_FILE)
    print(f"\n📋 {len(df)} candidates loaded from {INPUT_FILE}")

    subset = pick_candidates(df)
    total = len(subset)
    done = 0
    skipped = 0

    print(f"🚀 Scraping {total} candidate(s)...\n")

    for i, (_, row) in enumerate(subset.iterrows()):
        name_en = row["EnglishCandidateName"]
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        out_path = candidate_path(dist_en, const, name_en)

        if already_scraped(out_path):
            print(f"[{i + 1}/{total}] ⏭  {name_en} — already done")
            skipped += 1
            continue

        print(f"\n[{i + 1}/{total}] 🔎 {name_en} | {dist_en}-{const}")
        print(f"   📄 {out_path}")
        print(f"   ⚡ Round 1 — 11 sources in parallel...")

        try:
            rows = search_candidate(row)
        except Exception as e:
            print(f"   ❌ {e}")
            rows = []

        if not rows:
            print("   ⚠ No results.")
            rows = [
                {
                    "Constituency": const,
                    "DistrictEnglish": dist_en,
                    "DistrictNepali": row["DistrictName"],
                    "CandidateEnglish": name_en,
                    "CandidateNepali": row["CandidateName"],
                    "Source": "",
                    "Title": "",
                    "URL": "",
                    "Snippet": "",
                    "Content": "",
                    "RelevanceScore": 0,
                    "ScrapedAt": datetime.now().isoformat(),
                }
            ]
        else:
            with_text = sum(1 for r in rows if r.get("Content", "").strip())
            print(f"   ✅ {len(rows)} results  ({with_text} with full text)")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(out_path, index=False)
        done += 1

        time.sleep(random.uniform(*INTER_CANDIDATE_DELAY))

    print(f"\n{'─' * 60}")
    print(f"✅  scraped={done}  skipped={skipped}  total={total}")

    data_path = Path(OUTPUT_DIR)
    if data_path.exists():
        print("\n── Coverage ──")
        for folder in sorted(data_path.iterdir()):
            if folder.is_dir():
                files = list(folder.glob("*.csv"))
                with_data = sum(1 for f in files if already_scraped(f))
                pct = int(with_data / len(files) * 24) if files else 0
                bar = "█" * pct + "░" * (24 - pct)
                print(f"  {folder.name:<26} [{bar}] {with_data}/{len(files)}")


if __name__ == "__main__":
    main()

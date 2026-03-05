"""
Nepal Election Candidate News Scraper
======================================
Sources scraped:
  1. Nepal Election Commission (election.gov.np)
  2. Nepali Times (nepalitimes.com)
  3. The Kathmandu Post (kathmandupost.com)
  4. OnlineKhabar (onlinekhabar.com)
  5. Setopati (setopati.com)
  6. Ratopati (ratopati.com)
  7. Wikipedia (candidate background)
  8. DuckDuckGo HTML search (no API, no bot block)
  9. Bing News search (fallback)

Install deps:
    pip install requests beautifulsoup4 pandas lxml

Run:
    python election_scraper.py
"""

import os
import re
import json
import time
import random
import urllib.parse
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────────
INPUT_FILE = "candidates_list.csv"
OUTPUT_DIR = "data"  # root: data/district_const/candidate.csv
MAX_RESULTS_PER_CANDIDATE = 5
DELAY_MIN = 2.0  # seconds between requests
DELAY_MAX = 5.0
# ────────────────────────────────────────────────────────────────────────────

HEADERS_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,ne;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.bing.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept-Language": "ne,en-US;q=0.7,en;q=0.3",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,*/*;q=0.8",
        "Referer": "https://duckduckgo.com/",
    },
]

session = requests.Session()


def get_headers():
    return random.choice(HEADERS_POOL)


def safe_get(url, timeout=15, retries=2):
    """GET with retry + random delay."""
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=get_headers(), timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(random.uniform(2, 4))
            else:
                print(f"      ⚠ Failed {url[:80]} — {e}")
                return None


def sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ── Source 1: DuckDuckGo HTML (most reliable, no API key) ───────────────────
def duckduckgo_search(query, max_results=5):
    """
    Scrape DuckDuckGo HTML results. DDG does not block scrapers as aggressively as Google.
    Returns list of {title, url, snippet}.
    """
    encoded = urllib.parse.quote_plus(query)
    url = f"https://html.duckduckgo.com/html/?q={encoded}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    for result in soup.select(".result__body")[:max_results]:
        title_el = result.select_one(".result__title a")
        snippet_el = result.select_one(".result__snippet")

        title = title_el.get_text(strip=True) if title_el else ""
        snippet = snippet_el.get_text(strip=True) if snippet_el else ""
        href = title_el.get("href", "") if title_el else ""

        # DDG wraps URLs — extract real URL
        if "uddg=" in href:
            href = urllib.parse.unquote(re.search(r"uddg=([^&]+)", href).group(1))

        if title and href:
            results.append(
                {
                    "title": title,
                    "url": href,
                    "snippet": snippet,
                    "source": "DuckDuckGo",
                }
            )

    return results


# ── Source 2: Bing News ──────────────────────────────────────────────────────
def bing_news_search(query, max_results=5):
    """
    Scrape Bing News results (no API needed).
    Returns list of {title, url, snippet}.
    """
    encoded = urllib.parse.quote_plus(query)
    url = f"https://www.bing.com/news/search?q={encoded}&format=rss"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "xml")
    results = []

    for item in soup.find_all("item")[:max_results]:
        title = item.find("title").get_text(strip=True) if item.find("title") else ""
        link = item.find("link")
        url_val = (
            link.get_text(strip=True)
            if link
            else (item.find("guid").get_text(strip=True) if item.find("guid") else "")
        )
        desc = item.find("description")
        snippet = (
            BeautifulSoup(desc.get_text(), "lxml").get_text(strip=True)[:300]
            if desc
            else ""
        )

        if title and url_val:
            results.append(
                {
                    "title": title,
                    "url": url_val,
                    "snippet": snippet,
                    "source": "Bing News",
                }
            )

    return results


# ── Source 3: Nepali Times search ────────────────────────────────────────────
def nepali_times_search(name_en, district_en):
    query = f"{name_en} {district_en} election"
    url = f"https://www.nepalitimes.com/?s={urllib.parse.quote_plus(query)}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    for article in soup.select("article")[:3]:
        title_el = article.select_one("h2 a, h3 a")
        title = title_el.get_text(strip=True) if title_el else ""
        link = title_el.get("href", "") if title_el else ""
        if title and link:
            results.append(
                {"title": title, "url": link, "snippet": "", "source": "Nepali Times"}
            )

    return results


# ── Source 4: Kathmandu Post search ─────────────────────────────────────────
def kathmandu_post_search(name_en, district_en):
    query = f"{name_en} {district_en}"
    url = f"https://kathmandupost.com/search?query={urllib.parse.quote_plus(query)}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    for article in soup.select(
        ".article-image + .article-content, .article-list article"
    )[:3]:
        title_el = article.select_one("h2 a, h3 a, .title a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        link = title_el.get("href", "")
        if not link.startswith("http"):
            link = "https://kathmandupost.com" + link
        results.append(
            {"title": title, "url": link, "snippet": "", "source": "Kathmandu Post"}
        )

    return results


# ── Source 5: OnlineKhabar search ────────────────────────────────────────────
def onlinekhabar_search(name_ne, name_en):
    # Try English search
    url = f"https://english.onlinekhabar.com/?s={urllib.parse.quote_plus(name_en)}"
    resp = safe_get(url)
    results = []
    if resp:
        soup = BeautifulSoup(resp.text, "lxml")
        for article in soup.select(".ok-news-post")[:3]:
            title_el = article.select_one("h2 a, h3 a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            link = title_el.get("href", "")
            results.append(
                {"title": title, "url": link, "snippet": "", "source": "OnlineKhabar"}
            )

    return results


# ── Source 6: Setopati search ────────────────────────────────────────────────
def setopati_search(name_en):
    url = f"https://www.setopati.com/search/{urllib.parse.quote_plus(name_en)}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    for article in soup.select(".news-list li, .search-result-item")[:3]:
        title_el = article.select_one("a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        link = title_el.get("href", "")
        if not link.startswith("http"):
            link = "https://www.setopati.com" + link
        if title:
            results.append(
                {"title": title, "url": link, "snippet": "", "source": "Setopati"}
            )

    return results


# ── Source 7: Election Commission Nepal candidate data ───────────────────────
def election_commission_search(name_en, district_en, const):
    """
    Scrape Nepal Election Commission results pages.
    election.gov.np publishes constituency-wise candidate lists and results.
    """
    results = []

    # EC Nepal 2079 (2022) results — constituency results page
    url = f"https://election.gov.np/en/page/result-2079"
    resp = safe_get(url)
    if resp:
        soup = BeautifulSoup(resp.text, "lxml")
        # Check if name appears anywhere on page
        text = soup.get_text()
        if name_en.lower() in text.lower():
            results.append(
                {
                    "title": f"EC Nepal: {name_en} - Official Record",
                    "url": url,
                    "snippet": f"Candidate {name_en} found in Election Commission Nepal 2079 records.",
                    "source": "Election Commission Nepal",
                }
            )

    return results


# ── Fetch article full text ───────────────────────────────────────────────────
def fetch_article_text(url, max_chars=3000):
    """Fetch and extract main text from an article URL."""
    resp = safe_get(url, timeout=20)
    if not resp:
        return ""

    soup = BeautifulSoup(resp.text, "lxml")

    # Remove nav, footer, ads
    for tag in soup(
        ["script", "style", "nav", "footer", "header", "aside", "form", "iframe"]
    ):
        tag.decompose()

    # Try known article content selectors
    for selector in [
        "article",
        ".article-content",
        ".entry-content",
        ".post-content",
        ".story-content",
        ".news-content",
        "main",
        ".content-area",
    ]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if len(text) > 200:
                return text[:max_chars]

    # Fallback: all paragraphs
    paragraphs = [
        p.get_text(strip=True)
        for p in soup.find_all("p")
        if len(p.get_text(strip=True)) > 40
    ]
    return " ".join(paragraphs)[:max_chars]


# ── Election-relevance filter ─────────────────────────────────────────────────
ELECTION_KEYWORDS = [
    "election",
    "निर्वाचन",
    "candidate",
    "उम्मेदवार",
    "vote",
    "मत",
    "constituency",
    "क्षेत्र",
    "MP",
    "parliament",
    "संसद",
    "result",
    "winner",
    "विजयी",
    "defeated",
    "polling",
    "campaign",
    "चुनाव",
    "party",
    "दल",
    "manifesto",
    "seat",
    "ward",
]


def is_election_relevant(text):
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in ELECTION_KEYWORDS)


# ── Main per-candidate search ─────────────────────────────────────────────────
def search_candidate(row):
    name_en = row["EnglishCandidateName"]
    name_ne = row["CandidateName"]
    dist_en = row["EnglishDistrictName"]
    dist_ne = row["DistrictName"]
    const = row["ConstName"]

    all_links = []

    # Build targeted queries
    queries = [
        f'"{name_en}" {dist_en} constituency {const} Nepal election',
        f'"{name_en}" Nepal election 2079 candidate',
        f"{name_en} {dist_en} Nepal vote result",
        f'"{name_ne}" निर्वाचन {const}',  # Nepali query
    ]

    # 1. DuckDuckGo (primary — no bot block)
    print(f"      → DDG search...")
    for q in queries[:2]:
        results = duckduckgo_search(q, max_results=4)
        all_links.extend(results)
        sleep()
        if len(all_links) >= MAX_RESULTS_PER_CANDIDATE:
            break

    # 2. Bing News RSS (no JS, very reliable)
    if len(all_links) < MAX_RESULTS_PER_CANDIDATE:
        print(f"      → Bing News RSS...")
        bing_results = bing_news_search(
            f"{name_en} {dist_en} Nepal election", max_results=4
        )
        all_links.extend(bing_results)
        sleep()

    # 3. Dedicated Nepali news sites
    if len(all_links) < MAX_RESULTS_PER_CANDIDATE:
        print(f"      → Nepali Times...")
        all_links.extend(nepali_times_search(name_en, dist_en))
        sleep()

    if len(all_links) < MAX_RESULTS_PER_CANDIDATE:
        print(f"      → Kathmandu Post...")
        all_links.extend(kathmandu_post_search(name_en, dist_en))
        sleep()

    if len(all_links) < MAX_RESULTS_PER_CANDIDATE:
        print(f"      → OnlineKhabar...")
        all_links.extend(onlinekhabar_search(name_ne, name_en))
        sleep()

    if len(all_links) < MAX_RESULTS_PER_CANDIDATE:
        print(f"      → Setopati...")
        all_links.extend(setopati_search(name_en))
        sleep()

    # 4. Election Commission
    ec_results = election_commission_search(name_en, dist_en, const)
    all_links.extend(ec_results)

    # Deduplicate by URL
    seen_urls = set()
    unique_links = []
    for item in all_links:
        url = item.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_links.append(item)

    # Score and prioritize election-relevant results
    scored = []
    for item in unique_links:
        combined = (item.get("title", "") + " " + item.get("snippet", "")).lower()
        score = sum(1 for kw in ELECTION_KEYWORDS if kw.lower() in combined)
        # Boost if name appears in title
        if name_en.lower() in item.get("title", "").lower():
            score += 3
        if name_ne in item.get("title", ""):
            score += 3
        item["relevance_score"] = score
        scored.append(item)

    scored.sort(key=lambda x: x["relevance_score"], reverse=True)
    top = scored[:MAX_RESULTS_PER_CANDIDATE]

    # Fetch full article text for top results
    articles = []
    for item in top:
        url = item.get("url", "")
        snippet = item.get("snippet", "")

        content = snippet  # default to snippet
        if url and not snippet:
            print(f"      → Fetching article text: {url[:70]}")
            content = fetch_article_text(url)
            sleep()

        articles.append(
            {
                "Constituency": const,
                "DistrictEnglish": dist_en,
                "DistrictNepali": dist_ne,
                "CandidateEnglish": name_en,
                "CandidateNepali": name_ne,
                "Source": item.get("source", ""),
                "Title": item.get("title", ""),
                "URL": url,
                "Snippet": snippet[:500],
                "Content": content[:5000],
                "RelevanceScore": item.get("relevance_score", 0),
                "ScrapedAt": datetime.now().isoformat(),
            }
        )

    return articles


# ── Path helpers ──────────────────────────────────────────────────────────────
def slugify(text):
    """Convert 'Rabi Lamichhane' → 'rabi_lamichhane'"""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)  # remove punctuation
    text = re.sub(r"[\s\-]+", "_", text)  # spaces/hyphens → underscore
    text = re.sub(r"_+", "_", text).strip("_")  # collapse underscores
    return text


def candidate_path(dist_en, const, name_en):
    """
    Returns Path: data/chitwan_2/rabi_lamichhane.csv
    Folder = district_constituency  (e.g. chitwan_2)
    File   = candidate_name.csv     (e.g. rabi_lamichhane.csv)
    """
    folder = slugify(f"{dist_en}_{const}")
    fname = slugify(name_en) + ".csv"
    return Path(OUTPUT_DIR) / folder / fname


def already_scraped(path):
    """Skip if file exists and has at least one real result row."""
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path)
        return not df.empty and df["Title"].fillna("").str.strip().ne("").any()
    except Exception:
        return False


# ── Constituency picker ───────────────────────────────────────────────────────
def pick_constituencies(df):
    """
    Interactive menu — shows all unique district+constituency combos,
    lets the user pick one or more, or type 'all'.
    Returns a filtered DataFrame.
    """
    # Build sorted list of unique (district, const) pairs
    combos = (
        df[["EnglishDistrictName", "DistrictName", "ConstName"]]
        .drop_duplicates()
        .sort_values(["EnglishDistrictName", "ConstName"])
        .reset_index(drop=True)
    )

    # Show status for each (already scraped / total candidates)
    print("\n┌─────────────────────────────────────────────────────────────────┐")
    print("│          Nepal Election Scraper — Constituency Picker           │")
    print("├──────┬──────────────────────────────────┬────────────┬──────────┤")
    print("│  #   │ Constituency                     │ Candidates │  Status  │")
    print("├──────┼──────────────────────────────────┼────────────┼──────────┤")

    for i, row in combos.iterrows():
        dist_en = row["EnglishDistrictName"]
        const = row["ConstName"]
        label = f"{dist_en.title()} - {const}"

        # Count candidates and how many already scraped
        mask = (df["EnglishDistrictName"] == dist_en) & (df["ConstName"] == const)
        candidates = df[mask]
        total_c = len(candidates)
        done_c = sum(
            already_scraped(candidate_path(dist_en, const, r["EnglishCandidateName"]))
            for _, r in candidates.iterrows()
        )

        status = f"{done_c}/{total_c} done" if done_c > 0 else "not started"
        status_icon = "✅" if done_c == total_c else ("🔄" if done_c > 0 else "⬜")

        print(
            f"│ {i + 1:>3}  │ {label:<32} │ {total_c:>10} │ {status_icon} {status:<6} │"
        )

    print("└──────┴──────────────────────────────────┴────────────┴──────────┘")

    print("\n💡 Enter numbers to select (e.g.  1,3,5  or  2-5  or  all)")
    print("   Tip: each person on your team picks different numbers!\n")

    while True:
        raw = input("Your selection: ").strip().lower()

        if raw == "all":
            selected_indices = list(range(len(combos)))
            break

        # Parse "1,3,5" and "2-5" and mixed "1,3-5,7"
        selected_indices = []
        try:
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    a, b = part.split("-")
                    selected_indices.extend(range(int(a) - 1, int(b)))
                else:
                    selected_indices.append(int(part) - 1)

            # Validate
            if not selected_indices:
                raise ValueError
            if any(i < 0 or i >= len(combos) for i in selected_indices):
                print(f"   ⚠ Numbers must be between 1 and {len(combos)}. Try again.")
                continue
            break

        except (ValueError, AttributeError):
            print("   ⚠ Invalid input. Use numbers like: 1,3,5  or  2-5  or  all")

    # Build filter
    selected_combos = combos.iloc[selected_indices]
    print("\n📌 Selected constituencies:")
    for _, row in selected_combos.iterrows():
        print(f"   • {row['EnglishDistrictName'].title()} - {row['ConstName']}")

    # Filter main dataframe
    mask = df.apply(
        lambda r: any(
            r["EnglishDistrictName"] == sc["EnglishDistrictName"]
            and r["ConstName"] == sc["ConstName"]
            for _, sc in selected_combos.iterrows()
        ),
        axis=1,
    )
    return df[mask].reset_index(drop=True)


# ── Runner ────────────────────────────────────────────────────────────────────
def main():
    df = pd.read_csv(INPUT_FILE)

    print(f"📋 Loaded {len(df)} candidates from {INPUT_FILE}")
    print(f"📁 Saving to: {OUTPUT_DIR}/<district_const>/<candidate>.csv")
    print(
        f"🔍 Sources: DuckDuckGo · Bing News · Nepali Times · KPost · OnlineKhabar · Setopati"
    )

    # ── Ask which constituencies to scrape ────────────────────────────────────
    subset = pick_constituencies(df)
    total = len(subset)
    skipped = 0
    done = 0

    print(f"\n🚀 Starting scrape for {total} candidates...\n")

    for idx, row in subset.iterrows():
        name_en = row["EnglishCandidateName"]
        name_ne = row["CandidateName"]
        dist_en = row["EnglishDistrictName"]
        dist_ne = row["DistrictName"]
        const = row["ConstName"]

        out_path = candidate_path(dist_en, const, name_en)

        # ── Resume: skip already-scraped ──────────────────────────────────────
        if already_scraped(out_path):
            print(f"[{idx + 1}] ⏭  Skipping {name_en} (already scraped)")
            skipped += 1
            continue

        print(f"\n[{done + skipped + 1}/{total}] 🔎 {name_en} | {dist_en}-{const}")
        print(f"   📄 → {out_path}")

        try:
            articles = search_candidate(row)
        except Exception as e:
            print(f"   ❌ Error: {e}")
            articles = []

        if not articles:
            print(f"   ⚠ No results found.")
            articles = [
                {
                    "Constituency": const,
                    "DistrictEnglish": dist_en,
                    "DistrictNepali": dist_ne,
                    "CandidateEnglish": name_en,
                    "CandidateNepali": name_ne,
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
            print(f"   ✅ {len(articles)} result(s) saved.")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(articles).to_csv(out_path, index=False)
        done += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print(
        f"✅ Done!  Scraped: {done}  |  Skipped (cached): {skipped}  |  Total: {total}"
    )
    print(f"\n── Folder summary ──")
    data_path = Path(OUTPUT_DIR)
    if data_path.exists():
        for folder in sorted(data_path.iterdir()):
            if folder.is_dir():
                files = list(folder.glob("*.csv"))
                with_data = sum(1 for f in files if already_scraped(f))
                bar = "█" * with_data + "░" * (len(files) - with_data)
                print(f"   {folder.name:<30} {bar}  {with_data}/{len(files)}")


if __name__ == "__main__":
    main()

import os
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

PAGE_IDS = [
    "officialroutineofnepalbanda",
]
USER_DATA_DIR = "./sessions"
OUTPUT_DIR = "./harvested"
TARGET_POSTS = 15
MAX_SCROLLS = 50
SCROLL_PAUSE = 3.0


def capture_posts(page, page_id, seen_ids: set) -> list:
    # 1. Define the locator for stories
    story_locator = page.locator('div[data-focus="feed_story"]')

    # 2. Get the current count of visible stories
    # count() is a method in Playwright Python
    try:
        count = story_locator.count()
    except:
        return []

    saved = []

    for i in range(count):
        try:
            # 3. Re-fetch the node by index (Freshness)
            node = story_locator.nth(i)

            # Ensure the node is actually attached/visible before proceeding
            if not node.is_visible():
                continue

            # 4. Target the 3rd link (Index 2) for the timestamp
            all_links = node.locator('a[role="link"]')

            # Logic check: Must have at least 3 links (Name, Image, Timestamp)
            if all_links.count() < 3:
                continue

            link_el = all_links.nth(2)

            # 5. Extract ID and Check if Seen
            raw_href = link_el.get_attribute("href") or ""

            # If Index [2] doesn't look like a post link, try a generic search
            if not any(x in raw_href for x in ["/posts/", "pfbid", "permalink"]):
                fallback = node.locator(
                    'a[href*="/posts/"], a[href*="pfbid"], a[href*="/permalink/"]'
                ).first
                if fallback.count() > 0:
                    link_el = fallback
                    raw_href = link_el.get_attribute("href") or ""
                else:
                    continue  # Skip if no post link found at all

            post_id = raw_href.split("?")[0]
            if not post_id or post_id in seen_ids:
                continue

            # --- TIMESTAMP EXTRACTION ---
            full_timestamp = "Unknown"

            # A. Try aria-label first
            label = link_el.get_attribute("aria-label")
            if label and len(label) > 12:
                full_timestamp = label
            else:
                # B. Hover "Wiggle"
                link_el.scroll_into_view_if_needed()
                box = link_el.bounding_box()
                if box:
                    # Clear previous tooltips
                    page.mouse.move(0, 0)

                    # Target center of timestamp link
                    center_x = box["x"] + box["width"] / 2
                    center_y = box["y"] + box["height"] / 2

                    page.mouse.move(center_x, center_y)
                    # Small wiggle to wake up JS listeners
                    page.mouse.move(center_x + 2, center_y + 2)

                    try:
                        # Wait for tooltip injection
                        tooltip_selector = '[role="tooltip"], .uiContextualLayer'
                        page.wait_for_selector(tooltip_selector, timeout=1200)
                        tooltip = page.query_selector(tooltip_selector)
                        if tooltip:
                            full_timestamp = tooltip.inner_text().strip()
                    except:
                        full_timestamp = "Hover Timeout"

            # Move mouse away so the next post doesn't get a 'ghost' tooltip
            page.mouse.move(0, 0)

            # --- SAVE BLOCK ---
            seen_ids.add(post_id)
            index = len(seen_ids) - 1

            # Capture the fresh HTML
            outer_html = node.evaluate("el => el.outerHTML")

            # Embed metadata at the top for easy CSV parsing later
            metadata = f""
            save_content = f"{metadata}\n{outer_html}"

            PAGE_DIR = os.path.join(OUTPUT_DIR, page_id)
            os.makedirs(PAGE_DIR, exist_ok=True)
            filename = os.path.join(PAGE_DIR, f"[{index}].html")

            with open(filename, "w", encoding="utf-8") as f:
                f.write(save_content)

            saved.append(filename)
            print(f"    ✓ Captured [{index}]: {full_timestamp}")

        except Exception as e:
            # Handle potential stale element during processing
            continue

    return saved


def harvest_page(page, page_id):
    print(f"\n{'=' * 40}")
    print(f"Loading {page_id}...")

    page.goto(
        f"https://www.facebook.com/{page_id}",
        wait_until="domcontentloaded",
        timeout=30_000,
    )

    # Wait for at least one post to appear
    try:
        page.wait_for_selector('div[data-focus="feed_story"]', timeout=15_000)
    except Exception:
        print(f"  ✗ No feed stories found for {page_id}")
        return None

    # Let the page settle fully
    time.sleep(SCROLL_PAUSE)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    seen_hashes: set = set()
    all_saved: list = []
    stale_scrolls = 0

    for scroll_num in range(MAX_SCROLLS):
        # Capture whatever is visible RIGHT NOW before scrolling away
        newly_saved = capture_posts(page, page_id, seen_hashes)
        all_saved.extend(newly_saved)

        if newly_saved:
            stale_scrolls = 0
            print(
                f"  → Scroll {scroll_num}: +{len(newly_saved)} new | total={len(all_saved)}"
            )
        else:
            stale_scrolls += 1
            print(
                f"  → Scroll {scroll_num}: +0 new | total={len(all_saved)} (stale {stale_scrolls}/6)"
            )

        if len(all_saved) >= TARGET_POSTS:
            print(f"  ✓ Reached target of {TARGET_POSTS} posts!")
            break

        if stale_scrolls >= 6:
            print("  ✗ Feed exhausted")
            break

        # Scroll down and wait for new content to load
        page.evaluate("window.scrollBy(0, window.innerHeight * 0.5)")
        time.sleep(SCROLL_PAUSE)

        # Wait for DOM to update with new posts
        try:
            page.wait_for_function(
                """() => document.querySelectorAll('div[data-focus="feed_story"]').length > 0""",
                timeout=5_000,
            )
        except Exception:
            pass  # continue anyway

    print(f"  → Final: {len(all_saved)} posts saved for {page_id}")
    return all_saved if all_saved else None


def harvest_all():
    captured_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Starting bulk harvest at {captured_at}")
    print(f"Pages to scrape: {len(PAGE_IDS)}")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=True,
            viewport={"width": 1280, "height": 800},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        results = {"success": [], "failed": []}

        for i, page_id in enumerate(PAGE_IDS, 1):
            print(f"\n[{i}/{len(PAGE_IDS)}] {page_id}")
            try:
                saved = harvest_page(page, page_id)
                if saved:
                    results["success"].append(page_id)
                else:
                    results["failed"].append(page_id)
            except Exception as e:
                print(f"  ✗ Error: {e}")
                results["failed"].append(page_id)

        context.close()

    print(f"\n{'=' * 40}")
    print(
        f"Done! ✓ {len(results['success'])} succeeded, ✗ {len(results['failed'])} failed"
    )
    if results["failed"]:
        print(f"Failed: {results['failed']}")


if __name__ == "__main__":
    harvest_all()

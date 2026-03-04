import asyncio
from playwright.async_api import async_playwright


async def manual_login():
    async with async_playwright() as p:
        # USER_DATA_DIR saves your cookies and local storage
        user_data_dir = "./sessions"

        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,  # Must be False for you to log in
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = await context.new_page()
        await page.goto("https://x.com/login")

        print("--- ACTION REQUIRED ---")
        print("Please log in manually in the browser window.")
        print("Once you see your home feed, close this terminal or wait 60 seconds.")

        await asyncio.sleep(60)
        await context.close()


if __name__ == "__main__":
    asyncio.run(manual_login())

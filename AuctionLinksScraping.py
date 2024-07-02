import asyncio
import pymongo
from undetected_playwright.async_api import async_playwright
import asyncio
from dotenv import load_dotenv
load_dotenv()

async def open_browser(page):
    # await page.set_viewport_size({'width': 1920, 'height': 1080})
    # await page.set_viewport_size({'width': 1600, 'height': 1080})
    await page.emulate_media(color_scheme='dark')
    # weblink="https://www.iaai.com"
    # weblink="https://www.iaai.com/LiveAuctionsCalendar"
    weblink="https://www.iaai.com/Login/ExternalLogin?ReturnUrl=%2FDashboard%2FDefault"
    await page.goto(weblink, wait_until='load')
    await asyncio.sleep(3)
    # Find the email input field by its ID
    email_input = await page.query_selector('#Email')
    # Enter the desired content
    email = "matti19913@gmail.com"
    await email_input.fill(email)
    password_input = await page.query_selector('#Password')
    # Clear the existing value (if any)
    await password_input.fill('')
    # Enter the desired content
    password = "Copart2023"
    await password_input.fill(password)
    # Clicking on the remember me checkbox
    await page.click('text=Remember Me')
    # Clicking on the login button
    await page.click('button[type="submit"]')
    # Waiting for the page to load
    await asyncio.sleep(5)

    return page


async def navigate_to_auctions(page):
    await page.hover('text=Auctions')
    await page.click('text=Live Auctions')

    await asyncio.sleep(5)
    if await page.is_visible("text=Accept All Cookies"):
        await page.click("text=Accept All Cookies")
    else:
        await page.click("text=I understand")

async def fetch_live_auctions(browser , page, collection):
    live_auctions=[]

    while True:

        # from datetime import datetime

        # # Get the current time
        # now = datetime.now()

        # # Format the time to hours and minutes
        # time_string = now.strftime("%H:%M")

        # # Get the day of the week
        # day_of_week = now.strftime("%A")

        # print("Current Time =", time_string)
        # print("Day of the Week =", day_of_week)

        # weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

        # if time_string>="13:00" and (day_of_week in weekdays):
        # print("Time to scrape the Auction Links")

        await page.hover('text=Auctions')
        await page.click('text=Live Auctions')
        await asyncio.sleep(30)

        auction_section = await page.query_selector('#dvListLiveAuctions')
        all_auctions = await auction_section.query_selector_all('div.table-row.table-row-border')
        link_elements = await auction_section.query_selector_all("a.btn.btn-lg.btn-primary.btn-block")
        
        if len(link_elements)==0:
            await asyncio.sleep(300)
            print("No Auctions Found \nClosing the browser")
            await browser.close()
            break

        for auction in all_auctions:
            try:
                link_element = await auction.query_selector("a.btn.btn-lg.btn-primary.btn-block")
                if link_element is not None:
                    link_text = await link_element.inner_text()
                    if ("Bid Live" in link_text) or ("Join Auction" in link_text):
                        link = await link_element.get_attribute('href')
                        # Check if the link is already in the MongoDB collection
                        if collection.find_one({'link': link}) is None:
                            print(link)
                            # Upload the link to MongoDB
                            collection.insert_one({'link': link, 'Info': "None"})
                    print(f"Total Auctions: {len(live_auctions)}")
                else:
                    pass
            except Exception as e:
                print(e)
                pass
        await asyncio.sleep(2700)

async def main():
    async with async_playwright() as playwright:

        args = []
        # disable navigator.webdriver:true flag
        args.append("--disable-blink-features=AutomationControlled")
        browser = await playwright.chromium.launch(args=args,headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await open_browser(page)
        await asyncio.sleep(5)
        await navigate_to_auctions(page)
        await asyncio.sleep(5)


        client = pymongo.MongoClient(os.getenv("MONGOAUTh"))
        os.getenv("MONGO_URI")
        db = client['PortalAuction']
        collection = db['AuctionLinks']

        # Clearing all data from collection
        collection.delete_many({})

        await fetch_live_auctions(browser,page,collection)
      

if __name__ == "__main__":
    asyncio.run(main())

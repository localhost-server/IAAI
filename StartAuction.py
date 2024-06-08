import argparse
from undetected_playwright.async_api import async_playwright
import subprocess 
import asyncio
import pymongo
from pymongo import UpdateOne
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz

load_dotenv()
# Setting CDT timezone
cdt=pytz.timezone('America/Chicago')


async def open_browser(page, weblink):
    await page.emulate_media(color_scheme='dark')
    await page.goto(weblink, wait_until='load')
    return page

async def scrape_auction_data(auction_link, collection, link_collection):
    start_time = datetime.now()
    playwright = await async_playwright().start()
    args = ["--disable-blink-features=AutomationControlled"]
    browser = await playwright.chromium.launch(args=args, headless=False)

    context = await browser.new_context()
    page = await context.new_page()
    browse = await open_browser(page=page, weblink="https://www.iaai.com/Login/ExternalLogin?ReturnUrl=%2FDashboard%2FDefault")
    await asyncio.sleep(5)

    await page.goto("https://www.iaai.com/Login/ExternalLogin?ReturnUrl=%2FDashboard%2FDefault", wait_until='load')
    await asyncio.sleep(20)

    try:
        email_input = await page.query_selector('#Email')
        email = "matti19913@gmail.com"
        await email_input.fill(email)
        password_input = await page.query_selector('#Password')
        password = "Copart2023"
        await password_input.fill(password)
        await page.click('text=Remember Me')
        await page.click('button[type="submit"]')
        await asyncio.sleep(5)
    except:
        print("Bot detected")
        link_collection.update_one({'link': auction_link}, {'$set': {'Info': 'None'}})

    await page.goto(auction_link)
    await asyncio.sleep(5)

    button = await page.query_selector('button[type="submit"].btn.btn-md.btn-primary.d-flex.mt-20')
    if button:
        await button.click()
        await asyncio.sleep(5)

    data = {}
    while True:
        end_time = datetime.now()
        await page.wait_for_selector('div.AuctionContainer.event__item')
        multiple_auc_in_single_page = await page.query_selector_all('div.AuctionContainer.event__item')
        # auctioning_completed = await page.query_selector_all("div.event-empty__content")
        auctioning_completed = await page.query_selector_all('h2.event-empty__title[data-translate="AuctionCompleted"]')
        # print(f'Number of auctions in the page: {len(multiple_auc_in_single_page)}')
        # print(f'Number of auctions completed: {len(auctioning_completed)}')
        if (len(auctioning_completed)>0) and (len(auctioning_completed) == len(multiple_auc_in_single_page)) and ((end_time - start_time).total_seconds()/60 > 30):

            print(f'Auction Closed {auction_link}')
            await page.close()
            await browser.close()

            if not data:
                return

            data_list = [{"carLink": k, "price": v , "date": datetime.now(cdt).date().strftime("%d-%m-%Y").replace('-','.')} for k, v in data.items() if k != 'None' and v != ""]
            carLink_list = [i['carLink'] for i in data_list]

            subprocess.Popen(["python3", "check_link.py", ' '.join(carLink_list)])
            print(f"Data captured of {len(data_list)} cars")
            print(data_list)

            # Prepare bulk write operations
            operations = []
            for data in data_list:
                price_obj = {"date": data["date"], "price": data["price"]}
                operations.append(
                    UpdateOne(
                        {"carLink": data["carLink"]},
                        {"$push": {"prices": price_obj}, "$setOnInsert": {"carLink": data["carLink"]}},
                        upsert=True
                    )
                )

            # Execute bulk write operations
            collection.bulk_write(operations)
            # collection.insert_many(data_list)
            
            
            link_collection.update_one({'link': auction_link}, {'$set': {'Info': 'done'}})

            return
        else:
            if multiple_auc_in_single_page:
                for auc in multiple_auc_in_single_page:
                    content = await auc.query_selector('span.stock-number')
                    if content is None:
                        continue
                    internal_link = await content.query_selector('a')
                    if internal_link is None:
                        continue
                    else:
                        identity = await internal_link.get_attribute('href')

                    try:
                        price=None
                        await auc.wait_for_selector('div.js-BidActions')
                        content = await auc.query_selector('div.js-BidActions')
                        high_bid_element = await content.query_selector("span.high-bid__amount")
                        if high_bid_element is not None:
                            price = await high_bid_element.inner_text()
                        else:
                            bid_now_element = await content.query_selector("span.bid-now__amount")
                            if bid_now_element is not None:
                                price = await bid_now_element.inner_text()
                                del bid_now_element
                            else:
                                price = ""
                    except:
                        if not price:
                            price = ""
                        continue

                    data[str(identity)] = price
                    print(data)
                    del content , internal_link , identity , price , high_bid_element , auc

# Parse command line arguments
parser = argparse.ArgumentParser(description='Scrape auction data.')
parser.add_argument('CarLink', type=str, help='The auction link to scrape.')
args = parser.parse_args()

# Create a new client
client = pymongo.MongoClient(os.getenv("MONGOAUTH"))

# Get a reference to the database
db = client['PortalAuction']

# Get a reference to the collection
collection = db['CarsPrice']
link_collection = db['AuctionLinks']

# Usage
asyncio.run(scrape_auction_data(args.CarLink, collection, link_collection))

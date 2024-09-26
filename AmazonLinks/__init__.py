# AmazonLinks
import time
import asyncio
import random
import logging
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, Any

# Logging Intialization
logging.basicConfig(level=logging.INFO)


# List of User-Agent strings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0"
]


async def fetch_page(session, url, max_retries=3, initial_delay=2):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    delay = initial_delay
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 503:
                    logging.warning(
                        f"Received 503 error. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                    # Exponential backoff
                    delay *= 2
                else:
                    logging.error(
                        f"Error fetching {url}: HTTP status {response.status}")
                    return None
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            await asyncio.sleep(delay)
            delay *= 2

    logging.error(f"Max retries reached for {url}")
    return None


async def scrape_page_products(session, page_url, base_url):
    html = await fetch_page(session, page_url)
    if not html:
        return [], None

    soup = BeautifulSoup(html, 'html.parser')

    product_links = soup.find_all(
        'a', class_='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal')

    list_products_links = [urljoin(base_url, link.get('href'))
                           for link in product_links if link.get('href')]

    next_button = soup.select_one("a.s-pagination-next")
    next_page_url = urljoin(
        base_url, next_button['href']) if next_button and next_button.get('href') else None

    return list_products_links, next_page_url


async def main(input: Dict[str, Any]) -> Dict[str, Any]:
    start_page_url = input['start_url']
    region = input["region"]
    all_product_links = []
    page_urls = []
    current_page_url = start_page_url
    pages_scraped = 0

    logging.info(f'Amazon_{region}_page_links function processing...')

    if region == 'egypt':
        base_url = 'https://www.amazon.eg'
    elif region == 'saudi':
        base_url = 'https://www.amazon.sa'
    else:
        raise ValueError(f"Unsupported region: {region}")

    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        while True:
            products, next_page_url = await scrape_page_products(session, current_page_url, base_url)
            all_product_links.extend(products)
            page_urls.append(current_page_url)
            pages_scraped += 1

            elapsed_time = time.time() - start_time
            if elapsed_time >= 240:  # 8.5 minutes
                logging.warning("Reached time limit, stopping the scraping.")
                return {
                    "links": all_product_links,
                    "next_page_url": current_page_url,
                    "pages_scraped": pages_scraped,
                    "page_urls": page_urls
                }

            if not next_page_url:
                logging.info("No more pages to scrape.")
                break

            current_page_url = next_page_url
            # Random sleep to avoid getting blocked
            await asyncio.sleep(random.uniform(1, 3))

    logging.info(
        f"Amazon {region} Page Links: Found {len(all_product_links)} links across {pages_scraped} pages")

    return {
        "links": all_product_links,
        "next_page_url": None,
        "pages_scraped": pages_scraped,
        "page_urls": page_urls
    }

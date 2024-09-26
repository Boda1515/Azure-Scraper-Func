# AmazonData
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import aiohttp
import asyncio
import re
from datetime import datetime
import random
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)

# List of User-Agent strings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
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
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 503:
                    logging.warning(
                        f"Received 503 error. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    logging.warning(
                        f"Error fetching {url}: HTTP status {response.status}")
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Error fetching {url}: {str(e)}")
            await asyncio.sleep(delay)
            delay *= 2

    logging.info(f"Max retries reached for {url}")
    return None


def clean_text(text):
    text = re.sub(r'[\u200f\u200e]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def remove_key_from_value(key, value):
    key_cleaned = clean_text(key)
    value_cleaned = clean_text(value)
    if value_cleaned.startswith(key_cleaned):
        return value_cleaned[len(key_cleaned):].strip(" :")
    return value_cleaned


async def scrape_product_data(session, product_url):
    html = await fetch_page(session, product_url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')

    # Extracting Price
    price_selector_1 = "#corePriceDisplay_desktop_feature_div .a-price-whole"
    price_selector_2 = "div.a-section.a-spacing-micro span.a-price.a-text-price.a-size-medium span.a-offscreen"

    price_element = soup.select_one(price_selector_1)
    if price_element:
        price = price_element.get_text(strip=True)
    else:
        price_element = soup.select_one(price_selector_2)
        price = price_element.get_text(strip=True) if price_element else np.nan

    # Extract discount
    discount = None

    # Try various selectors
    possible_selectors = [
        "span.a-color-price",
        ".savingsPercentage"
    ]

    for selector in possible_selectors:
        discount_elements = soup.select(selector)
        for element in discount_elements:
            discount_text = element.get_text(strip=True)

            # Flexible regex to capture both negative and non-negative percentages
            discount_match = re.search(r'(-?\d+%)', discount_text)

            if discount_match:
                discount = discount_match.group(1)
                break
        if discount:
            break

    # Logic of the row price if found
    Price_before_discount = soup.find(
        'span', class_="a-size-small aok-offscreen")
    row_price = Price_before_discount.get_text() if Price_before_discount else np.nan

    # Rating extraction logic
    rate_element = soup.select_one("span.a-icon-alt")
    if rate_element and "out of 5 stars" in rate_element.text:
        rate = rate_element.text.replace("out of 5 stars", "").strip()
    else:
        rate = np.nan

    product_data = {
        "date_column": datetime.today().strftime('%Y-%m-%d'),
        "product_url": product_url,
        "site": "amazon_sa",
        "category": "mobile phones",
        "Title": soup.select_one("#productTitle").text.strip() if soup.select_one("#productTitle") else np.nan,
        "Rate": rate,
        "Price": price,
        "Price Before Discount": row_price,
        "Discount": discount,
        "Image URL": soup.select_one("#imgTagWrapperId img")['src'] if soup.select_one("#imgTagWrapperId img") else np.nan,
        "Description": soup.select_one("#feature-bullets").text.strip() if soup.select_one("#feature-bullets") else np.nan
    }

    tables = {
        'first_table': '.a-normal.a-spacing-micro',
        'tech_specs': '#productDetails_techSpec_section_1',
        'right_table': '#productDetails_detailBullets_sections1',
        'new_table': 'ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list'
    }

    for table_name, selector in tables.items():
        table = soup.select_one(selector)
        if table:
            if table_name == 'new_table':
                items = table.find_all('li')
                for item in items:
                    key_element = item.select_one('span.a-text-bold')
                    value_element = item.find(
                        'span', class_=lambda x: x != 'a-text-bold')
                    if key_element and value_element:
                        key = clean_text(
                            key_element.text.strip().replace(':', ''))
                        value = clean_text(value_element.text.strip())
                        value = remove_key_from_value(key, value)
                        product_data[key] = value
            else:
                rows = table.find_all('tr')
                for row in rows:
                    key_element = row.find(['th', 'td'])
                    value_element = row.find_all(
                        'td')[-1] if row.find_all('td') else None
                    if key_element and value_element:
                        key = clean_text(key_element.get_text(strip=True))
                        value = clean_text(value_element.get_text(strip=True))
                        product_data[key] = value

    reviews = []
    review_cards = soup.select("div[data-hook='review']")
    for review in review_cards[:5]:
        reviewer_name = review.select_one("span.a-profile-name").text.strip()
        review_rating = review.select_one(
            "i.a-icon-star span.a-icon-alt").text.strip().replace("out of 5 stars", "")
        review_date = review.select_one("span.review-date").text.strip()
        review_text = review.select_one(
            "span[data-hook='review-body']").text.strip()
        reviews.append({
            "Reviewer": reviewer_name,
            "Rating": review_rating,
            "Date": review_date,
            "Review": review_text
        })

    product_data['reviews'] = reviews
    return product_data


async def scrape_all_products(product_links, time_limit=240):
    async with aiohttp.ClientSession() as session:
        tasks = []
        start_time = time.time()

        for url in product_links:
            elapsed_time = time.time() - start_time
            if elapsed_time >= time_limit:
                logging.warning(
                    f"Time limit reached. Scraping stopped after {elapsed_time:.2f} seconds.")
                break

            tasks.append(scrape_product_data(session, url))

        all_product_data = await asyncio.gather(*tasks, return_exceptions=True)

    scraped_data = []
    remaining_links = []

    for i, result in enumerate(all_product_data):
        if isinstance(result, Exception):
            logging.error(f"Error scraping {product_links[i]}: {str(result)}")
            remaining_links.append(product_links[i])
        elif result is not None:
            scraped_data.append(result)
        else:
            remaining_links.append(product_links[i])

    return scraped_data, remaining_links

# Main function to call from the orchestrator


async def main(input: dict) -> dict:
    # A list of product URLs
    product_links = input['product_link']
    region = input["region"]

    logging.info(f'Amazon_{region}_product_data function processing...')
    logging.info(f"Number of product links accepted: {len(product_links)}")
    # Scrape products with a time limit of 4 minutes
    product_data, remaining_links = await scrape_all_products(product_links, time_limit=240)

    # Return both the scraped data and any remaining links for the orchestrator
    return {
        "scraped_data": product_data,
        "remaining_links": remaining_links
    }

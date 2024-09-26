# DurableFunctionsOrchestrator1
import pandas as pd
import logging
import random
from datetime import timedelta
from azure.durable_functions import DurableOrchestrationContext, Orchestrator
import azure.durable_functions as df
from datetime import datetime
import os


def orchestrator_function(context: df.DurableOrchestrationContext):
    # Get Data from Http starter
    input_data = context.get_input()
    start_url = input_data["start_url"]
    region = input_data["region"]

    # Initialize call counts For Activity Functions
    AmazonLinks = 0
    AmazonData = 0

    ###########################################################################################

    # Process AmazonLinks   ---> First Activity
    links = []
    pages_scraped = 0
    current_url = start_url

    # Loop through pages
    while current_url:
        AmazonLinks += 1  # Increment activity call count
        result = yield context.call_activity("AmazonLinks", {"start_url": current_url, "region": region})
        links.extend(result["links"])
        pages_scraped += result["pages_scraped"]

        # Check if time limit is reached in the activity function
        if result["next_page_url"] is None:
            logging.info("No more pages to scrape.")
            break
        # Move to next page
        current_url = result["next_page_url"]

        # Add a random delay between page requests
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=random.uniform(1, 3)))
    logging.info(
        f"AmazonLinks completed. Pages scraped: {pages_scraped}, Links found: {len(links)}")

    ###########################################################################################
    # Process AmazonData   ---> Second Activity
    scraped_data = []
    product_links = links  # Initially, all links are remaining
    # Smaller chunk size for consumption plan (around 5 pages)
    chunk_size = 120
    max_retries = 3  # Maximum number of retries for failed chunks

    # Loop through  links to scrape product data
    while product_links:
        AmazonData += 1
        chunk = product_links[:chunk_size]
        retries = 0
        while retries < max_retries:
            try:
                amazon_products_data = yield context.call_activity("AmazonData", {
                    "product_link": chunk,
                    "region": region
                })

                # Check if amazon_products_data is a dictionary and has the expected keys
                scraped_data.extend(amazon_products_data["scraped_data"])
                product_links = product_links[chunk_size:] + \
                    amazon_products_data["remaining_links"]
                break
            except Exception as e:
                logging.error(
                    f"Error processing chunk (attempt {retries + 1}): {str(e)}")
                retries += 1
                if retries < max_retries:
                    # Add an exponential backoff delay before retrying
                    yield context.create_timer(context.current_utc_datetime + timedelta(seconds=2 ** retries))
                else:
                    # Move the failed chunk to the end of the list after max retries
                    product_links = product_links[chunk_size:] + chunk
        # Add a random delay between chunk processing
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=random.uniform(2, 5)))

    #############################################################################################

    # Log activity call counts
    logging.info(
        f"AmazonLinks activity function called {AmazonLinks} times.")
    logging.info(
        f"AmazonData activity function called {AmazonData} times.")

    # ##########
    # # Save page links to CSV
    # links_csv_path = save_to_csv(f"Links_{region}", links)

    # # # Save products data to CSV
    data_csv_path = save_to_csv(f"Data_{region}", scraped_data)

    # ##########

    # Return the combined results with metrics and CSV paths
    return {
        "AmazonLinks": {
            "links": links,
            "pages_scraped": pages_scraped,
            "links_found": len(links),
            # "csv_path": links_csv_path,
        },
        "AmazonData": {
            "scraped_data": scraped_data,
            "remaining_links": product_links,
            "csv_path": data_csv_path,
        }
    }


def save_to_csv(region, links):
    # Generate a unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{region}_pages_{timestamp}.csv"
    filepath = os.path.join(os.path.dirname(__file__), filename)

    # Create a DataFrame from the links
    df = pd.DataFrame(links)

    # Save the DataFrame to a CSV file using UTF-8 encoding
    df.to_csv(filepath, index=False, encoding="utf-8")

    return filepath


main = Orchestrator.create(orchestrator_function)

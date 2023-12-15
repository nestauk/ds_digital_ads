"""
Script to collect google ads from the Google Ads Transparency Center (https://adstransparency.google.com/?region=GB)
Uses the Serp API (https://serpapi.com/google-ads-transparency-center-api)
Collects all ads run since 2018, given a set of parameters (including advertiser ids and format). Advertisers are arranged by parent company, with individual brands sharing one ID.
To run the script, you will need an API key from Serp API (https://serpapi.com/) saved in your .env file

To run in test mode:
python collect_google_ads.py

To run in full mode:
python collect_google_ads.py --test_mode False

"""


import requests
import time
import random
import pandas as pd

import os
from dotenv import load_dotenv
import serpapi
import logging
from datetime import datetime

from ds_digital_ads import BUCKET_NAME
from ds_digital_ads.getters.data_getters import save_to_s3
from ds_digital_ads.utils.data_collection_utils import query_parameters_serp


def create_list_query_params(search_parameters: dict) -> list:
    load_dotenv()
    params_list = []
    search_parameters["api_key"] = os.environ.get("SERP_API_KEY")
    for advertiser in search_parameters["advertiser_id"]:
        for creative_format in search_parameters["creative_format"]:
            param_entry = search_parameters.copy()
            param_entry["advertiser_id"] = advertiser
            param_entry["creative_format"] = [creative_format]
            params_list.append(param_entry)

    return params_list


def get_serp_data(params_list: list) -> list:
    """Iterate through the parameters and place and API call for each unique combination"""
    # Configure and create the logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    serp_data = []
    for params in params_list:
        try:
            # Call the Serp API
            search = serpapi.search(params)

            # Create dictionary from search results and append to main results
            results = search.__dict__
            results = results["data"]

            params["results"] = results
            logger.info(
                f'API call successful for {params["advertiser_id"]}, {params["creative_format"]}'
            )
            serp_data.append(params)
        except:
            logger.warning(
                f'API call failed for {params["advertiser_id"]}, {params["creative_format"]}'
            )
            pass

        # Pause between calls
        time.sleep(random.randint(1, 5))

    return serp_data


def collect_save_ads(filename: str, test_mode=True) -> list:
    """Collects and saves the data from the Serp API"""
    params_list = create_list_query_params(query_parameters_serp)

    if test_mode == True:
        json_file_path = f"data_collection/google_ads/raw_json/{filename}_test.json"
        results = get_serp_data(params_list[0:2])
        save_to_s3(BUCKET_NAME, results, json_file_path)
    else:
        json_file_path = f"data_collection/google_ads/raw_json/{filename}.json"
        results = get_serp_data(params_list)
        save_to_s3(BUCKET_NAME, results, json_file_path)
    return results


def get_image_list(results: list) -> list:
    """Takes in the full list of results, and returns a list of dictionaries containing
    the advertiser id, URL and format of the ad for text and images. With current results, returns 10 links
    """

    image_list = []

    for i in range(0, len(results)):
        print(i)
        if (
            "total_results" in results[i]["results"]["search_information"]
        ):  # Only select non-empty results
            # Iterate through the API results, one per ad, and append to list
            for j in range(0, len(results[i]["results"]["ad_creatives"])):
                if results[i]["results"]["ad_creatives"][j]["format"] == "video":
                    pass
                else:
                    image_dictionary = {
                        "id": results[i]["advertiser_id"],
                        "image": results[i]["results"]["ad_creatives"][j]["image"],
                        "format": results[i]["results"]["ad_creatives"][j]["format"],
                    }
                    image_list.append(image_dictionary)

    return image_list


if __name__ == "__main__":
    collect_save_ads(filename=f"results_{datetime.now().strftime('%Y-%m-%d')}")

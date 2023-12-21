"""
Flow to enrich tweets by:
    - concatenating .json files per twitter account into one json;
    - creating a media table with image URLs and a media id key;
    - creating a core table with all tweets, tweet ids, media ids and their public metrics;
    - saving twitter images to S3.

if you want to test the flow:
python ds_digital_ads/pipeline/enrich_tweets_flow.py run

if you want to run the flow in production:
python ds_digital_ads/pipeline/enrich_tweets_flow.py run --production True
"""
from metaflow import FlowSpec, step, Parameter

import pandas as pd
import requests
import os

from typing import List

from ds_digital_ads import BUCKET_NAME
from ds_digital_ads.utils.data_collection_utils import PROCESSED_DATA_COLLECTION_FOLDER
from ds_digital_ads.getters.data_getters import save_to_s3, save_images_to_s3


class EnrichTweetsFlow(FlowSpec):
    production = Parameter("production", help="Run in production?", default=False)

    @step
    def start(self):
        """
        Start of flow.
        """
        self.next(self.load_data)

    @step
    def load_data(self):
        """
        Loads and concatenates collected tweets from S3.
        """
        from ds_digital_ads.getters.data_getters import load_s3_data, get_s3_data_paths
        from ds_digital_ads.utils.data_collection_utils import (
            RAW_DATA_COLLECTION_FOLDER,
        )

        raw_tweet_files = get_s3_data_paths(
            BUCKET_NAME, RAW_DATA_COLLECTION_FOLDER, file_types=["*.json"]
        )

        raw_tweet_files = raw_tweet_files if self.production else raw_tweet_files[:5]

        all_tweets_dfs = []
        self.media_data = []
        self.all_tweets = {}
        for tweet_file in raw_tweet_files:
            if "production_true" in tweet_file:
                tweets = load_s3_data(BUCKET_NAME, tweet_file)
                name = tweet_file.split("/")[-1].split("_")[2]
                tweet_df = pd.DataFrame(tweets["data"])
                tweet_df["name"] = name

                self.media_data.extend(tweets["includes"]["media"])
                all_tweets_dfs.append(tweet_df)
                self.all_tweets[name] = tweets

        self.all_tweets_df = pd.concat(all_tweets_dfs)

        self.next(self.clean_media_data)

    @step
    def clean_media_data(self):
        """
        clean and create media dataframe from raw data.
        """
        self.media_df = pd.DataFrame(self.media_data)
        self.media_df["public_metrics"] = self.media_df["public_metrics"].apply(
            lambda x: x["view_count"] if isinstance(x, dict) else None
        )
        self.media_df["url"].fillna(self.media_df["preview_image_url"], inplace=True)
        self.media_df.drop(columns=["preview_image_url"], inplace=True)
        self.media_df["image_name"] = self.media_df["url"].apply(
            lambda x: x.split("/")[-1]
        )
        self.media_df.rename(columns={"media_key": "media_id"}, inplace=True)

        self.next(self.clean_core_data)

    @step
    def clean_core_data(self):
        """
        Clean up core dataframe.
        """
        # clean up core dataframe
        self.all_tweets_df = self.all_tweets_df.assign(
            created_at=lambda x: pd.to_datetime(x["created_at"])
        )
        self.all_tweets_df["media_id"] = self.all_tweets_df["attachments"].apply(
            lambda x: x["media_keys"] if isinstance(x, dict) else None
        )

        # add public metrics to the beginning of the df
        public_metrics_df = (
            self.all_tweets_df["public_metrics"]
            .apply(pd.Series)
            .add_prefix("public_metrics_")
        )
        self.all_tweets_df = pd.concat([self.all_tweets_df, public_metrics_df], axis=1)
        self.all_tweets_df.drop(columns=["public_metrics", "attachments"], inplace=True)

        self.all_tweets_df = (
            self.all_tweets_df.assign(
                hashtags=lambda x: x["entities"].apply(
                    lambda y: [tag["tag"] for tag in y.get("hashtags", [])]
                ),
                url_titles=lambda x: x["entities"].apply(
                    lambda y: [url.get("title") for url in y.get("urls", [])]
                ),
                url_descriptions=lambda x: x["entities"].apply(
                    lambda y: [url.get("description") for url in y.get("urls", [])]
                ),
                mentions=lambda x: x["entities"].apply(
                    lambda y: [mention["username"] for mention in y.get("mentions", [])]
                ),
            )
            .drop(columns=["entities"])[
                [
                    "id",
                    "media_id",
                    "name",
                    "created_at",
                    "lang",
                    "text",
                    "public_metrics_retweet_count",
                    "public_metrics_reply_count",
                    "public_metrics_like_count",
                    "public_metrics_quote_count",
                    "public_metrics_bookmark_count",
                    "public_metrics_impression_count",
                    "hashtags",
                    "url_titles",
                    "url_descriptions",
                    "mentions",
                ]
            ]
            .explode("media_id")
            .reset_index(drop=True)
        )

        self.next(self.save_data)

    @step
    def save_data(self):
        """
        Save dataset to s3.
        """
        from datetime import datetime

        date = datetime.now().strftime("%Y-%m-%d").replace("-", "")

        print("saving media table...")

        media_path = os.path.join(
            PROCESSED_DATA_COLLECTION_FOLDER,
            f"media_table_production_{str(self.production).lower()}_{date}.csv",
        )
        save_to_s3(BUCKET_NAME, self.media_df, media_path)

        print("saving core table...")

        core_path = os.path.join(
            PROCESSED_DATA_COLLECTION_FOLDER,
            f"core_table_{str(self.production).lower()}_{date}.csv",
        )
        save_to_s3(BUCKET_NAME, self.all_tweets_df, core_path)

        print("saving concatenated tweets...")
        core_concat_path = os.path.join(
            PROCESSED_DATA_COLLECTION_FOLDER,
            f"all_tweets_{str(self.production).lower()}_{date}.json",
        )
        save_to_s3(BUCKET_NAME, self.all_tweets, core_concat_path)

        print("saving images...")
        image_list = self.media_df["url"].unique().tolist()
        save_images_to_s3(
            image_urls=image_list, output_folder=PROCESSED_DATA_COLLECTION_FOLDER
        )

        self.next(self.end)

    @step
    def end(self):
        """
        Ends the flow.
        """
        pass


if __name__ == "__main__":
    EnrichTweetsFlow()

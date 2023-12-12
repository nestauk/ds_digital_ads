## Twitter Data Collection

You will need to add a BEARER_TOKEN to your environment variables. You can get one by creating a twitter developer account and creating an app. See [here](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api) for more details.

To add a BEARER_TOKEN to an `.env` file at the root of your directory, run the following command in your terminal:

```
echo "BEARER_TOKEN=your_actual_token" >> ds_digital_ads/.env
```

To collect tweets from a defined list of gambling related twitter accounts in the last seven days, run the following command:

```
python ds_digital_ads/pipeline/collect_tweets_flow.py run --production False
```

To clean the raw collected tweets by: 
- concatenating .json files per twitter account into one main json;
- creating a media table with image URLs and a media id key;
- creating a core table with all tweets, tweet ids, media ids and their public metrics;
- saving twitter images to S3.

run the following command:

```
python ds_digital_ads/pipeline/clean_tweets_flow.py run --production False
```

If you would like to run the above commands in production, change the `--production` flag to `True`.

## Twitter Data Collection

To collect tweets from a defined list of gambling related accounts in the last seven days, run the following command:

```
python ds_digital_ads/pipeline/collect_tweets_flow.py --production True
```

To clean the raw collected data, run the following command:

```
python ds_digital_ads/pipeline/clean_tweets_flow.py --production True
```

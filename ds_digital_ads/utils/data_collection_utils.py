"""
Utils for twitter data collection and enrichment
"""
cols_to_explode = ["attachments", "public_metrics", "referenced_tweets"]

TWITTER_HANDLES = [
    "betway",
    "TNLUK",
    "paddypower",
    "PokerStars",
    "Betfred",
    "foxybingo",
    "GalaBingo",
    "888casino",
    "skyvegas",
    "WillHillVegas",
    "Coral",
    "Ladbrokes",
]

# ENDPOINT_URL = "https://api.twitter.com/2/tweets/search/all" # we don't have access to this end point
ENDPOINT_URL = "https://api.twitter.com/2/tweets/search/recent"

RAW_DATA_COLLECTION_FOLDER = "data_collection/gambling_tweets/raw/"
PROCESSED_DATA_COLLECTION_FOLDER = "data_collection/gambling_tweets/processed/"

query_parameters_twitter = {
    "tweet.fields": "id,text,author_id,attachments,conversation_id,created_at,lang,entities,geo,public_metrics,referenced_tweets",
    "user.fields": "id,name,username,created_at,description,location,verified,public_metrics,entities,url",
    "place.fields": "id,country,country_code,name,full_name,geo,place_type,contained_within",
    "media.fields": "media_key,type,url,duration_ms,preview_image_url,public_metrics,alt_text",
    "expansions": "author_id,geo.place_id,attachments.media_keys",
    "max_results": 5,  # adjust as needed
}

# Define the rules for the query
digital_ads_ruleset_twitter = [
    {"value": f"from:{handle} -is:retweet has:media", "tag": f"{handle}_promotions"}
    for handle in TWITTER_HANDLES
]

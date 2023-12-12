"""
Utils for twitter data collection
"""
import re

# -----TWITTER DATA COLLECTION-----

ENDPOINT_URL = "https://api.twitter.com/2/tweets/search/recent"

DATA_COLLECTION_FOLDER = "data_collection/recent_search_twitter/"

digital_ads_ruleset_twitter = [
    {"value": "from:betway promotion", "tag": "betway_promotions"}
]


query_parameters_twitter = {
    "tweet.fields": "id,text,author_id,edit_history_tweet_ids,attachments,conversation_id,created_at,lang,context_annotations,entities,geo,public_metrics,source,in_reply_to_user_id,possibly_sensitive,referenced_tweets,reply_settings",
    "user.fields": "id,name,username,created_at,description,location,verified,public_metrics,entities,url",
    "place.fields": "id,country,country_code,name,full_name,geo,place_type,contained_within",
    "media.fields": "media_key,type,url,duration_ms,preview_image_url,public_metrics,alt_text",
    "expansions": "author_id,geo.place_id,attachments.media_keys",
    "max_results": 10,  # number of results per page when making a call to the API
}

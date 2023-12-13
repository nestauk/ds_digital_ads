"""
Utils for data collection and enrichment"""

# Dictionary containing gambling advertisers- {parent_company: {google_id: google ad id, twitter_handle: list of handles, brand: list of brands}}

gambling_advertisers_uk = {
    "betdaq": {
        "google_id": "AR06886900069665079297",
        "twitter_handle": ["BETDAQ"],
        "brand": ["betdaq"],
    },
    "newcote": {
        "google_id": "AR11315017811471892481",
        "twitter_handle": ["BetVictor", "ParimatchUK", "HeartBingo"],
        "brand": ["betvictor", "parimatch", "heart_bingo"],
    },
    "betway": {
        "google_id": "AR06681372621692469249",
        "twitter_handle": ["betway"],
        "brand": ["betway"],
    },
    "hestview": {
        "google_id": "AR03855649178784890881",
        "twitter_handle": ["SkyBet"],
        "brand": ["sky_bet"],
    },
    "hillside": {
        "google_id": "AR14088544963507781633",
        "twitter_handle": ["bet365"],
        "brand": ["bet365"],
    },
    "petfre": {
        "google_id": "AR17559410289986764801",
        "twitter_handle": ["betfred", "BuckyBingo"],
        "brand": ["betfred", "bucky_bingo"],
    },
    "lc": {
        "google_id": "AR15766602704828235777",
        "twitter_handle": ["foxybingo", "GalaBingo", "Coral", "Ladbrokes"],
        "brand": [
            "ladbrokes",
            "coral",
            "foxy_games",
            "foxy_bingo",
            "gala_spins",
            "gala_bingo",
        ],
    },
    "power_leisure": {
        "google_id": "AR13484353978196033537",
        "twitter_handle": ["paddypower", "Betfair"],
        "brand": ["paddy_power", "betfair"],
    },
    "camelot": {
        "google_id": None,
        "twitter_handle": "TNLUK",
        "brand": ["national_lottery"],
    },
    "tsg": {
        "google_id": "AR16548216246517104641",
        "twitter_handle": "PokerStars",
        "brand": ["pokerstars"],
    },
    "888_holdings": {
        "google_id": "AR04559231342323171329",
        "twitter_handle": [
            "888casino",
            "888poker",
            "888sport",
            "777Casino1",
            "WilliamHill",
            "WillHillVegas",
        ],
        "brand": ["888_Casino", "888_Poker", "888_Sport", "777_Casino", "William_Hill"],
    },
    "bonne_terre": {
        "google_id": "AR07879625822481416193",
        "twitter_handle": ["sky_vegas", "SkyCasino", "skybingo"],
        "brand": [
            "sky_vegas",
            "sky_casino",
            "sky_bingo",
        ],
    },
}


"""
Utils for twitter data collection and enrichment
"""
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


"""
Utils for google ads data collection and enrichment
"""

import crawler_utils
import time
import datetime
import tweepy

# Twitter Credentials
consumer_key, consumer_secret, access_token, access_token_secret = crawler_utils.loadAccessData("twitter")

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

last_retrieved_id = crawler_utils.loadLastId("twitter")
client = crawler_utils.connect()

while(True):
    print("- Crawling initialized. Collecting Tweets ...")
    tweet_ids = []
    tweet_count = 0
    user_count = 0
    lang_count = 0
    for item in crawler_utils.crawlTwitter(api, "servicenow -filter:retweets", last_retrieved_id):
        if not(bool(client.twitter.tweets.find_one({"id":item.id}))):
            tweet_count += 1
            tweet_ids.append(item.id)

            tweet = {}
            tweet["id"] = item.id
            tweet["text"] = item.text
            tweet["lang"] = item.lang
            tweet["created_at"] = str(item.created_at)
            tweet["user_location"] = item.user.location
            tweet["user"] = item.user.id
            tweet["processed"] = 0
            tweet["category"] = "N/A"

            client.twitter.tweets.insert_one(tweet)
        
        if not(bool(client.twitter.users.find_one({"id":item.user.id}))):
            user = {}
            user["id"] = item.user.id
            user["description"] = item.user.description
            user["followers"] = item.user.followers_count
            user["friends"] = item.user.friends_count
            user["tweets_count"] = 0
            
            client.twitter.users.insert_one(user)
            user_count += 1
        
        if not(bool(client.twitter.languages.find_one({"iso_short":item.lang}))):
            language = {}
            language["iso_short"] = item.lang
            language["tweets_count"] = 0
            
            client.twitter.languages.insert_one(language)
            lang_count += 1
            
        client.twitter.languages.update_one({"iso_short":item.lang}, {"$inc": {"tweets_count":1} } )
        client.twitter.users.update_one({"id":item.user.id}, {"$inc": {"tweets_count":1} } )

    if (len(tweet_ids) > 0):
        last_retrieved_id = max(tweet_ids)
        crawler_utils.writeCrawlerLog("twitter", {"last_retrieved_id":last_retrieved_id})
        
        logText = "INFO: " + str(tweet_count) + " new Tweets, " + str(user_count) + " new Users, and " + str(lang_count) + " new Languages. Last retrieved Tweet's id: " + str(last_retrieved_id)
        log = {}
        log["message"] = logText
        log["created_at"] = str(datetime.datetime.now())

        client.twitter.log.insert_one(log)
    else:
        logText = "INFO: No new Tweets founded."
        log = {}
        log["message"] = logText
        log["created_at"] = str(datetime.datetime.now())

        client.twitter.log.insert_one(log)

    print("-- Finished and waiting for 5 minutes.")
    time.sleep(300)
    
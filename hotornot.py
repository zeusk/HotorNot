#!/usr/bin/env python2

import os, sys, string, time, datetime, re, requests, json, urllib, urllib2, base64, pymongo
from multiprocessing import Pool, Lock, Queue, Manager

def main(search_term, search_type, num_tweets):
    credentials = get_credentials()
    auth = oauth(credentials)
    enriched_tweets = enrich(credentials, dedup(search(search_term, search_type, num_tweets, auth)), sentiment_target = search_term)
    store(enriched_tweets)
    print_results()
    plot_results()
    return

def plot_results():
    l_client = pymongo.MongoClient()
    l_allTweets = l_client.twitter_db.tweets.find()
    l_posTweets = l_client.twitter_db.tweets.find({"sentiment" : "positive"})
    l_negTweets = l_client.twitter_db.tweets.find({"sentiment" : "negative"})
    l_neuTweets = l_client.twitter_db.tweets.find({"sentiment" : "neutral"})
    l_pos_score_file = open("scores.pos", "w")
    l_pos_time_file  = open("times.pos", "w")
    for l_tweet in l_posTweets:
        l_dateString = l_tweet['time'].replace("+0000 ", "")
        l_dateFormat = "%a %b %d %H:%M:%S %Y"
        l_outputDateString = datetime.datetime.strptime(l_dateString, l_dateFormat).strftime("%Y-%m-%d %H:%M:%S +0000")
        l_pos_score_file.write("%.4f\n" % l_tweet['score'])
        l_pos_time_file.write("%s\n" % l_outputDateString)
    l_pos_score_file.close()
    l_pos_time_file.close()
    l_neg_score_file = open("scores.neg", "w")
    l_neg_time_file  = open("times.neg", "w")
    for l_tweet in l_negTweets:
        l_dateString = l_tweet['time'].replace("+0000 ", "")
        l_dateFormat = "%a %b %d %H:%M:%S %Y"
        l_outputDateString = datetime.datetime.strptime(l_dateString, l_dateFormat).strftime("%Y-%m-%d %H:%M:%S +0000")
        l_neg_score_file.write("%.4f\n" % l_tweet['score'])
        l_neg_time_file.write("%s\n" % l_outputDateString)
    l_neg_score_file.close()
    l_neg_time_file.close()
    os.system('R --quiet --vanilla < plot.R 2>&1 >/dev/null')
    os.system('rm -f *.pos *.neg *raw*')

def get_credentials():
    creds = {}
    import credentials
    creds['consumer_key'] = credentials.twitter_consumer_key
    creds['consumer_secret'] = credentials.twitter_consumer_secret
    creds['apikey'] = credentials.alchemy_apikey 
    return creds

def oauth(credentials):
    print "Requesting token from Twitter API"
    try:
        encoded_credentials = base64.b64encode(credentials['consumer_key'] + ':' + credentials['consumer_secret'])        
        post_url = "https://api.twitter.com/oauth2/token"
        parameters = {'grant_type' : 'client_credentials'}
        auth_headers = {
            "Authorization" : "Basic %s" % encoded_credentials,
            "Content-Type"  : "application/x-www-form-urlencoded;charset=UTF-8"
            }
        results = requests.post(url=post_url, data=urllib.urlencode(parameters), headers=auth_headers)
        response = results.json()
        auth = {}
        auth['access_token'] = response['access_token']
        auth['token_type'] = response['token_type']
        print "Token received"
        return auth
    except Exception as e:
        print "Failed to authenticate with Twitter credentials:", e
        sys.exit()

def search(search_term, search_type, num_tweets, auth):
    collection = []
    search_term = search_term + search_type + ' filter:safe'
    url = "https://api.twitter.com/1.1/search/tweets.json"
    search_headers = {
        "Authorization" : "Bearer %s" % auth['access_token']
        }
    max_count = 100
    next_results = ''
    while True:
        print "Search iteration, Tweet collection size: %d" % len(collection)
        count = min(max_count, int(num_tweets)-len(collection))
        if next_results:
            get_url = url + next_results
        else:
            parameters = {
                'q' : search_term,
                'count' : count,
                'lang' : 'en'
                } 
            get_url = url + '?' + urllib.urlencode(parameters)
        results = requests.get(url=get_url, headers=search_headers)
        response = results.json()
        for status in response['statuses']:
            text = status['text'].encode('utf-8')
            if status['retweeted'] == True:
                continue
            if text[:3] == 'RT ':
                continue
            tweet = {}
            tweet['text'] = text
            tweet['id'] = status['id']
            tweet['time'] = status['created_at'].encode('utf-8')
            tweet['screen_name'] = status['user']['screen_name'].encode('utf-8')
            collection += [tweet]
            if len(collection) >= num_tweets:
                print "Search complete! Found %d tweets" % len(collection)
                return collection
        if 'next_results' in response['search_metadata']:
            next_results = response['search_metadata']['next_results']
        else:
            print "Uh-oh! Twitter has dried up. Only collected %d Tweets (requested %d)" % (len(collection), num_tweets)
            print "Last successful Twitter API call: %s" % get_url
            print "HTTP Status:", results.status_code, results.reason
            return collection

def enrich(credentials, tweets, sentiment_target = ''):
    apikey = credentials['apikey']
    pool = Pool(processes = 10)
    mgr = Manager()
    result_queue = mgr.Queue()
    for tweet in tweets:
        pool.apply_async(get_text_sentiment, (apikey, tweet, sentiment_target, result_queue))
    pool.close()
    pool.join()
    collection = []
    while not result_queue.empty():
        collection += [result_queue.get()]
    return collection

def get_text_sentiment(apikey, tweet, target, output):
    alchemy_url = "http://access.alchemyapi.com/calls/text/TextGetTextSentiment"
    parameters = {
        "apikey" : apikey,
        "text"   : tweet['text'],
        "outputMode" : "json",
        "showSourceText" : 1
        }
    try:
        results = requests.get(url=alchemy_url, params=urllib.urlencode(parameters))
        response = results.json()
    except Exception as e:
        print "Error while calling TextGetTargetedSentiment on Tweet (ID %s)" % tweet['id']
        print "Error:", e
        return
    try:
        if 'OK' != response['status'] or 'docSentiment' not in response:
            print "Problem finding 'docSentiment' in HTTP response from AlchemyAPI"
            print response
            print "HTTP Status:", results.status_code, results.reason
            print "--"
            return
        tweet['sentiment'] = response['docSentiment']['type']
        tweet['score'] = 0.
        if tweet['sentiment'] in ('positive', 'negative'):
            tweet['score'] = float(response['docSentiment']['score'])
        output.put(tweet)
    except Exception as e:
        print "D'oh! There was an error enriching Tweet (ID %s)" % tweet['id']
        print "Error:", e
        print "Request:", results.url
        print "Response:", response
    return

def dedup(tweets):
    used_ids = []
    collection = []
    for tweet in tweets:
        if tweet['id'] not in used_ids:
            used_ids += [tweet['id']]
            collection += [tweet]
    print "After de-duplication, %d tweets" % len(collection)
    return collection

def store(tweets):
    mongo_client = pymongo.MongoClient()
    db = mongo_client.twitter_db
    db_tweets = db.tweets
    db_tweets.drop()
    for tweet in tweets:
        db_id = db_tweets.insert(tweet)
    db_count = db_tweets.count()    
    return

def print_results():
    print ''
    print 'Insights:'
    print ''
    db = pymongo.MongoClient().twitter_db
    tweets = db.tweets
    num_positive_tweets = tweets.find({"sentiment" : "positive"}).count()
    num_negative_tweets = tweets.find({"sentiment" : "negative"}).count()
    num_neutral_tweets = tweets.find({"sentiment" : "neutral"}).count()
    num_tweets = tweets.find().count()
    if num_tweets != sum((num_positive_tweets, num_negative_tweets, num_neutral_tweets)):
        print "Counting problem!"
        print "Number of tweets (%d) doesn't add up (%d, %d, %d)" % (num_tweets, 
                                                                     num_positive_tweets, 
                                                                     num_negative_tweets, 
                                                                     num_neutral_tweets)
        sys.exit()
    most_positive_tweet = tweets.find_one({"sentiment" : "positive"}, sort=[("score", -1)])
    most_negative_tweet = tweets.find_one({"sentiment" : "negative"}, sort=[("score", 1)])
    mean_results = list(tweets.aggregate([{"$group" : {"_id": "$sentiment", "avgScore" : { "$avg" : "$score"}}}]))
    if (len(mean_results) < 2):
        avg_pos_score = 0
    else:
        avg_pos_score = mean_results[1]['avgScore']
    if (len(mean_results) < 1):
        avg_neg_score = 0
    else:
        avg_neg_score = mean_results[0]['avgScore']
    print "Number (%%) of positive tweets: %d (%.2f%%)" % (num_positive_tweets, 100*float(num_positive_tweets) / num_tweets)
    print "Number (%%) of negative tweets: %d (%.2f%%)" % (num_negative_tweets, 100*float(num_negative_tweets) / num_tweets)
    print "Number (%%) of neutral tweets:  %d (%.2f%%)" % (num_neutral_tweets, 100*float(num_neutral_tweets) / num_tweets)
    print ""
    print "AVERAGE POSITIVE TWEET SCORE: %f" % float(avg_pos_score)
    print "AVERAGE NEGATIVE TWEET SCORE: %f" % float(avg_neg_score)
    print ""
    if most_positive_tweet is not None:
        print "MOST POSITIVE TWEET"
        print "Text: %s" % most_positive_tweet['text']
        print "User: %s" % most_positive_tweet['screen_name']
        print "Time: %s" % most_positive_tweet['time']
        print "Score: %f" % float(most_positive_tweet['score'])
        print ""
    if most_negative_tweet is not None:
        print "MOST NEGATIVE TWEET"
        print "Text: %s" % most_negative_tweet['text']
        print "User: %s" % most_negative_tweet['screen_name']
        print "Time: %s" % most_negative_tweet['time']
        print "Score: %f" % float(most_negative_tweet['score'])
    print ""
    print "Positive confidence: %f%%" % (100*float(num_positive_tweets+num_neutral_tweets)/num_tweets)
    print "Negative confidence: %f%%" % (100*float(num_negative_tweets+num_neutral_tweets)/num_tweets)
    return

if __name__ == "__main__":
    if not len(sys.argv) == 4 and not len(sys.argv) == 3:
        print "Usage: ./hotornot.py <SEARCH_TERM> <NUMB_TWEETS>"
        print "\t<SEARCH_TERM> : the stock to be used when searching for Tweets"
        print "\t<SEARCH_TYPE> : the class of search to be performed, ie: stock, generic etc"
        print "\t<NUMB_TWEETS> : the preferred number of Tweets to pull from Twitter's API"
        sys.exit()
    elif not len(sys.argv) == 4:
    	main(sys.argv[1], '', int(sys.argv[2]))
    else:
        main(sys.argv[1], ' ' + sys.argv[2], int(sys.argv[3]))


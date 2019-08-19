import datetime
from pymongo import MongoClient
import twitter
import urllib.parse



# loading twitter tokens from a file
def load_tokens(path):
    with open(path, 'r') as f:
 #   f = file(path, 'r')
        for line in f.readlines():
            if line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split(",")]
            (consumer_key, consumer_secret, auth_key, auth_secret) = parts
            tokens = dict()
            tokens["CLIENT_KEY"] = consumer_key
            tokens["CLIENT_SECRET"] = consumer_secret
            tokens["ATOKEN_KEY"] = auth_key
            tokens["ATOKEN_SECRET"] = auth_secret
            break  # assume first token line needed

        return tokens

# downloading new tweets from user's timeline - in blocks of up to 200
def get_user_tweets(api, user, s_id, m_id):
    end = False
    statuses = api.GetUserTimeline(screen_name=user, since_id=s_id, max_id=m_id, count=200)
    if len(statuses) == 0:
        end = True
    else:
        print('\t%s has %i new tweets' % (user, len(statuses)))
    return end,statuses

# downloading replies to a user, twitter api does not allow downloading replies to a specific tweet - in blocks of up to 200
def get_replies_to_user(api, user, s_id, m_id):
    end = False
    if m_id is not None:
        q = urllib.parse.urlencode({"q": "to:%s since_id:%s max_id:%s " % (user, s_id, m_id)})
    else:
        q = urllib.parse.urlencode({"q": "to:%s since_id:%s" % (user, s_id)})
    print(q, s_id, m_id)
    statuses = api.GetSearch(raw_query=q, since_id=s_id, max_id=m_id, count=200)
    if len(statuses) == 0:
        end = True
    else:
        print('\t%s has %i new replies' % (user, len(statuses)))
    return end,statuses

# loads user screen names from a file, retrieves last user's tweet id and last reply to user's tweet available in database
def load_users(user_file, tweetsDB):
    user_id2last_tweet_id = {}
    user_id2last_reply_id = {}
    with open(user_file, 'r') as f:
        for line in f:
            spl = line.strip().split("\t")
            user_id = spl[0]
            user_id2last_tweet_id[user_id] = "1"
            user_id2last_reply_id[user_id] = "1"

            try:
                last_tweet = tweetsDB.find({"user.screen_name": user_id}).sort("created_at_mongo", -1).limit(1)
                if last_tweet.count_documents(True) == 1:
                    user_id2last_tweet_id[user_id] = last_tweet[0]['id_str']
                last_reply = tweetsDB.find({"in_reply_to_screen_name":user_id}).sort("created_at_mongo", -1).limit(1)
                if last_reply.count_documents(True) == 1:
                    user_id2last_reply_id[user_id] = last_reply[0]['id_str']
            except Exception as ex:
                print (ex)
                print ("Unable to find last downloaded tweet id or reply id for user", user_id)

    return user_id2last_tweet_id, user_id2last_reply_id

# downloading tweets for a user - starting with tweet with a given id
def getAllTweetsForUser(api, user_id, newest_sid):
    end, tweets = get_user_tweets(api, user_id, newest_sid, None)
    tlist = tweets
    oldest_current_id = "1"
    if len(tweets)>0:
        oldest_current_id = tweets[len(tweets) - 1].id
    if not end:
        while int(oldest_current_id) > int(newest_sid):
            end, tweets = get_user_tweets(api, user_id, newest_sid, int(oldest_current_id) - 1)
            if end:
                break
            print(len(tweets), len(tlist))
            oldest_current_id = tweets[len(tweets) - 1].id
            tlist += tweets
        print(len(tlist))
    return tlist

# downloading replies for a given user - starting with tweet with a given id
def getAllRepliesForUser(api, user_id, newest_sid):
    print("doing replies now")
    end, replies = get_replies_to_user(api, user_id, newest_sid, None)
    rlist = replies
    oldest_reply_id = "1"
    if len(replies)>0:
        oldest_reply_id = replies[len(replies) - 1].id
    print(len(rlist))
    if not end:
        while int(oldest_reply_id) > int(newest_sid):
            end, replies = get_replies_to_user(api, user_id, newest_sid, int(oldest_reply_id) - 1)
            if end:
                break
            oldest_reply_id = replies[len(replies) - 1].id
            rlist += replies
        print(len(rlist))
    return rlist

# saving downloaded tweets in mongo database
def saveTweets(tlist, tweetsDB, not_saved_in_db, exceptions):
    tlist.reverse()
    print(len(tlist))
    for tweet in tlist:
        try:
            created_at = tweet.created_at
            dt = datetime.datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
            json_tweet = tweet.AsDict()
            json_tweet['created_at_mongo'] = dt

            tweetsDB.insert_one(json_tweet)
        except Exception as ex:
            not_saved_in_db += 1
            exceptions.append(ex)


def main():
    # change in case your file with tokens has a different name
    tokens = load_tokens("twittertokens.txt")

    api = twitter.Api(
        consumer_key=tokens["CLIENT_KEY"],
        consumer_secret=tokens["CLIENT_SECRET"],
        access_token_key=tokens["ATOKEN_KEY"],
        access_token_secret=tokens["ATOKEN_SECRET"],
        sleep_on_rate_limit=True
    )

    client = MongoClient()
    # rename database and collection names in line below
    tweetsDB = client["test"]["test"]
    # change in case your file with user screen names has a different name
    user_id2last_tweet_id, user_id2last_reply_id = load_users("user_list.txt", tweetsDB)
    start = datetime.datetime.now()
    print(start, "Starting collecting tweets")
    i = 1
    total_new = 0
    not_saved_in_db = 0
    exceptions = []
    for user_id in user_id2last_tweet_id:
        print("User", i, "out of", len(user_id2last_tweet_id), ". User id:", user_id)
        i += 1

        newest_saved_sid = user_id2last_tweet_id[user_id]
        newest_saved_reply_sid = user_id2last_reply_id[user_id]
        print("newest post", newest_saved_sid)
        print("newest reply", newest_saved_reply_sid)

        tlist = getAllTweetsForUser(api, user_id, newest_saved_sid)
        rlist = getAllRepliesForUser(api, user_id, newest_saved_reply_sid)

        # process the tweets we got in reverse order so that we maintain the #order of timestamps

        saveTweets(tlist, tweetsDB, not_saved_in_db, exceptions)
        saveTweets(rlist, tweetsDB, not_saved_in_db, exceptions)

        total_new += len(tlist)
        total_new += len(rlist)

    end = datetime.datetime.now()
    print(end, "Done collecting", total_new, "tweets.")
    print("Took:", end - start)
    print(not_saved_in_db, "not saved in db:")
    for ex in exceptions:
        print(ex)

main()



from pymongo import MongoClient
import sys

tweets=[]

client = MongoClient()

# change here if you have other names for database and collection in your mongodb
tweetsDB = client["tweets"]["threads"]

def find_tweet(id, tweetsDB):
    idt=str(id)	
    try:
        tweet = tweetsDB.find_one({"id_str": idt})
    except Exception as ex:
        print (ex)
        print ("Unable to find last downloaded tweet id or reply id for user", user_id)

    return tweet



nextID=sys.argv[1]
while nextID:
    tweet = find_tweet(nextID,tweetsDB)
    if tweet is None:
        # can also be triggered if database is incomplete - make sure you have downloaded all tweets needed
        tweets.append("---tweet has been deleted---")
        nextID=''

    else:    
        tweets.append(tweet)
        if "in_reply_to_status_id" in tweet.keys():
            try:
                nextID = tweet["in_reply_to_status_id"]["$numberLong"]
                
            except:
                nextID = tweet["in_reply_to_status_id"]
                
        else:
            # this is root
            nextID=''
            
# we print ids from root to leaf        
for i in reversed(tweets):
    print(i)

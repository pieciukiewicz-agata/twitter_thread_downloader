import sys
import json


#### first and only argument must be json file with database dump

### function returns ids of tweets for which replies exist in our database dump
def list_ancestors(in_file):
        with open(in_file, 'r') as f:
                for line in f.readlines():
                        try:
                                y = json.loads(line)
                                # tweet is a reply to another tweet        
                                if "in_reply_to_status_id" in y.keys():

                                        try:
                                                print(y["in_reply_to_status_id"]["$numberLong"])
                                        except:
                                                print(y["in_reply_to_status_id"])

                        except:
                                # in order to handle ex. deleted tweets
                                pass
                        




if ((len(sys.argv) < 2)):
        print("Wrong number of parameters. First argument is json file with database dump.")
        sys.exit(1)
l = list_ancestors(sys.argv[1])


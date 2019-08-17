import sys
import json


#### first and only argument must be json file with database dump

### function returns ids of tweets which are replies in our database dump

def list_non_root(in_file):
        with open(in_file, 'r') as f:
                for line in f.readlines():
                        try:
                                y = json.loads(line)
                                # tweet is a reply to another tweet
                                if "in_reply_to_status_id" in y.keys():

                                        try: 
                                                idd=(y["id"]["$numberLong"])

                                        except:
                                                idd=(y["id"])

                                        print(str(idd))
                        except:
                                # in order to handle ex. deleted tweets
                                pass





if ((len(sys.argv) < 2)):
        print("Wrong number of parameters. First argument is json file with database dump.")
        sys.exit(1)
l = list_non_root(sys.argv[1])



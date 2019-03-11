import json
from tqdm import tqdm
import numpy as np
import pandas as pd
from SQLite_handler import find_tweet

class Who_is_fake(object):
    def __init__(self):
        self.NEW_HOST_1 = {}
        # for k, v in json.load(open("data/sources.json")).items():
        #     hostname = k.lower()
        #     _type = v["type"]
        #     if _type in ["fake", "conspiracy", "hate"]:
        #         self.NEW_HOST_1[hostname] = "FAKE"
        #     elif _type == "bias":
        #         self.NEW_HOST_1[hostname] = "BIAS"

        # self.NEW_HOST_2 = {k.lower(): v for k, v in json.load(open("data/mbfc_host_label.json")).items()}
        self.NEW_HOST_2 = {k.lower(): v for k, v in json.load(open("data/mbfc_dict.json")).items()}

        self.HOST = {
                "thegatewaypundit.com": 0,
                "truthfeed.com": 0,
                "infowars.com": 0,
                "therealstrategy.com": 0,
                "conservativetribune.com": 0,
                "zerohedge.com": 0,
                "rickwells.us": 0,
                "departed.co": 0,
                "thepoliticalinsider.com": 0,
                "therightscoop.com": 0,
                "teaparty.org": 0,
                "usapoliticsnow.com": 0,
                "clashdaily.com": 0,
                "thefederalistpapers.org": 0,
                "redflagnews.com": 0,
                "thetruthdivision.com": 0,
                "breitbart.com": 1,
                "dailycaller.com": 1,
                "americanthinker.com": 1,
                "wnd.com": 1,
                "freebeacon.com": 1,
                "newsninja2012.com": 1,
                "hannity.com": 1,
                "newsmax.com": 1,
                "endingthefed.com": 1,
                "truepundit.com": 1,
                "westernjournalism.com": 1,
                "dailywire.com": 1,
                "newsbusters.org": 1,
                "ilovemyfreedom.org": 1,
                "100percentfedup.com": 1,
                "pjmedia.com": 1,
                "weaselzippers.us": 1,
                "foxnews.com": 2,
                "dailymail.co.uk": 2,
                "washingtonexaminer.com": 2,
                "nypost.com": 2,
                "bizpacreview.com": 2,
                "nationalreview.com": 2,
                "lifezette.com": 2,
                "redstate.com": 2,
                "allenbwest.com": 2,
                "theconservativetreehouse.com": 2,
                "townhall.com": 2,
                "investors.com": 2,
                "theblaze.com": 2,
                "theamericanmirror.com": 2,
                "ijr.com": 2,
                "judicialwatch.org": 2,
                "thefederalist.com": 2,
                "hotair.com": 2,
                "conservativereview.com": 2,
                "weeklystandard.com": 2,
                "wsj.com": 3,
                "washingtontimes.com": 3,
                "rt.com": 3,
                "realclearpolitics.com": 3,
                "telegraph.co.uk": 3,
                "forbes.com": 3,
                "fortune.com": 3,
                "cnn.com": 4,
                "thehill.com": 4,
                "politico.com": 4,
                "usatoday.com": 4,
                "reuters.com": 4,
                "bloomberg.com": 4,
                "businessinsider.com": 4,
                "apnews.com": 4,
                "observer.com": 4,
                "fivethirtyeight.com": 4,
                "bbc.com": 4,
                "ibtimes.com": 4,
                "bbc.co.uk": 4,
                "nytimes.com": 5,
                "washingtonpost.com": 5,
                "nbcnews.com": 5,
                "abcnews.go.com": 5,
                "theguardian.com": 5,
                "vox.com": 5,
                "slate.com": 5,
                "buzzfeed.com": 5,
                "cbsnews.com": 5,
                "politifact.com": 5,
                "latimes.com": 5,
                "nydailynews.com": 5,
                "theatlantic.com": 5,
                "mediaite.com": 5,
                "newsweek.com": 5,
                "npr.org": 5,
                "independent.co.uk": 5,
                "cnb.cx": 5,
                "hollywoodreporter.com": 5,
                "huffingtonpost.com": 6,
                "thedailybeast.com": 6,
                "dailykos.com": 6,
                "rawstory.com": 6,
                "politicususa.com": 6,
                "time.com": 6,
                "motherjones.com": 6,
                "talkingpointsmemo.com": 6,
                "msnbc.com": 6,
                "mashable.com": 6,
                "salon.com": 6,
                "thinkprogress.org": 6,
                "newyorker.com": 6,
                "mediamatters.org": 6,
                "nymag.com": 6,
                "theintercept.com": 6,
                "thenation.com": 6,
                "people.com": 6,
                "dailynewsbin.com": 7,
                "bipartisanreport.com": 7,
                "bluenationreview.com": 7,
                "crooksandliars.com": 7,
                "occupydemocrats.com": 7,
                "shareblue.com": 7,
                "usuncut.com": 7
            }

    def identify(self, ht):
        ht = ht.lower()

        if ht in self.HOST:
            return self.HOST[ht]
        else:
            return -1


    def identify_v2(self, ht):
        ht = ht.lower()
        # if ht in self.NEW_HOST_1:
        #     labels.append(self.NEW_HOST_1[ht])
        # else:
        #     labels.append("GOOD")

        if ht in self.NEW_HOST_2:
            bias = self.NEW_HOST_2[ht][0].lower()
            fact = self.NEW_HOST_2[ht][1].lower()
            labels = [bias, fact]
        else:
            labels = ["-1", "-1"]
        return labels

        # if ht in self.HOST:
        #     return self.HOST[ht]
        # else:
        #     return -1

    def is_fake(self, ht):
        if self.identify(ht)[0] == "FAKE":
            return True
        else:
            return False


class Are_you_IRA(object):

    def __init__(self):
        self._map = json.load(open("data/IRA_map.json"))
        # self.IRA_users_before_set = pd.read_csv("data/ira_users_csv_hashed.csv", usecols=["userid"], dtype=str)["userid"]
        self.IRA_user_set = set(json.load(open("data/IRA_user_list.json"))) # all IRA (匿名或非匿名) included

    def fuck(self, ht):
        return ht in self.IRA_user_set

    def find_IRA_tweets(self):
        # IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv",
        #                 usecols=["tweetid", "userid"], dtype=str)
        # with open("data/IRA-tweets-in-SQLite.json", "w") as f:
        #     for _, row in tqdm(IRA_info.iterrows()):
        #         tweetid = row["tweetid"]
        #         uid = row["userid"]

        #         d = find_tweet(tweetid)
        #         if d:
        #             f.write("{},{}\n".format(tweetid, uid))
        with open("data/IRA-tweets-in-SQLite-v2.json", "w") as f:
            for line in tqdm(open("data/IRA-tweets-in-SQLite.json")):
                tid, uid = line.strip().split(",")
                real_uid = str(find_tweet(tid)["user_id"])
                f.write("{},{},{}\n".format(tid, real_uid, uid))

    def find_IRA_retweets(self):
        # IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv",
        #                 usecols=["retweet_tweetid", "retweet_userid"], dtype=str)
        # with open("data/IRA-retweets-in-SQLite.json", "w") as f:
        #     for _, row in tqdm(IRA_info.iterrows()):
        #         tweetid = row["retweet_tweetid"]
        #         uid = row["retweet_userid"]
        #         if type(tweetid) == str:
        #             d = find_tweet(tweetid)
        #             if d:
        #                 f.write("{},{}\n".format(tweetid, uid))
        with open("data/IRA-retweets-in-SQLite-v2.json", "w") as f:
            for line in tqdm(open("data/IRA-retweets-in-SQLite.json")):
                tid, uid = line.strip().split(",")
                real_uid = str(find_tweet(tid)["user_id"])
                f.write("{},{},{}\n".format(tid, real_uid, uid))


    def cal_IRA_map(self):
        data = []
        for line in open("data/IRA-(re)tweets-in-SQLite.json"):
            d = json.loads(line.strip())
            data.append(d)

        """
        IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                        usecols=["tweetid", "userid", "tweet_time", "retweet_userid", "retweet_tweetid"], dtype=str)
        with open("data/IRA-(re)tweets-in-SQLite.json", "w") as f:
            for _, row in tqdm(IRA_info.iterrows()):
                tweetid = row["tweetid"]
                uid = row["userid"]
                retweet_id = row["retweet_tweetid"]

                d = find_tweet(tweetid)
                if d:
                    d["IRA_userid"] = uid
                    data.append(d)
                    f.write(json.dumps(d, ensure_ascii=False) + "\n")
``
                if type(retweet_id) == str:
                    d = find_tweet(retweet_id)
                    if d:
                        d["IRA_userid"] = row["retweet_userid"]
                        data.append(d)
        """

        IRA_map = {}
        for d in data:
            if len(d["IRA_userid"]) == 64:
                IRA_map[str(d["IRA_userid"])] = str(d["user_id"])


        IRA_user_list = []
        data_ira_users = pd.read_csv("data/ira_users_csv_hashed.csv", usecols=["userid"], dtype=str)
        for _, row in tqdm(data_ira_users.iterrows()):
            uid = row["userid"]
            IRA_user_list.append(uid)
            if uid in IRA_map:
                IRA_user_list.append(IRA_map[uid])

        json.dump(IRA_user_list, open("data/IRA_user_list.json", "w"), ensure_ascii=False, indent=2)
        json.dump(IRA_map, open("data/IRA_map.json", "w"), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    # who = Who_is_fake()
    # print(who.identify("baidu.com"))
    putin = Are_you_IRA()
    putin.find_IRA_retweets()
    putin.find_IRA_tweets()
    # putin.cal_IRA_map()
    # print(putin._map)

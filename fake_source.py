#-*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
from my_weapon import *
from SQLite_handler import find_tweet
from collections import defaultdict


class Fake_source(object):
    """
    1. 根据URL获取传播fake news的tweets；(fake_tweets.json和fake_IRA_tweets.json)
    2. 若转发了之上tweets，则添加到url_tweets中；(属于key则说明是转发推特，fake_retweet_network.json)
    3. 找到那些用户！

    每行必要数据：tweet_id, user_id, dt, is_first, is_source, URL, hostname, is_IRA
    """
    def __init__(self):
        self.fake_source_tweetids = set([line.strip() for line in open("data/fake_news_source.txt")])
        self.url_tweets = {}
        # self.tweets = None
        self.tweets_csv = []
        self.IRA_map = json.load(open("data/IRA_map_ele.json"))
        self.url_timeseries = defaultdict(list)
        # self.set_tweets = set()

    def fill_url_tweets(self):
        print("原始数据处理中 ...")
        for line in tqdm(open("data/fake_tweets.json")):
            d = json.loads(line.strip())
            if str(d["tweet_id"]) in self.fake_source_tweetids:
                tweet = {
                    "tweet_id": str(d["tweet_id"]),
                    "user_id": str(d["user_id"]),
                    "dt": d["datetime_EST"],
                    # "is_first": None,
                    # "is_source": None,
                    "is_IRA": None,
                    "URL": d["final_url"].lower(),
                    "hostname": d["final_hostname"].lower()
                }
                self.url_tweets[str(d["tweet_id"])] = tweet

        for line in tqdm(open("data/IRA_fake_tweets.json")):
            d = json.loads(line.strip())
            if str(d["tweetid"]) in self.url_tweets:
                self.url_tweets[d["tweetid"]]["is_IRA"] = 1
            elif str(d["tweetid"]) in self.fake_source_tweetids:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": None,
                    "dt": None,
                    # "is_first": None,
                    # "is_source": None,
                    "is_IRA": 1,
                    "URL": d["final_url"].lower(),
                    "hostname": d["hostname"].lower()
                }
                self.url_tweets[str(d["tweetid"])] = tweet

    def fill_IRA_info(self):
        print("补充IRA数据处理中 ...")
        cnt = 0
        IRA_info = pd.read_csv("data/ira-tweets-ele.csv", usecols=["tweetid", "userid", "tweet_time"], dtype=str)
        for i, row in tqdm(IRA_info.iterrows()):
            uid = row["userid"]
            if uid in self.IRA_map:
                uid = str(self.IRA_map[uid])

            if row["tweetid"] in self.url_tweets:
                self.url_tweets[row["tweetid"]].update(
                    {
                        "user_id": uid,
                        "is_IRA": 1,
                        "dt": row["tweet_time"] + ":00"
                    }
                )
                cnt += 1
        print("Count of IRA tweets:", cnt)

    def save_csv(self):
        for tweetid, values in self.url_tweets.items():
            self.tweets_csv.append(values)

        print("*.csv文件保存中 ...")
        pd.DataFrame(self.tweets_csv).to_csv("data/fake-tweets-source.csv", index=None)

    def save_url_ts(self):
        if self.url_timeseries:
            json.dump(self.url_timeseries, open("data/url-tweets.txt", "w"), ensure_ascii=False, indent=2)


    def convert_url_timeseries(self):
        print("转换成时间序列 ...")
        print(len(self.url_tweets))
        for tweet_id, tweet in tqdm(self.url_tweets.items()):
            if tweet["dt"]:
                self.url_timeseries[tweet["URL"]].append(tweet)

        sorted_url = sorted(self.url_timeseries.items(), key=lambda d: len(d[1]), reverse=True)

        self.url_timeseries = []
        for v in tqdm(sorted_url):
            url = v[0]
            tweet_list = v[1]
            sorted_tweets_list = sorted(tweet_list, key=lambda d: d["dt"])
            for i in range(len(sorted_tweets_list)):
                if i == 0:
                    sorted_tweets_list[0]["is_first"] = 1
                else:
                    sorted_tweets_list[i]["is_first"] = 0
            self.url_timeseries.append({"url": url, "tweets": sorted_tweets_list})
        self.save_url_ts()

        self.tweets_csv = []
        # for csv
        for url_ts in self.url_timeseries:
            for tweet in url_ts["tweets"]:
                self.tweets_csv.append(tweet)
        print(len(self.tweets_csv))


    def run(self):
        self.fill_url_tweets()
        self.fill_IRA_info()

        # 保存
        self.save_csv()


if __name__ == "__main__":
    LeBron = Fake_source()
    LeBron.run()




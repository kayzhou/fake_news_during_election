#-*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
from my_weapon import *
from SQLite_handler import find_tweet
from collections import defaultdict


class URL_TWEET:
    """
    1. 根据URL获取传播fake news的tweets；(fake-tweets.json)
    2. 若转发了之上tweets，则添加到url_tweets中；(属于key则说明是转发推特，retweet_network_fake.json)
    3. 将IRAs数据中的存在于选举时间且包括相关关键词的数据筛选出，若URL为fake news则添加到url_tweets中；IRAs = True
    4. 若IRAs数据中转发了以上的tweets，则添加到url_tweets中；IRA = True
    5. 整理数据结构，URL为key；
    6. url_tweets中tweets按时间排序；

    每行必要数据：tweet_id, user_id, dt, is_first, is_source, URL, hostname, is_IRA
    """
    def __init__(self):
        self.url_tweets = {}
        # self.tweets = None
        self.tweets_csv = None
        self.url_timeseries = defaultdict(list)
        # self.set_tweets = set()

    def fill_url_tweets(self):
        print("原始数据处理中 ...")
        for line in tqdm(open("data/fake_tweets.json")):
            d = json.loads(line.strip())
            tweet = {
                "tweet_id": str(d["tweet_id"]),
                "user_id": str(d["user_id"]),
                "dt": d["datetime_EST"],
                "is_first": None,
                "is_source": None,
                "is_IRA": 0,
                "URL": d["final_url"].lower(),
                "hostname": d["final_hostname"].lower()
            }
            self.url_tweets[str(d["tweet_id"])] = tweet

        for line in tqdm(open("data/IRA_fake_tweets.json")):
            d = json.loads(line.strip())
            if str(d["tweetid"]) in self.url_tweets:
                self.url_tweets[d["tweetid"]]["is_IRA"] = 1
            else:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": None,
                    "dt": None,
                    "is_first": None,
                    "is_source": None,
                    "is_IRA": 1,
                    "URL": d["real_url"].lower(),
                    "hostname": d["hostname"].lower()
                }
                self.url_tweets[str(d["tweetid"])] = tweet

    def fill_retweets(self):
        print("扩展转发处理中 ...")
        fake_retweets_links = json.load(open("data/fake_retweet_network.json"))
        for tweet_id, retweetd_id in tqdm(fake_retweets_links.items()):
            tweetid, origin_tweetdid = str(tweet_id), str(retweetd_id)

            # 新扩展进来的
            if tweetid not in self.url_tweets and origin_tweetdid in self.url_tweets:
                d = find_tweet(tweetid)
                if d:
                    tweet = {
                        "tweet_id": tweetid,
                        "user_id": str(d["user_id"]),
                        "dt": d["datetime_EST"],
                        "is_first": 0,
                        "is_source": 0,
                        "is_IRA": 0,
                        "URL": self.url_tweets[origin_tweetdid]["URL"],
                        "hostname": self.url_tweets[origin_tweetdid]["hostname"]
                    }
                else: # 找不到？IRA来补充，否则没救！
                    tweet = {
                        "tweet_id": tweetid,
                        "user_id": None,
                        "dt": None,
                        "is_first": 0,
                        "is_source": 0,
                        "is_IRA": 0,
                        "URL": self.url_tweets[origin_tweetdid]["URL"],
                        "hostname": self.url_tweets[origin_tweetdid]["hostname"]
                    }
                self.url_tweets[tweetid] = tweet

            # 原来就有，而且原来推特就是fake news
            elif tweetid in self.url_tweets and origin_tweetdid in self.url_tweets:
                self.url_tweets[tweetid].update({
                        "is_first": 0,
                        "is_source": 0,
                        "URL": self.url_tweets[origin_tweetdid]["URL"],
                        "hostname": self.url_tweets[origin_tweetdid]["hostname"]
                })

            else: # 我需要看你转发前是不是fake news，如果不是的话，我就不要了！
                  # 这个情况很特殊，转发了别人的信息，然后附带了fake news URL，需要特别处理；这里不考虑
                del self.url_tweets[tweetid]

        # 什么是source？没有转发别人的！
        for url, values in self.url_tweets.items():
            if self.url_tweets[url]["is_source"] == None:
                self.url_tweets[url]["is_source"] = 1


    def fill_IRA_info(self):
        print("补充IRA数据处理中 ...")
        cnt = 0
        IRA_match = json.load(open("data/IRA_match.json"))
        IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid", "tweet_time"], dtype=str)
        for i, row in tqdm(IRA_info.iterrows()):
            uid = row["userid"]
            if uid in IRA_match:
                uid = str(IRA_match[uid])

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
        print("*.csv文件保存中 ...")
        pd.DataFrame(self.tweets_csv).to_csv("data/url-fake-tweets.csv", index=None)


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
        self.fill_retweets()
        self.fill_IRA_info()

        # 补充is_first
        self.convert_url_timeseries()

        # 保存
        self.save_csv()


if __name__ == "__main__":
    LeBron = URL_TWEET()
    LeBron.run()




#-*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
from my_weapon import *
from SQLite_handler import find_tweet
from collections import defaultdict
from fake_identify import Who_is_fake
import sqlite3

class ALL_TWEET(object):
    """
    FAKE_TWEET的全面版本
    """
    def __init__(self):
        self.tweet_ids = []

        self.tweets = {}
        self.tweets_csv = []
        self.url_timeseries = defaultdict(list)
        self.IRA_map = json.load(open("data/IRA_map_ele.json"))
        # self.set_tweets = set()

    def find_all_tweets(self):
        # newest
        judge = Who_is_fake()

        # all
        conn = sqlite3.connect(
            "/home/alex/network_workdir/elections/databases/urls_db.sqlite")
        c = conn.cursor()
        c.execute('''SELECT * FROM urls;''')
        col_names = [t[0] for t in c.description]

        with open("data/all_tweets.json", "w") as f:
            print("start ...")
            for d in tqdm(c.fetchall()):
                if d[8]:
                    hostname = d[8]
                    # print(hostname)
                    labels = judge.identify(hostname)
                    fake_label = labels[0]
                    polarity_label = labels[1]
                    if fake_label == "GOOD" and polarity_label == -1:
                        continue
                    json_d = {k: v for k, v in zip(col_names, d)}
                    json_d["fake"] = fake_label
                    json_d["polarity"] = polarity_label
                    f.write(json.dumps(json_d, ensure_ascii=False) + '\n')
                    self.tweet_ids.append(json_d["tweet_id"])
        conn.close()

        # IRA
        with open("data/all_IRA_tweets.json", "w") as f:
            for line in open("data/ira-final-urls.json"):
                d = json.loads(line.strip())
                hostname = d["hostname"]
                # print(hostname)
                labels = judge.identify(hostname)
                fake_label = labels[0]
                polarity_label = labels[1]
                d["fake"] = fake_label
                d["polarity"] = polarity_label
                f.write(json.dumps(d, ensure_ascii=False) + '\n')
                self.tweet_ids.append(json_d["tweet_id"])


    def find_links(self):
        tweets_ids = self.tweet_ids
        retweet_link = {}
        conn = sqlite3.connect(
            "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
        c = conn.cursor()

        for _id in tqdm(tweets_ids):
            # 找到谁转发了我？
            c.execute(
                '''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
            for next_d in c.fetchall():
                next_id = str(next_d[0])
                retweet_link[next_id] = str(_id)
        conn.close()

        # 下一个！
        conn = sqlite3.connect(
            "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c = conn.cursor()

        for _id in tqdm(tweets_ids):
            # 找到谁转发了我？
            c.execute(
                '''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
            for next_d in c.fetchall():
                next_id = str(next_d[0])
                retweet_link[next_id] = str(_id)
        conn.close()

        cnt = 0
        data_ira = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "retweet_tweetid"], dtype=str)
        data_ira = data_ira.dropna()
        for i, row in data_ira.iterrows():
            tid = row["tweetid"]
            re_tid = row["retweet_tweetid"]

            # if re_tid in tweets_ids or tid in tweets_ids:
            if re_tid in tweets_ids:
                retweet_link[tid] = re_tid
                cnt += 1

        print("IRA -> ", cnt)

        json.dump(retweet_link, open("data/all_retweet_network.json",
                                    "w"), ensure_ascii=False, indent=2)

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
            self.tweets[str(d["tweet_id"])] = tweet

        for line in tqdm(open("data/IRA_fake_tweets.json")):
            d = json.loads(line.strip())
            if str(d["tweetid"]) in self.tweets:
                self.tweets[d["tweetid"]]["is_IRA"] = 1
            else:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": None,
                    "dt": None,
                    "is_first": None,
                    "is_source": None,
                    "is_IRA": 1,
                    "URL": d["final_url"].lower(),
                    "hostname": d["hostname"].lower()
                }
                self.tweets[str(d["tweetid"])] = tweet

    def fill_retweets(self):
        print("扩展转发处理中 ...")

        fake_retweets_links = json.load(open("data/fake_retweet_network.json"))
        for tweet_id, retweetd_id in tqdm(fake_retweets_links.items()):
            tweetid, origin_tweetdid = str(tweet_id), str(retweetd_id)

            # 新扩展进来的
            if tweetid not in self.tweets:
                tweet = {
                    "tweet_id": tweetid,
                    "user_id": None,
                    "dt": None,
                    "is_first": 0,
                    "is_source": 0,
                    "is_IRA": 0,
                    "URL": self.tweets[origin_tweetdid]["URL"],
                    "hostname": self.tweets[origin_tweetdid]["hostname"]
                }
                d = find_tweet(tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                self.tweets[tweetid] = tweet

            # 原来就存在
            else:
                self.tweets[tweetid]["is_source"] = 0

        # 什么是source？没有转发别人的！
        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_source"] == None:
                self.tweets[tweetid]["is_source"] = 1


    def fill_IRA_info(self):
        print("补充IRA数据处理中 ...")
        cnt = 0
        IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid", "tweet_time"], dtype=str)
        for i, row in tqdm(IRA_info.iterrows()):
            tweetid = row["tweetid"]

            if tweetid in self.tweets:
                uid = row["userid"]
                if uid in self.IRA_map:
                    uid = str(self.IRA_map[uid])

                self.tweets[tweetid]["is_IRA"] = 1
                self.tweets[tweetid]["user_id"] = uid
                if not self.tweets[tweetid]["dt"]:
                    self.tweets[tweetid]["dt"] = row["tweet_time"] + ":00"
                cnt += 1
        print("Count of IRA tweets:", cnt)

    def convert_url_timeseries(self):
        print("转换成时间序列 ...")
        print(len(self.tweets))
        for tweet_id, tweet in tqdm(self.tweets.items()):
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

        # for csv
        for url_ts in self.url_timeseries:
            for tweet in url_ts["tweets"]:
                self.tweets_csv.append(tweet)
        print(len(self.tweets_csv))


    # -- save -- #
    def save_url_ts(self):
        if self.url_timeseries:
            json.dump(self.url_timeseries, open("data/fake-url-tweets.json", "w"), ensure_ascii=False, indent=2)

    def save_csv(self):
        print("*.csv文件保存中 ...")
        pd.DataFrame(self.tweets_csv).to_csv("data/fake-tweets.csv", index=None)

    def save_network(self):
        tweets_csv = pd.read_csv("data/fake-tweets.csv")
        retweet_network = json.load(open("data/fake_retweet_network.json"))
        G = nx.DiGraph()
        nodes = tweets_csv["user_id"].tolist()
        edges = []
        dict_tweetid_userid = {}
        for _, row in tweets_csv.iterrows():
            dict_tweetid_userid[row["tweet_id"]] = row["user_id"]

        for n2, n1 in retweet_network.items():
            u1 = dict_tweetid_userid[n1]
            u2 = dict_tweetid_userid[n2]
            edges.append((u1, u2))

        print("add nodes from ...")
        G.add_nodes_from(nodes)
        print("add edge from ...")
        G.add_edges_from(edges)
        nx.write_gpickle(G, "data/fake_network.gpickle")

    def run(self):
        self.find_all_tweets()
        self.find_links()

        # self.fill_url_tweets()
        # self.fill_retweets()
        # self.fill_IRA_info()

        # 补充is_first
        # self.convert_url_timeseries()

        # 保存
        # self.save_url_ts()
        # self.save_csv()
        # self.save_network()


if __name__ == "__main__":
    LeBron = ALL_TWEET()
    LeBron.run()




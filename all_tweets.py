#-*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
import sqlite3
from collections import defaultdict
from datetime import datetime

import graph_tool.all as gt

from fake_identify import Who_is_fake
from my_weapon import *
from SQLite_handler import find_tweet


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

        with open("disk/all_tweets.json", "w") as f:
            print("start ...")
            for d in tqdm(c.fetchall()):
                if d[8]:
                    hostname = d[8]
                    # print(hostname)
                    labels = judge.identify(hostname)
                    fake_label = labels[0]
                    polarity_label = labels[1]
                    # 既没有极性也不是fake或bias
                    if fake_label == "GOOD" and polarity_label == -1:
                        continue
                    json_d = {k: v for k, v in zip(col_names, d)}
                    json_d["fake"] = fake_label
                    json_d["polarity"] = polarity_label
                    f.write(json.dumps(json_d, ensure_ascii=False) + '\n')
                    # self.tweet_ids.append(json_d["tweet_id"])
        conn.close()

        cnt = 0
        # IRA
        with open("disk/all_IRA_tweets.json", "w") as f:
            for line in open("data/ira-final-urls.json"):
                d = json.loads(line.strip())
                hostname = d["hostname"]
                # print(hostname)
                labels = judge.identify(hostname)
                fake_label = labels[0]
                polarity_label = labels[1]
                if fake_label == "GOOD" and polarity_label == -1:
                    continue
                cnt += 1
                d["fake"] = fake_label
                d["polarity"] = polarity_label
                f.write(json.dumps(d, ensure_ascii=False) + '\n')
                # self.tweet_ids.append(d["tweetid"])
        print("count of IRAs:", cnt)

    def find_links(self):
        if not self.tweet_ids:
            for line in open("disk/all_tweets.json"):
                self.tweet_ids.append(json.loads(line.strip())["tweet_id"])
            print(len(self.tweet_ids))
            for line in open("disk/all_IRA_tweets.json"):
                self.tweet_ids.append(json.loads(line.strip())["tweetid"])
            print(len(self.tweet_ids))

        tweets_ids = set(self.tweet_ids)
        print("目前所有tweets的量", len(tweets_ids))
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

        json.dump(retweet_link, open("disk/all_retweet_network.json",
                                    "w"), ensure_ascii=False, indent=2)

    def fill_url_tweets(self):
        print("原始数据处理中 ...")

        for line in tqdm(open("disk/all_tweets.json")):
            d = json.loads(line.strip())
            tweet = {
                "tweet_id": str(d["tweet_id"]),
                "user_id": str(d["user_id"]),
                "dt": d["datetime_EST"],
                "is_first": None,
                "is_source": None,
                "is_IRA": 0,
                "URL": d["final_url"].lower(),
                "hostname": d["final_hostname"].lower(),
                "fake": d["fake"],
                "polarity": d["polarity"]
            }
            self.tweets[str(d["tweet_id"])] = tweet

        for line in tqdm(open("disk/all_IRA_tweets.json")):
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
                    "hostname": d["hostname"].lower(),
                    "fake": d["fake"],
                    "polarity": d["polarity"]
                }
                self.tweets[str(d["tweetid"])] = tweet

    def fill_retweets(self):
        print("扩展转发处理中 ...")

        fake_retweets_links = json.load(open("disk/all_retweet_network.json"))
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
                    "hostname": self.tweets[origin_tweetdid]["hostname"],
                    "fake": self.tweets[origin_tweetdid]["fake"],
                    "polarity": self.tweets[origin_tweetdid]["polarity"]
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
            json.dump(self.url_timeseries, open("disk/all-url-tweets.json", "w"), ensure_ascii=False, indent=2)

    def save_csv(self):
        print("*.csv文件保存中 ...")
        pd.DataFrame(self.tweets_csv).to_csv("disk/all-tweets.csv", index=None)

    def save_network(self):
        tweets_csv = pd.read_csv("data/all-tweets.csv")
        retweet_network = json.load(open("disk/all_retweet_network.json"))
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


    def load_retweet_network(self):
        r_net = json.load(open("disk/all_retweet_network.json"))
        self.retweet_network = r_net


    def save_network_gt(self, _tweets, _users, out_name):
        g = gt.Graph()

        nodes = _users.index.tolist()
        node_map = {n:i for i, n in enumerate(nodes)}

        dict_tweetid_userid = {}
        for _, row in _tweets.iterrows():
            dict_tweetid_userid[row["tweet_id"]] = row["user_id"]

        print("add nodes from ...")
        vlist = g.add_vertex(len(nodes))

        edges = []
        print("add edge from ...")
        for n2, n1 in tqdm(self.retweet_network.items()):
            if n1 in dict_tweetid_userid:
                u1 = node_map[dict_tweetid_userid[n1]]
                try:
                    u2 = node_map[dict_tweetid_userid[n2]]
                except:
                    print("can not find n2!")
                    continue

                g.add_edge(g.vertex(u1), g.vertex(u2))

        json.dump(node_map, open(out_name + "_node_map.json", "w"))

        print("saving the graph ...", out_name + ".gt")
        g.save(out_name + ".gt")
        print("finished!")

    def get_users(self, tweets):
        user_count = pd.value_counts(tweets["user_id"]).rename("cnt")
        user_sources_count = tweets["is_source"].groupby(tweets["user_id"]).sum().sort_values(ascending=False).rename("source_cnt")
        user_first_count = tweets["is_first"].groupby(tweets["user_id"]).sum().sort_values(ascending=False).rename("first_cnt")
        users = pd.concat([user_count, user_first_count, user_sources_count], axis=1, sort=False)
        users["source_rate"] = users["source_cnt"] / users["cnt"]
        users["first_rate"] = users["first_cnt"] / users["cnt"]
        users["first_source_rate"] = users["first_cnt"] / users["source_cnt"]
        users.fillna(0, inplace=True)
        # print(users.head())
        return users

    def relation_betw_source_and_CI(self):
        all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str)
        print("loaded all tweets!")
        all_tweets = all_tweets.astype({"is_IRA": int, "is_first": int, "is_source": int, "dt": datetime})
        self.load_retweet_network()

        fake_labels = ["FAKE", "BIAS"]

        for f_label in fake_labels:
            print(f_label, "...")
            tweets = all_tweets[all_tweets["fake"]==f_label]
            users = self.get_users(tweets)
            users.to_csv("data/users_{}.csv".format(f_label))
            self.save_network_gt(tweets, users, "disk/network_{}.gt".format(f_label))

        polarity_labels = ["LEFT", "LEFTCENTER", "CENTER", "RIGHTCENTER", "RIGHT"]

        for p_label in polarity_labels:
            print(p_label, "...")
            tweets = all_tweets[all_tweets["polarity"]==p_label]
            users = self.get_users(tweets)
            users.to_csv("data/users_{}.csv".format(p_label))
            self.save_network_gt(tweets, users, "disk/network_{}".format(p_label))

    def run(self):
        # 找数据
        # self.find_all_tweets()
        # self.find_links()

        self.fill_url_tweets()
        self.fill_retweets()
        self.fill_IRA_info()

        # 补充is_first
        self.convert_url_timeseries()

        # 保存
        self.save_url_ts()
        self.save_csv()
        # self.save_network()
        self.save_network_gt()


if __name__ == "__main__":
    LeBron = ALL_TWEET()
    # LeBron.run()

    # make graphs and make user dataset
    LeBron.relation_betw_source_and_CI()

#-*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
import sqlite3
from collections import defaultdict
from datetime import datetime

import graph_tool.all as gt

from fake_identify import Who_is_fake, Are_you_IRA
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
                    hostname = d[8].lower()
                    # print(hostname)
                    media_type = judge.identify(hostname)
                    if media_type == -1:
                        continue
                    json_d = {k: v for k, v in zip(col_names, d)}
                    json_d["media_type"] = media_type
                    f.write(json.dumps(json_d, ensure_ascii=False) + '\n')
                    # self.tweet_ids.append(json_d["tweet_id"])
        conn.close()

        cnt = 0
        # IRA
        with open("disk/all_IRA_tweets.json", "w") as f:
            for line in open("data/ira-final-urls.json"):
                d = json.loads(line.strip())
                hostname = d["hostname"].lower()
                # print(hostname)
                media_type = judge.identify(hostname)
                if media_type == -1:
                    continue
                cnt += 1
                d["media_type"] = media_type
                f.write(json.dumps(d, ensure_ascii=False) + '\n')

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
            # 寻找双向
            if tid in tweets_ids or re_tid in tweets_ids: 
                retweet_link[tid] = re_tid

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
                "is_first": -1,
                "is_source": -1,
                "is_IRA": -1,
                "URL": d["final_url"].lower(),
                "hostname": d["final_hostname"].lower(),
                "media_type": d["media_type"],
                "retweeted_id": -1
            }
            self.tweets[str(d["tweet_id"])] = tweet

        for line in tqdm(open("disk/all_IRA_tweets.json")):
            d = json.loads(line.strip())

            if str(d["tweetid"]) in self.tweets:
                self.tweets[d["tweetid"]]["is_IRA"] = 1

            else:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": -1,
                    "dt": "1900-01-01 00:00:00",
                    "is_first": -1,
                    "is_source": -1,
                    "is_IRA": 1,
                    "URL": d["final_url"].lower(),
                    "hostname": d["hostname"].lower(),
                    "media_type": d["media_type"],
                    "retweeted_id": -1
                }
                self.tweets[str(d["tweetid"])] = tweet

    def fill_retweets(self):
        print("扩展转发处理中 ...")

        fake_retweets_links = json.load(open("disk/all_retweet_network.json"))
        for tweet_id, retweetd_id in tqdm(fake_retweets_links.items()):
            tweetid, origin_tweetdid = str(tweet_id), str(retweetd_id)

            # tweetid 一定是转发的！

            # 新扩展进来的
            if tweetid not in self.tweets:
                tweet = {
                    "tweet_id": tweetid,
                    "user_id": -1,
                    "dt": "1900-01-01 00:00:00",
                    "is_first": 0,
                    "is_source": 0,
                    "is_IRA": -1,
                    "URL": self.tweets[origin_tweetdid]["URL"],
                    "hostname": self.tweets[origin_tweetdid]["hostname"],
                    "media_type": self.tweets[origin_tweetdid]["media_type"],
                    "retweeted_id": origin_tweetdid
                }
                d = find_tweet(tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                self.tweets[tweetid] = tweet

            # 原来就存在
            else:
                self.tweets[tweetid]["is_source"] = 0
                self.tweets[tweetid]["retweeted_id"] = origin_tweetdid

            if origin_tweetdid not in self.tweets:
                tweet = {
                    "tweet_id": origin_tweetdid,
                    "user_id": -1,
                    "dt": "1900-01-01 00:00:00",
                    "is_first": -1,
                    "is_source": 1,
                    "is_IRA": -1,
                    "URL": self.tweets[tweetid]["URL"],
                    "hostname": self.tweets[tweetid]["hostname"],
                    "media_type": self.tweets[tweetid]["media_type"],
                    "retweeted_id": 0
                }
                d = find_tweet(tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                self.tweets[tweetid] = tweet

            else:
                self.tweets[tweetid]["is_source"] = 1
                self.tweets[tweetid]["retweeted_id"] = 0

        # 什么是source？没有转发别人的！
        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_source"] == -1:
                self.tweets[tweetid]["is_source"] = 1


    def fill_IRA_info(self):
        putin = Are_you_IRA()
        print("补充IRA数据处理中 ...")
        cnt = 0
        IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                        usecols=["tweetid", "userid", "tweet_time", "retweet_userid", "retweet_tweetid"], dtype=str)
        for i, row in tqdm(IRA_info.iterrows()):
            tweetid = row["tweetid"]
            retweet_id = row["retweet_tweetid"]

            if tweetid in self.tweets:
                uid = row["userid"]
                if uid in putin._map:
                    uid = str(putin._map[uid])

                self.tweets[tweetid]["is_IRA"] = 1
                self.tweets[tweetid]["user_id"] = uid

                if self.tweets[tweetid]["dt"] == "1900-01-01 00:00:00":
                    self.tweets[tweetid]["dt"] = row["tweet_time"] + ":00"
                cnt += 1

            if retweet_id in self.tweets:
                if self.tweets[retweet_id]["user_id"] == -1:
                    r_uid = row["retweet_userid"]
                    if r_uid in putin._map:
                        r_uid = str(putin._map[r_uid])
                    self.tweets[retweet_id]["user_id"] = r_uid

        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_IRA"] == -1:
                self.tweets[tweetid]["is_IRA"] = 0

        print("Count of IRA tweets:", cnt)


    def convert_url_timeseries(self):
        print("转换成时间序列 ...")
        print(len(self.tweets))
        url_type = {}

        for tweet_id, tweet in tqdm(self.tweets.items()):
            self.url_timeseries[tweet["URL"]].append(tweet)
            url_type[tweet["URL"]] = tweet["media_type"]

        sorted_url = sorted(self.url_timeseries.items(), key=lambda d: len(d[1]), reverse=True)

        self.url_timeseries = []

        for v in tqdm(sorted_url):
            url = v[0]
            tweet_list = v[1]
            sorted_tweets_list = sorted(tweet_list, key=lambda d: d["dt"]) # 有可能存在1900-01-01 00:00:00
            is_first_marked = False
            for i, _tweets in enumerate(sorted_tweets_list):
                if sorted_tweets_list[i]["is_source"] == 1 and is_first_marked == False:
                    sorted_tweets_list[i]["is_first"] = 1
                    is_first_marked = True
                else:
                    sorted_tweets_list[i]["is_first"] = 0

            self.url_timeseries.append({"url": url, "media_type": url_type[url], "tweets": sorted_tweets_list})

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

    # def save_network(self):
    #     tweets_csv = pd.read_csv("data/all-tweets.csv")
    #     retweet_network = json.load(open("disk/all_retweet_network.json"))
    #     G = nx.DiGraph()

    #     nodes = tweets_csv["user_id"].tolist()
    #     edges = []
    #     dict_tweetid_userid = {}
    #     for _, row in tweets_csv.iterrows():
    #         dict_tweetid_userid[row["tweet_id"]] = row["user_id"]

    #     for n2, n1 in retweet_network.items():
    #         u1 = dict_tweetid_userid[n1]
    #         u2 = dict_tweetid_userid[n2]
    #         edges.append((u1, u2))

    #     print("add nodes from ...")
    #     G.add_nodes_from(nodes)
    #     print("add edge from ...")
    #     G.add_edges_from(edges)
    #     nx.write_gpickle(G, "data/fake_network.gpickle")


    def load_retweet_network(self):
        r_net = json.load(open("disk/all_retweet_network.json"))
        self.retweet_network = r_net


    def load_all_tweets(self):
        all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str)
        all_tweets = all_tweets.astype({"is_IRA": int, "is_first": int, "is_source": int, "dt": datetime})
        self.tweets_csv = all_tweets


    def load_all_users(self):
        all_users = pd.read_csv("disk/all-users.csv", dtype=str)
        all_tweets = all_tweets.astype({"is_IRA": int, "is_first": int, "is_source": int, "dt": datetime})
        self.tweets_csv = all_tweets


    def make_users(self):
        self.load_all_tweets()
        all_tweets = self.tweets_csv
        users = None
        map_labels = {
            "0": "fake",
            "1": "extreme bias (right)",
            "2": "right",
            "3": "right leaning",
            "4": "center",
            "5": "left leaning",
            "6": "left",
            "7": "extreme bias (left)"
        }

        for _type, f_label in map_labels.items():
            print(_type, "...")
            tweets = all_tweets[all_tweets["media_type"]==_type]

            user_count = pd.value_counts(tweets["user_id"]).rename(f_label)
            user_sources_count = tweets["is_source"].groupby(tweets["user_id"]).sum().rename(f_label + "_source")
            user_first_count = tweets["is_first"].groupby(tweets["user_id"]).sum().rename(f_label + "_first")

            if users is None:
                users = pd.concat([user_count, user_first_count, user_sources_count], axis=1, sort=False)
            else:
                users = pd.concat([users, user_count, user_first_count, user_sources_count], axis=1, sort=False)

        for _type, f_label in map_labels.items():
            users[f_label + "_source_rate"] = users[f_label + "_source"] / users[f_label]
            users[f_label + "_first_rate"] = users[f_label + "_first"] / users[f_label]
            users[f_label + "_first_source_rate"] = users[f_label + "_first"] / users[f_label + "_source"]

        IRA_tweets = all_tweets[all_tweets["is_IRA"]==1]
        IRA_u = IRA_tweets["is_IRA"].groupby(IRA_tweets["user_id"]).sum().rename("is_IRA")
        users = pd.concat([users, IRA_u], axis=1, sort=False)

        users.fillna(0, inplace=True)
        # save data

        users["user_id"] = users.index
        users.to_csv("data/all-users.csv", index=False)

    def save_network_gt(self, _tweets, dict_tweetid_userid, node_map, out_name):
        g = gt.Graph()

        print("add nodes from ...")
        vlist = g.add_vertex(len(node_map))

        print("add edge from ...")
        for n2, n1 in tqdm(self.retweet_network.items()):
            if n1 in _tweets:
                try:
                    u1 = node_map[dict_tweetid_userid[n1]]
                    u2 = node_map[dict_tweetid_userid[n2]]
                    g.add_edge(g.vertex(u1), g.vertex(u2))
                except:
                    print(n2, n1)

        print("saving the graph ...", out_name)
        g.save(out_name)
        print("finished!")

    def make_graph_for_CI(self):
        self.load_all_tweets()
        all_tweets = self.tweets_csv
        print("loaded all tweets!")

        print("loading retweet network ...")
        self.load_retweet_network()

        print("making dict_tweetid_userid ...")
        dict_tweetid_userid = {}
        for _, row in tqdm(all_tweets.iterrows()):
            dict_tweetid_userid[row["tweet_id"]] = str(row["user_id"])
        nodes = all_tweets["user_id"].unique().tolist()
        node_map = {n:i for i, n in enumerate(nodes)}
        json.dump(node_map, open("disk/node_map.json", "w"))

        map_labels = {
            "0": "fake",
            "1": "extreme bias (right)",
            "2": "right",
            "3": "right leaning",
            "4": "center",
            "5": "left leaning",
            "6": "left",
            "7": "extreme bias (left)"
        }

        for _type, f_label in map_labels.items():
            print(_type, "...")
            tweets = all_tweets[all_tweets["media_type"]==_type]
            self.save_network_gt(set(tweets.tweet_id), dict_tweetid_userid, node_map, "disk/network_{}.gt".format(f_label))


    def run(self):
        # 找数据
        # self.find_all_tweets()
        # self.find_links()

        # self.fill_url_tweets()
        # self.fill_retweets()
        # self.fill_IRA_info()

        # 补充is_first
        # self.convert_url_timeseries()

        # 保存
        # self.save_url_ts()
        # self.save_csv()

        # self.make_users()
        self.make_graph_for_CI()


if __name__ == "__main__":
    LeBron = ALL_TWEET()
    LeBron.run()

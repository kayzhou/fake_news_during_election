# -*- coding: utf-8 -*-

"""
Created on 2018-12-01 17:02:07
@author: https://kayzhou.github.io/
"""
import sqlite3
from collections import defaultdict
from datetime import datetime

import graph_tool.all as gt
from fake_identify import Are_you_IRA, Who_is_fake
from my_weapon import *
from SQLite_handler import find_tweet, find_retweet_network


class ALL_TWEET(object):
    """
    FAKE_TWEET的全面版本
    """

    def __init__(self):
        self.tweet_ids = []
        self.tweets = {}
        self.tweets_csv = []
        self.url_timeseries = []

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

        print("count of IRA tweets:", cnt)

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
        retweet_link = find_retweet_network(tweets_ids)

        # IRA
        data_ira = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                               usecols=["tweetid", "retweet_tweetid"], dtype=str)
        data_ira = data_ira.dropna()
        for _, row in data_ira.iterrows():
            tid = row["tweetid"]
            re_tid = row["retweet_tweetid"]
            if tid in tweets_ids or re_tid in tweets_ids:
                retweet_link[tid] = re_tid

        print("saving retweet network ...")
        json.dump(retweet_link, open("data/all_retweet_network.json",
                                     "w"), ensure_ascii=False, indent=2)

    def fill_tweets(self):
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

        cnt = 0
        for line in tqdm(open("disk/all_IRA_tweets.json")):
            d = json.loads(line.strip())
            if str(d["tweetid"]) not in self.tweets:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    "is_first": -1,
                    "is_source": -1,
                    "is_IRA": 1,
                    "URL": d["final_url"].lower(),
                    "hostname": d["hostname"].lower(),
                    "media_type": d["media_type"],
                    "retweeted_id": -1
                }
                self.tweets[str(d["tweetid"])] = tweet
            else:
                cnt += 1
        print("IRA已经存在！", cnt)

    def fill_retweets(self):
        print("扩展转发处理中 ...")
        tweets_from_SQL = json.load(open("disk/tweets_from_SQL.json"))
        retweets_links = json.load(open("data/all_retweet_network.json"))

        for tweetid, origin_tweetid in tqdm(retweets_links.items()):
            # tweetid 一定是转发的！

            # 新扩展进来的
            if tweetid not in self.tweets:
                tweet = {
                    "tweet_id": tweetid,
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    "is_first": 0,
                    "is_source": 0,
                    "is_IRA": -1,
                    "URL": self.tweets[origin_tweetid]["URL"],
                    "hostname": self.tweets[origin_tweetid]["hostname"],
                    "media_type": self.tweets[origin_tweetid]["media_type"],
                    "retweeted_id": origin_tweetid
                }
                d = {}
                if tweetid in tweets_from_SQL:
                    d = tweets_from_SQL[tweetid]
                else:
                    d = find_tweet(tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                    tweets_from_SQL[tweetid] = d
                self.tweets[tweetid] = tweet

            # 原来就存在
            else:
                self.tweets[tweetid]["is_source"] = 0
                self.tweets[tweetid]["is_first"] = 0
                self.tweets[tweetid]["retweeted_id"] = origin_tweetid

            # 原始的不在里面，只可能是IRA-tweets里面发现的。但是我不知道详细的信息，所以这里暂时不需要 ???
            if origin_tweetid not in self.tweets:
                tweet = {
                    "tweet_id": origin_tweetid,
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    "is_first": -1,
                    "is_source": 1,
                    "is_IRA": -1,
                    "URL": self.tweets[tweetid]["URL"],
                    "hostname": self.tweets[tweetid]["hostname"],
                    "media_type": self.tweets[tweetid]["media_type"],
                    "retweeted_id": 0
                }
                d = {}
                if origin_tweetid in tweets_from_SQL:
                    d = tweets_from_SQL[origin_tweetid]
                else:
                    d = find_tweet(origin_tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                    tweets_from_SQL[origin_tweetid] = d

                self.tweets[origin_tweetid] = tweet

            else:
                self.tweets[origin_tweetid]["is_source"] = 1
                self.tweets[origin_tweetid]["retweeted_id"] = 0

        print("saving tweets_from_SQL ...")
        # json.dump(tweets_from_SQL, open("disk/tweets_from_SQL.json",
        #                                 "w"), indent=2, ensure_ascii=False)

        # 什么是source？没有转发别人的！其他的全部为源！
        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_source"] == -1:
                self.tweets[tweetid]["is_source"] = 1
            if self.tweets[tweetid]["retweeted_id"] == -1:
                self.tweets[tweetid]["retweeted_id"] = 0


        # fix URL
        print("fixing URL ...")
        for tweetid in self.tweets.keys():
            ret_id = self.tweets[tweetid]["retweeted_id"]
            if ret_id != 0:
                self.tweets[tweetid]["URL"] = self.tweets[ret_id]["URL"]
                self.tweets[tweetid]["media_type"] = self.tweets[ret_id]["media_type"]


    def fill_IRA_info(self):
        putin = Are_you_IRA()
        print("补充IRA数据处理中 ...")
        cnt = 0
        IRA_info = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                               usecols=["tweetid", "userid", "tweet_time", "retweet_userid", "retweet_tweetid"], dtype=str)
        for _, row in tqdm(IRA_info.iterrows()):
            tweetid = row["tweetid"]
            retweet_id = row["retweet_tweetid"]

            if tweetid in self.tweets:
                uid = row["userid"]
                if uid in putin._map:
                    uid = str(putin._map[uid])

                self.tweets[tweetid]["is_IRA"] = 1

                if self.tweets[tweetid]["user_id"] == -1:
                    self.tweets[tweetid]["user_id"] = uid
                if self.tweets[tweetid]["dt"] == "2000-01-01 00:00:00":
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

        url_type = {}
        url_timeseries = defaultdict(list)

        # 半路出家
        if not self.tweets:

            self.tweets = self.load_all_tweets()
            for _, tweet in self.tweets.iterrows():
                tweet = tweet.to_dict()
                url_timeseries[tweet["URL"]].append(tweet)
                url_type[tweet["URL"]] = tweet["media_type"]

        # 一气呵成
        else:
            for _, tweet in tqdm(self.tweets.items()):
                url_timeseries[tweet["URL"]].append(tweet)
                url_type[tweet["URL"]] = tweet["media_type"]


        # 涉及到url的tweets数量排序
        sorted_url = sorted(url_timeseries.items(),
                            key=lambda d: len(d[1]), reverse=True)

        cnt = 0
        # find first
        for v in tqdm(sorted_url):
            url = v[0]
            tweet_list = v[1]
            # 有可能存在2000-01-01 00:00:00
            sorted_tweets_list = sorted(tweet_list, key=lambda d: d["tweet_id"])
            for i, _tweet in enumerate(sorted_tweets_list):
                if i == 0:
                    sorted_tweets_list[i]["is_first"] = 1
                else:
                    sorted_tweets_list[i]["is_first"] = 0

            self.url_timeseries.append(
                {"URL": url, "media_type": url_type[url], "tweets": sorted_tweets_list})
        print("convert ts finished! error:", cnt)

        # for csv
        if not self.tweets_csv:
            for url_ts in self.url_timeseries:
                for tweet in url_ts["tweets"]:
                    self.tweets_csv.append(tweet)
        # print(len(self.tweets_csv))

        # saving!
        self.save_csv()
        self.save_url_ts()

    # -- save -- #

    def save_url_ts(self):

        if not self.url_timeseries:
            return 0

        # json.dump(self.url_timeseries, open("disk/all-url-tweets.json", "w"), ensure_ascii=False, indent=2)
        data_group = [list(), list(), list(), list(),
                      list(), list(), list(), list()]

        for d in self.url_timeseries:
            url = d["URL"]
            media_type = int(d["media_type"])
            for i, tweet in enumerate(d["tweets"]):
                del d["tweets"][i]["URL"]
                del d["tweets"][i]["media_type"]
                del d["tweets"][i]["hostname"]
            data_group[media_type].append(d)

        for i in range(8):
            print("saving url_ts ...", i)
            json.dump(data_group[i], open(
                "disk/url_ts_media_{}.json".format(i), "w"), ensure_ascii=False, indent=1)

    def save_csv(self):
        print("*.csv文件保存中 ...")
        pd.DataFrame(self.tweets_csv).to_csv("disk/all-tweets.csv", index=None)

    def load_retweet_network(self):
        r_net = json.load(open("disk/all_retweet_network.json"))
        self.retweet_network = r_net

    def load_all_tweets(self):
        all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str)
        all_tweets = all_tweets.astype(
            {"is_IRA": int, "is_first": int, "is_source": int, "dt": datetime})
        self.tweets_csv = all_tweets
        return all_tweets

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
            tweets = all_tweets[all_tweets["media_type"] == _type]

            user_count = pd.value_counts(tweets["user_id"]).rename(f_label)
            user_sources_count = tweets["is_source"].groupby(
                tweets["user_id"]).sum().rename(f_label + "_source")
            user_first_count = tweets["is_first"].groupby(
                tweets["user_id"]).sum().rename(f_label + "_first")

            if users is None:
                users = pd.concat(
                    [user_count, user_first_count, user_sources_count], axis=1, sort=False)
            else:
                users = pd.concat(
                    [users, user_count, user_first_count, user_sources_count], axis=1, sort=False)

        for _type, f_label in map_labels.items():
            users[f_label + "_source_rate"] = users[f_label +
                                                    "_source"] / users[f_label]
            users[f_label + "_first_rate"] = users[f_label +
                                                   "_first"] / users[f_label]
            users[f_label + "_first_source_rate"] = users[f_label +
                                                          "_first"] / users[f_label + "_source"]

        IRA_tweets = all_tweets[all_tweets["is_IRA"] == 1]
        IRA_u = IRA_tweets["is_IRA"].groupby(
            IRA_tweets["user_id"]).sum().rename("is_IRA")
        users = pd.concat([users, IRA_u], axis=1, sort=False)

        users.fillna(0, inplace=True)
        # save data

        users["user_id"] = users.index
        users.to_csv("data/all-users.csv", index=False)

    # abandon
    def save_network_gt(self, _tweets, dict_tweetid_userid, node_map, out_name):
        g = gt.Graph()

        print("add nodes from ...")
        vlist = g.add_vertex(len(node_map))

        print("add edge from ...")
        for n2, n1 in tqdm(self.retweet_network.items()):
            if n1 in _tweets:
                try:
                    u1 = dict_tweetid_userid[n1]
                    u1 = node_map[u1]
                    u2 = dict_tweetid_userid[n2]
                    u2 = node_map[u2]
                    g.add_edge(g.vertex(u1), g.vertex(u2))
                except:
                    print(n2, ">", n1)

        print("saving the graph ...", out_name)
        g.save(out_name)
        print("finished!")

    def make_graph_for_CI(self):

        self.load_all_tweets()
        all_tweets = self.tweets_csv
        print("loaded all tweets!")

        self.load_retweet_network()
        print("loaded retweet network!")

        print("making dict_tweetid_userid ...")
        dict_tweetid_userid = {}
        for _, row in tqdm(all_tweets.iterrows()):
            dict_tweetid_userid[str(row["tweet_id"])] = str(row["user_id"])

        # nodes = all_tweets["user_id"].unique().tolist()
        # print("count of nodes(users):", len(nodes))
        # node_map = {n:i for i, n in enumerate(nodes)}
        # json.dump(node_map, open("disk/node_map.json", "w"))

        def save_network_nx(_tweets, out_name):
            g = nx.DiGraph()

            print("add edge from ...")
            for n2, n1 in tqdm(self.retweet_network.items()):
                if n1 in _tweets:
                    try:
                        u1 = dict_tweetid_userid[n1]
                        u2 = dict_tweetid_userid[n2]
                        g.add_edge(u1, u2)
                    except:
                        print(n2, ">", n1)

            print("saving the graph ...", out_name)
            nx.write_gpickle(g, out_name)
            # print("finished!")

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
            tweets = all_tweets[all_tweets["media_type"] == _type]
            save_network_nx(set(tweets.tweet_id),
                            "disk/network/network_{}.gpickle".format(f_label))

    def run(self):
        # 找数据
        # self.find_all_tweets()
        # self.find_links()

        # self.fill_tweets()
        # self.fill_retweets()
        # self.fill_IRA_info()

        # 补充is_first
        self.convert_url_timeseries()
        # 保存，已经放在covert里面
        # self.save_url_ts()
        # self.save_csv()

        self.make_users()
        self.make_graph_for_CI()


if __name__ == "__main__":
    LeBron = ALL_TWEET()
    LeBron.run()

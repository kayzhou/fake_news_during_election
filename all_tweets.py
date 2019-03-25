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
import ujson as json


class ALL_TWEET(object):
    """
    FAKE_TWEET的全面版本
    """

    def __init__(self):
        self.tweet_ids = []
        self.tweets = {}
        self.tweets_csv = []
        self.url_timeseries = []
        self.map_labels = {
            "-1": "-1",
            "0": "fake",
            "1": "extreme bias (right)",
            "2": "right",
            "3": "right leaning",
            "4": "center",
            "5": "left leaning",
            "6": "left",
            "7": "extreme bias (left)"
        }

    def find_all_tweets(self):
        # newest
        Putin = Who_is_fake()

        # all
        # conn = sqlite3.connect(
        #     "/home/alex/network_workdir/elections/databases/urls_db.sqlite")
        # c = conn.cursor()
        # c.execute('''SELECT * FROM urls;''')
        # col_names = [t[0] for t in c.description]

        # with open("disk/all_tweets.json", "w") as f:
        #     print("start ...")
        #     for d in tqdm(c.fetchall()):
        #         if d[8]:
        #             hostname = d[8].lower()
        #             # print(hostname)
        #             if hostname.startswith("www."):
        #                 hostname = hostname[4:]
        #             media_type = judge.identify(hostname)
        #             if media_type == -1:
        #                 continue
        #             json_d = {k: v for k, v in zip(col_names, d)}
        #             json_d["media_type"] = media_type
        #             f.write(json.dumps(json_d, ensure_ascii=False) + '\n')
        #             # self.tweet_ids.append(json_d["tweet_id"])
        # conn.close()


        # IRA
        with open("disk/all_IRA_tweets.json", "w") as f:
            for line in tqdm(open("data/ira-urls-plus-2.json")):
                d = json.loads(line.strip())
                hostname = d["hostname"].lower()
                # print(hostname)
                if hostname.startswith("www."):
                    hostname = hostname[4:]

                label_b = self.map_labels[str(Putin.identify(hostname))]
                label = Putin.identify_v2(hostname)
                label_sci_f = Putin.identify_science_fake(hostname)
                label_sci_a = Putin.identify_science_align(hostname)

                json_d = d
                json_d["media_type"] = label_b
                json_d["c_mbfc"] = label              
                json_d["c_sci_fake"] = label_sci_f                         
                json_d["c_sci_align"] = label_sci_a

                f.write(json.dumps(d, ensure_ascii=False) + '\n')


    def find_links(self):
        if not self.tweet_ids:
            for line in open("disk/bingo_tweets.json"):
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

        for line in tqdm(open("disk/bingo_tweets.json")):
            d = json.loads(line.strip())
            if d["c_sci_fake"] != "-1" and d["media_type"] == "-1":
                d["media_type"] = "fake"

            tweet = {
                "tweet_id": str(d["tweet_id"]),
                "user_id": str(d["user_id"]),
                "dt": d["datetime_EST"],
                # "is_first": -1,
                "is_source": -1,
                "is_IRA": -1,
                # "URL": d["final_url"].lower(),
                # "hostname": d["final_hostname"].lower(),
                "c_alex": d["media_type"],
                # "c_mbfc": d["c_mbfc"],
                # "c_sci_f": d["c_sci_fake"],
                # "c_sci_s": d["c_sci_align"],
                "retweeted_id": -1,
            }
            # if tweet["URL"].endswith("/"):
            #     tweet["URL"] = tweet["URL"][:-1]

            self.tweets[str(d["tweet_id"])] = tweet

        cnt = 0
        for line in tqdm(open("disk/all_IRA_tweets.json")):
            d = json.loads(line.strip())
            if d["c_sci_fake"] != "-1" and d["media_type"] == "-1":
                d["media_type"] = "fake"
            if str(d["tweetid"]) not in self.tweets:
                tweet = {
                    "tweet_id": str(d["tweetid"]),
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    # "is_first": -1,
                    "is_source": -1,
                    "is_IRA": 1,
                    # "URL": d["final_url"].lower(),
                    # "hostname": d["hostname"].lower(),
                    "c_alex": d["media_type"],
                    # "c_mbfc": d["c_mbfc"],
                    # "c_sci_f": d["c_sci_fake"],
                    # "c_sci_s": d["c_sci_align"],
                    "retweeted_id": -1,
                }
                # if tweet["URL"].endswith("/"):
                #     tweet["URL"] = tweet["URL"][:-1]
                self.tweets[str(d["tweetid"])] = tweet
            else:
                cnt += 1
        print("IRA已经存在数量：", cnt)

    def fill_retweets(self):
        print("扩展转发处理中 ...")
        # tweets_from_SQL = json.load(open("disk/tweets_from_SQL.json"))
        retweets_links = json.load(open("data/all_retweet_network.json"))

        cnt = 0

        for tweetid, origin_tweetid in tqdm(retweets_links.items()):
            # tweetid 一定是转发的！

            # 新扩展进来的
            if tweetid not in self.tweets:
                tweet = {
                    "tweet_id": tweetid,
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    # "is_first": 0,
                    "is_source": 0,
                    "is_IRA": -1,
                    # "URL": self.tweets[origin_tweetid]["URL"],
                    # "hostname": self.tweets[origin_tweetid]["hostname"],
                    "c_alex": self.tweets[origin_tweetid]["c_alex"],
                    # "c_mbfc": self.tweets[origin_tweetid]["c_mbfc"],
                    # "c_sci_f": self.tweets[origin_tweetid]["c_sci_f"],
                    # "c_sci_s": self.tweets[origin_tweetid]["c_sci_s"],
                    "retweeted_id": origin_tweetid
                }
                # d = {}
                # if tweetid in tweets_from_SQL:
                #     d = tweets_from_SQL[tweetid]
                # else:
                #     d = find_tweet(stweetid)

                d = find_tweet(tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                    # tweets_from_SQL[tweetid] = d
                self.tweets[tweetid] = tweet

            # 原来就存在
            else:
                self.tweets[tweetid]["is_source"] = 0
                # self.tweets[tweetid]["is_first"] = 0
                self.tweets[tweetid]["retweeted_id"] = origin_tweetid

            # 原始的不在里面，只可能是IRA-tweets里面发现的
            # 但是ira data中original tweets不知道详细信息

            # 用被转发的结果来定义原始的结果可能会存在问题
            if origin_tweetid not in self.tweets:
                cnt += 1
                tweet = {
                    "tweet_id": origin_tweetid,
                    "user_id": -1,
                    "dt": "2000-01-01 00:00:00",
                    # "is_first": -1,
                    "is_source": 1,
                    "is_IRA": -1,
                    # "URL": self.tweets[tweetid]["URL"],
                    # "hostname": self.tweets[tweetid]["hostname"],
                    "c_alex": self.tweets[tweetid]["c_alex"],
                    # "c_mbfc": self.tweets[tweetid]["c_mbfc"],
                    # "c_sci_f": self.tweets[tweetid]["c_sci_f"],
                    # "c_sci_s": self.tweets[tweetid]["c_sci_s"],
                    "retweeted_id": 0
                }
                # d = {}
                # if origin_tweetid in tweets_from_SQL:
                #     d = tweets_from_SQL[origin_tweetid]
                # else:
                #     d = find_tweet(origin_tweetid)
                d = find_tweet(origin_tweetid)
                if d:
                    tweet["user_id"] = str(d["user_id"])
                    tweet["dt"] = d["datetime_EST"]
                    # tweets_from_SQL[origin_tweetid] = d

                self.tweets[origin_tweetid] = tweet

            else:
                self.tweets[origin_tweetid]["is_source"] = 1
                self.tweets[origin_tweetid]["retweeted_id"] = 0

        # print("saving tweets_from_SQL ...")
        # json.dump(tweets_from_SQL, open("disk/tweets_from_SQL.json",
        #                                 "w"), indent=2, ensure_ascii=False)

        # 什么是source？没有转发别人的！其他的全部为源！
        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_source"] == -1:
                self.tweets[tweetid]["is_source"] = 1
            if self.tweets[tweetid]["retweeted_id"] == -1:
                self.tweets[tweetid]["retweeted_id"] = 0


        # fix URL
        # print("fixing URL ...") # 转发还包括了引用啊！
        # for tweetid in self.tweets.keys():
        #     ret_id = self.tweets[tweetid]["retweeted_id"]
        #     if ret_id != 0:
        #         self.tweets[tweetid]["URL"] = self.tweets[ret_id]["URL"]
        #         self.tweets[tweetid]["media_type"] = self.tweets[ret_id]["media_type"]


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
                uid = putin.uncover(uid)

                self.tweets[tweetid]["is_IRA"] = 1

                if self.tweets[tweetid]["user_id"] == -1:
                    self.tweets[tweetid]["user_id"] = uid
                if self.tweets[tweetid]["dt"] == "2000-01-01 00:00:00":
                    self.tweets[tweetid]["dt"] = row["tweet_time"] + ":00"

                cnt += 1

            if retweet_id in self.tweets:
                if self.tweets[retweet_id]["user_id"] == -1:
                    r_uid = row["retweet_userid"]
                    r_uid = putin.uncover(r_uid)
                    self.tweets[retweet_id]["user_id"] = r_uid

        for tweetid in self.tweets.keys():
            if self.tweets[tweetid]["is_IRA"] == -1:
                if putin.fuck(self.tweets[tweetid]["user_id"]):
                    self.tweets[tweetid]["is_IRA"] = 1
                    cnt += 1
                else:
                    self.tweets[tweetid]["is_IRA"] = 0

        print("Count of IRA tweets:", cnt)


    def fill_other_info(self):
        # tweets数据因分类变化，扩充后需要把之前的数据项补全
        data = pd.read_csv("disk/all-tweets.csv", dtype=str)
        len_URL_id = 0
        dict_URL_id = {}

        # data["is_first"] = -1 first目前先不考虑，在做时间序列的时候再考虑。

        # 先找到所有source_tweets，填补数据
        # source_tweets = data[data.is_source=="1"]
        tweets_id = set(data.tweet_id.tolist())
        print("tweets", len(tweets_id))

        tweets = {}
        for line in tqdm(open("disk/bingo_tweets.json")):
            d = json.loads(line.strip())
            t_id = str(d["tweet_id"])
            if t_id not in tweets_id:
                continue
            
            d["final_url"] = d["final_url"].lower()

            if d["final_url"].endswith("/"):
                d["final_url"] = d["final_url"][:-1]

            if d["final_url"] not in dict_URL_id:
                dict_URL_id[d["final_url"]] = len_URL_id
                len_URL_id += 1

            url_id = dict_URL_id[d["final_url"]]

            tweets[t_id] = {
                "tweet_id": t_id,
                "URL_id": url_id,
                "hostname": d["final_hostname"].lower()
            }

        # IRA
        for line in tqdm(open("disk/all_IRA_tweets.json")):
            d = json.loads(line.strip())
            t_id = str(d["tweetid"])
            if t_id not in tweets_id:
                continue

            d["final_url"] = d["final_url"].lower()

            if d["final_url"].endswith("/"):
                d["final_url"] = d["final_url"][:-1]

            if d["final_url"] not in dict_URL_id:
                dict_URL_id[d["final_url"]] = len_URL_id
                len_URL_id += 1

            url_id = dict_URL_id[d["final_url"]]

            tweets[t_id] = {
                "tweet_id": t_id,
                "URL_id": url_id,
                "hostname": d["hostname"].lower()
            }

        error_cnt = 0
        fuck = 0
        non_source_tweets = data[data["is_source"]=="0"]
        for i, row in tqdm(non_source_tweets.iterrows()):
            t_id = row["tweet_id"]
            ret_id = row["retweeted_id"]
            if ret_id in tweets:
                tmp_t = tweets[ret_id]
                tmp_t["tweet_id"] = t_id
                tweets[t_id] = tmp_t
            elif t_id in tweets: 
                error_cnt += 1
                tmp_t = tweets[t_id]
                tmp_t["tweet_id"] = ret_id
                tweets[ret_id] = tmp_t
            else:
                fuck += 1

        print("原始推特缺少url信息：", error_cnt)
        print("谁都没有url还玩个P：", fuck)

        # save
        json.dump(dict_URL_id, open("disk/dict_URL_id.json", "w"), indent=1)
        tweets = pd.DataFrame(list(tweets.values()))
        print("saving ...")
        tweets.to_csv("disk/all-tweets-ht.csv", index=None)


    def convert_url_timeseries(self):
        print("转换成时间序列 ...")

        url_type = {}
        url_timeseries = defaultdict(list)

        shit = False

        # 半路出家
        if not self.tweets:
            self.load_all_tweets()
            for _, tweet in tqdm(self.tweets_csv.iterrows()):
                tweet = tweet.to_dict()
                url_timeseries[tweet["URL"]].append(tweet)
                url_type[tweet["URL"]] = tweet["media_type"]

        # 一气呵成
        else:
            for _, tweet in tqdm(self.tweets.items()):
                shit = True
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
            # 有可能存在2000-01-01 00:00:00，因为未知具体时间
            sorted_tweets_list = sorted(tweet_list, key=lambda d: d["tweet_id"])
            for i, _tweet in enumerate(sorted_tweets_list):
                if i == 0:
                    sorted_tweets_list[i]["is_first"] = 1
                    if sorted_tweets_list[0]["is_source"] != 1:
                        # 时间上第一个居然不是source?
                        print(url, sorted_tweets_list)
                        cnt += 1
                else:
                    sorted_tweets_list[i]["is_first"] = 0

            self.url_timeseries.append(
                {"URL": url, "media_type": url_type[url], "tweets": sorted_tweets_list})
        print("convert ts finished! error:", cnt)

        # for csv
        if shit:
            self.tweets_csv = []
            for url_ts in self.url_timeseries:
                for tweet in url_ts["tweets"]:
                    self.tweets_csv.append(tweet)
            self.save_csv()

        # saving!
        # self.save_url_ts()

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
        if not self.tweets_csv:
            self.tweets_csv = list(self.tweets.values())
        pd.DataFrame(self.tweets_csv).to_csv("disk/all-tweets-v2.csv", index=None)

    def load_retweet_network(self):
        r_net = json.load(open("data/all_retweet_network.json"))
        self.retweet_network = r_net

    def load_all_tweets(self):
        print("loading all tweets_csv ...")
        all_tweets = pd.read_csv("disk/all-tweets-v2.csv", dtype=str)
        self.tweets_csv = all_tweets.astype(
            {"is_IRA": int, "is_source": int, "dt": datetime})
        print("finished!")
        return self.tweets_csv

    def make_users(self):
        # self.load_all_tweets()
        # all_tweets = self.tweets_csv

        print("loading all tweets_csv ...")
        all_tweets = pd.read_csv("disk/all-tweets-v2.csv", dtype=str).astype({"is_IRA": int, "is_source": int})
        # all_tweets = all_tweets[all_tweets.c_alex != "-1"]
        # all_tweets.to_csv("disk/all-tweets-sf.csv")

        users = None
        # map_labels = {
        #     "0": "fake",
        #     "1": "extreme bias (right)",
        #     "2": "right",
        #     "3": "right leaning",
        #     "4": "center",
        #     "5": "left leaning",
        #     "6": "left",
        #     "7": "extreme bias (left)"
        # }

        labels = [
            "fake",
            "extreme bias (right)",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",
            "extreme bias (left)"
        ]

        # mbfc_labels = [
        #     "fake",
        #     "right",
        #     "right leaning",
        #     "center",
        #     "left leaning",
        #     "left",     
        # ]

        # labels = [
        #     "Black",
        #     "Red",
        #     "Orange",
        # ]

        for _type in labels:
            print(_type, "...")
            tweets = all_tweets[all_tweets["c_alex"] == _type]
            # tweets = all_tweets[all_tweets["c_sci_f"] == _type]
            user_count = pd.value_counts(tweets["user_id"]).rename(_type)
            user_sources_count = tweets["is_source"].groupby(
                tweets["user_id"]).sum().rename(_type + "_source")
            # user_first_count = tweets["is_first"].groupby(
            #     tweets["user_id"]).sum().rename(f_label + "_first")
            if users is None:
                users = pd.concat(
                    [user_count, user_sources_count], axis=1, sort=False)
            else:
                users = pd.concat(
                    [users, user_count, user_sources_count], axis=1, sort=False)

        for _type in labels:
            users[_type + "_source_rate"] = users[_type + "_source"] / users[_type]
            # users[f_label + "_first_rate"] = users[f_label +
            #                                        "_first"] / users[f_label]
            # users[f_label + "_first_source_rate"] = users[f_label +
            #                                               "_first"] / users[f_label + "_source"]

        IRA_tweets = all_tweets[all_tweets["is_IRA"] == 1]
        IRA_u = IRA_tweets["is_IRA"].groupby(
            IRA_tweets["user_id"]).sum().rename("is_IRA")
        users = pd.concat([users, IRA_u], axis=1, sort=False)

        users.fillna(0, inplace=True)
        # save data
        users["user_id"] = users.index
        users.to_csv("data/all-users-v2.csv", index=False)

    def make_graph_for_CI(self):

        print("loading all tweets_csv ...")
        all_tweets = pd.read_csv("disk/all-tweets-v2.csv", dtype=str, usecols=["user_id", "tweet_id", "c_alex"])
        # all_tweets = all_tweets[all_tweets.c_mbfc != "-1"]
        # all_tweets = all_tweets[all_tweets.c_sci_f != "-1"]
        # all_tweets = all_tweets[all_tweets.c_alex != "-1"]

        self.load_retweet_network()
        print("loaded retweet network!")

        labels = [
            "fake",
            "extreme bias (right)",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",
            "extreme bias (left)"
        ]

        # labels = [
        #     "fake",
        #     "right",
        #     "right leaning",
        #     "center",
        #     "left leaning",
        #     "left",
        # ]

        # labels = [
        #     "Black",
        #     "Red",
        #     "Orange",
        #     "Fake",
        # ]

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
                if n1 in _tweets and n1 in dict_tweetid_userid and n2 in dict_tweetid_userid:
                    try:
                        u1 = dict_tweetid_userid[n1]
                        u2 = dict_tweetid_userid[n2]
                        g.add_edge(u1, u2)
                    except:
                        pass
                        # print(n2, ">", n1)

            print("saving the graph ...", out_name)
            nx.write_gpickle(g, out_name)
            # print("finished!")

        for _type in labels:
            print(_type, "...")
            tweets = all_tweets[all_tweets["c_alex"] == _type]
            # tweets = all_tweets[all_tweets["c_mbfc"] == _type]
            
            # if _type == "Fake":
            #     tweets = all_tweets
            # else:
            #     tweets = all_tweets[all_tweets["c_sci_f"] == _type]

            save_network_nx(set(tweets.tweet_id),
                            "disk/network/{}_v2.gpickle".format(_type.lower()))


    def load_all_users(self):
        print("Loading all users ...")
        # map_labels = {
        #     "0": "fake",
        #     "1": "extreme bias (right)",
        #     "2": "right",
        #     "3": "right leaning",
        #     "4": "center",
        #     "5": "left leaning",
        #     "6": "left",
        #     "7": "extreme bias (left)"
        # }

        users = pd.read_csv("data/all-users-mbfc.csv", index_col="user_id", dtype={"user_id": str})

        # change the type
        # f_labels = [map_labels[k] for k in map_labels]
        users = users.astype({"is_IRA": int})

        mbfc_labels = [
            "fake",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",     
        ]

        for label in mbfc_labels:
            users = users.astype({label: int, label + "_source": int, label + "_first": int, 
                                label + "_source_rate": float, label + "_first_rate": float,
                                label + "_first_source_rate": float,})

            users = users.astype({label: int, label + "_source": int, label + "_source_rate": float})

        print("Finished!")
        
        return users


    def for_fake_clique(self):
        self.load_retweet_network()
        self.load_all_tweets()
 
        retweet_network = self.retweet_network
        all_tweets = self.tweets_csv

        all_users = self.load_all_users()

        retweeted_count = defaultdict(int)

        for k, v in retweet_network.items():
            retweeted_count[v] += 1

        dict_tweetid_userid = defaultdict(list)
        for _, row in tqdm(all_tweets.iterrows()):
            if row["media_type"] == "0":
                dict_tweetid_userid[row["user_id"]].append(row["tweet_id"])

        def get_num_of_retweets_for_user(uid):
            """
            uid作为源的次数及被转总次数
            """
            # print(uid)
            tids = dict_tweetid_userid[uid]
            num_of_retweets = [retweeted_count[tid] for tid in tids]
            _sum = sum(num_of_retweets)
            return len(tids), _sum

        cnt = 0
        with open("data/CI_clique.txt", "w") as f:
            for uid in tqdm(all_users.index):
                cnt += 1
                rst = get_num_of_retweets_for_user(uid)
                print(uid, rst[0], rst[1], file=f, sep=",")
                if cnt % 100 == 0:
                    print(cnt)

    def run(self):
        # 找数据
        # self.find_all_tweets()
        # self.find_links()

        self.fill_tweets()
        self.fill_retweets()
        self.fill_IRA_info()
        self.save_csv()

        # 补充is_first
        # self.convert_url_timeseries()
        # self.save_csv()

        # self.fill_other_info()

        # 保存，已经放在covert里面
        # self.save_url_ts()
        # self.save_csv()

        self.make_users()
        self.make_graph_for_CI()

        # 2019-02-05 遵照Hernan的指示，增加实验
        # self.for_fake_clique()


if __name__ == "__main__":
    LeBron = ALL_TWEET()
    LeBron.run()

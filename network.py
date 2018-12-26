#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/

12th week in NY
"""

from my_weapon import *
import sqlite3
from SQLite_handler import get_user_id, find_tweet, find_all_uids


class analyze_IRA_in_network:
    def __init__(self):
        self.author = "kay"
        self.user_id_map = json.load(open("data/IRA_map_ele.json"))
        # self.network = nx.graph()

    def find_user_id_map(self):
        data = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                            usecols=["tweetid", "userid"], dtype=str)
        with open("data/IRA_map_v3.json", "w") as f:
            for _, row in tqdm(data.iterrows()):
                tweet_id = row["tweetid"]
                user_id = row["userid"]
                # if user_id in self.user_id_map:
                #     continue
                d = find_tweet(tweet_id)
                if d:
                    # self.user_id_map[user_id] = real_user_id
                    f.write("{},{},{}\n".format(tweet_id, user_id, d["user_id"]))

    def cal_map(self):
        # IRA_map_v2.json 较大库的映射
        map1 = []
        for line in open("data/IRA_map_v2.json"):
            words = line.strip().split(",")
            try:
                map1.append((words[1], words[2]))
            except:
                print(words)

        # 相同的匿名ID会不会存在不同的ID？
        test_data1 = {}
        for i, j in map1:
            if i in test_data1 and j != test_data1[i]:
                print("重复啦！")
                continue
            test_data1[i] = j

        print(len(test_data1))

        # SQLITE 数据库中的映射
        map2 = [(line.strip().split(",")[1], line.strip().split(",")[2]) for line in open("data/IRA_map_v3.json")]
        test_data2 = {}
        for i, j in map2:
            if i in test_data2 and j != test_data2[i]:
                print("重复啦！")
                continue
            test_data2[i] = j
        print(len(test_data2))

        IRA_map = {}
        for k, v in test_data1.items():
                IRA_map[k] = v
        for k, v in test_data2.items():
                IRA_map[k] = v
        print(len(IRA_map))

        self.user_id_map = IRA_map

        # save
        json.dump(test_data2, open("data/IRA_map_ele.json", "w"), indent=2)
        json.dump(self.user_id_map, open("data/IRA_map.json", "w"), indent=2)

    def un_anonymization(self):
        # 取消匿名化
        data = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                            usecols=["tweetid", "userid"], dtype=str)

        un_ano_count = 0
        for _, row in tqdm(data.iterrows()):
            user_id = row["userid"]
            # if user_id in self.user_id_map or len(user_id) != 64:
            if len(user_id) != 64:
                un_ano_count += 1
        print(un_ano_count, un_ano_count / len(data))

    def load_node(self):
        # uids = find_all_uids()
        # for uid in self.user_id_map.values():
        #     uids.append(uid)
        # uids = set(uids)
        # data = pd.read_csv("data/ira_users_csv_hashed.csv",
        #     usecols=["userid"], dtype=str)
        # for _, row in tqdm(data.iterrows()):
        #     user_id = row["userid"]
        #     if user_id not in self.user_id_map:
        #         uids.add(user_id)
        # json.dump(list(uids), open("data/node.json", "w"), indent=2)

        uids = json.load(open("data/node.json"))
        return uids

    def load_edge(self):
        retweet_link = []

        conn = sqlite3.connect(
            "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
        c = conn.cursor()
        c.execute(
            '''SELECT retweeted_uid, author_uid FROM tweet_to_retweeted_uid''')
        for row in c.fetchall():
            u1 = str(row[0])
            u2 = str(row[1])
            retweet_link.append((u1, u2))
        conn.close()

        # 下一个！
        conn = sqlite3.connect(
            "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c = conn.cursor()
        c.execute(
            '''SELECT retweeted_uid, author_uid FROM tweet_to_retweeted_uid''')
        for row in c.fetchall():
            u1 = str(row[0])
            u2 = str(row[1])
            retweet_link.append((u1, u2))
        conn.close()

        cnt = 0
        data_ira = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["userid", "retweet_userid"], dtype=str)
        data_ira = data_ira.dropna()
        for i, row in data_ira.iterrows():
            u1 = row["retweet_userid"]
            if u1 in self.user_id_map:
                u1 = self.user_id_map[u1]
            u2 = row["userid"]
            if u2 in self.user_id_map:
                u2 = self.user_id_map[u2]
            retweet_link.append((u1, u2))

        json.dump(retweet_link, open("data/edges.json", "w"),
            ensure_ascii=False, indent=2)

    # def build_network(self):



    def run(self):
        # self.find_user_id_map()
        # self.cal_map()
        # self.un_anonymization()
        # self.load_node()
        self.load_edge()


if __name__ == "__main__":
    Lebron = analyze_IRA_in_network()
    Lebron.run()

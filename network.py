#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/

12th week in NY
"""

from my_weapon import *
import sqlite3
from collections import defaultdict
from SQLite_handler import get_user_id, find_tweet, find_all_uids
import graph_tool as gt
import os


class analyze_IRA_in_network:
    def __init__(self):
        self.author = "kay"
        self.IRA_map = json.load(open("data/IRA_map_ele.json"))
        self.uid_index = {}
        self.chosed_nodes = set()

        self.G = nx.DiGraph()

    def find_IRA_map(self):
        data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"], dtype=str)
        with open("data/IRA_map_v3.json", "w") as f:
            for _, row in tqdm(data.iterrows()):
                tweet_id = row["tweetid"]
                user_id = row["userid"]
                # if user_id in self.IRA_map:
                #     continue
                d = find_tweet(tweet_id)
                if d:
                    # self.IRA_map[user_id] = real_user_id
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

        # self.IRA_map = IRA_map

        # save
        json.dump(test_data2, open("data/IRA_map_ele.json", "w"), indent=2)
        # json.dump(self.IRA_map, open("data/IRA_map.json", "w"), indent=2)

    def un_anony(self):
        # 取消匿名化
        data = pd.read_csv("data/ira_tweets_csv_hashed.csv",
                            usecols=["tweetid", "userid"], dtype=str)

        un_ano_count = 0
        for _, row in tqdm(data.iterrows()):
            user_id = row["userid"]
            # if user_id in self.IRA_map or len(user_id) != 64:
            if len(user_id) != 64:
                un_ano_count += 1
        print(un_ano_count, un_ano_count / len(data))

    def load_node(self):
        # uids = find_all_uids()
        # for uid in self.IRA_map.values():
        #     uids.append(uid)
        # uids = set(uids)
        # data = pd.read_csv("data/ira_users_csv_hashed.csv",
        #     usecols=["userid"], dtype=str)
        # for _, row in tqdm(data.iterrows()):
        #     user_id = row["userid"]
        #     if user_id not in self.IRA_map:
        #         uids.add(user_id)

        # # load from edge.txt
        # for line in open("data/edge.txt"):
        #     w = line.strip().split("-")
        #     uids.add(w[0])
        #     uids.add(w[1])

        # save
        # json.dump(list(uids), open("data/node.json", "w"), indent=2)

        # load
        uids = json.load(open("data/node.json"))
        for i, uid in enumerate(uids):
            self.uid_index[uid] = i
        print("nodes:", len(self.uid_index))

        degree = json.load(open("data/degree.json"))
        nodes = [int(k) for k, v in degree.items() if v["all_d"] > 1]
        self.chosed_nodes = set(nodes)
        print("chosed nodes:", len(self.chosed_nodes))

        return nodes

    def load_edge(self):
        # e_count = 0
        # print("loading edge ...")
        # retweet_link = set()

        # conn = sqlite3.connect(
        #     "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
        # c = conn.cursor()
        # c.execute(
        #     '''SELECT retweeted_uid, author_uid FROM tweet_to_retweeted_uid''')
        # for row in c.fetchall():
        #     e_count += 1
        #     try:
        #         u1 = str(row[0])
        #         u2 = str(row[1])
        #         # u1 = self.uid_index[str(row[0])]
        #         # u2 = self.uid_index[str(row[1])]
        #         retweet_link.add(str(u1) + "-" + str(u2))
        #     except:
        #         pass
        # conn.close()
        # print(e_count)

        # # 下一个！
        # conn = sqlite3.connect(
        #     "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        # c = conn.cursor()
        # c.execute(
        #     '''SELECT retweeted_uid, author_uid FROM tweet_to_retweeted_uid''')
        # for row in c.fetchall():
        #     e_count += 1
        #     try:
        #         u1 = str(row[0])
        #         u2 = str(row[1])
        #         # u1 = self.uid_index[str(row[0])]
        #         # u2 = self.uid_index[str(row[1])]
        #         retweet_link.add(str(u1) + "-" + str(u2))
        #     except:
        #         pass
        # conn.close()
        # print(e_count)

        # data_ira = pd.read_csv("data/ira_tweets_csv_hashed.csv",
        #         usecols=["userid", "retweet_userid"], dtype=str)
        # data_ira = data_ira.dropna()
        # for i, row in data_ira.iterrows():
        #     e_count += 1
        #     u1 = row["retweet_userid"]
        #     if u1 in self.IRA_map:
        #         u1 = self.IRA_map[u1]
        #     # try:
        #     #     u1 = self.uid_index[u1]
        #     # except:
        #     #     continue

        #     u2 = row["userid"]
        #     if u2 in self.IRA_map:
        #         u2 = self.IRA_map[u2]
        #     # try:
        #     #     u2 = self.uid_index[u2]
        #     # except:
        #     #     continue

        #     retweet_link.add(str(u1) + "-" + str(u2))

        # print(e_count)

        # save all edges
        # with(open("data/edge.txt", "w")) as f:
        #     for edge in retweet_link:
        #         f.write(edge + "\n")

        retweet_link = []
        for line in open("data/edge.txt"):
            w = line.strip().split("-")
            u1 = self.uid_index[w[0]]
            u2 = self.uid_index[w[1]]
            if u1 in self.chosed_nodes and u2 in self.chosed_nodes:
                retweet_link.append((u1, u2))

        print("edge:", len(retweet_link))
        print("finished!")

        return retweet_link

    def cal_degree(self):
        degree = {}
        for v in self.uid_index.values():
            degree[v] = {
                "in_d": 0,
                "out_d": 0,
                "all_d": 0
            }

        for line in open("data/edge.txt"):
            w = line.strip().split("-")
            u1 = self.uid_index[w[0]]
            u2 = self.uid_index[w[1]]
            degree[u1]["out_d"] += 1
            degree[u1]["all_d"] += 1
            degree[u2]["in_d"] += 1
            degree[u2]["all_d"] += 1

        json.dump(degree, open("data/degree.json", "w"), indent=2)
        degree = json.load(open("data/degree.json"))
        nodes = [k for k, v in degree.items() if v["all_d"] > 1]
        print(len(nodes))

    def build_network(self):
        nodes = self.load_node()
        # self.cal_degree()
        edges = self.load_edge()
        print("add nodes from ...")
        self.G.add_nodes_from(nodes)
        print("add edge from ...")
        self.G.add_edges_from(edges)
        nx.write_gpickle(self.G, "data/whole_network.gpickle")

    def build_network_gt(self):
        nodes = self.load_node()
        # self.cal_degree()
        edges = self.load_edge()
        g = Graph(directed=True)

        print("add nodes from ...")
        g.add_vertex(len(nodes))
        print("add edge from ...")
        for e in edges:
            g.add_edge(e[1], e[0])

        g.save("data/whole_network.gt")

    def run(self):

        # self.find_IRA_map()
        # self.cal_map()
        # self.un_anony()

        self.build_network()


#-*- coding: utf-8 -*-

"""
Created on 2019-01-18 19:00:31
@author: https://kayzhou.github.io/
"""

# import json
# import sqlite3

def get_whole_network():
    out_file = open("disk/all-links.json", "w")

    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid = str(d[0]),
                    r_uid = str(d[1]),
                    uid = str(d[2]),
                    r_tid = str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid = str(d[0]),
                    r_uid = str(d[1]),
                    uid = str(d[2]),
                    r_tid = str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()


def get_whole_network():
    out_file = open("disk/all-links.json", "w")

    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid = str(d[0]),
                    r_uid = str(d[1]),
                    uid = str(d[2]),
                    r_tid = str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid = str(d[0]),
                    r_uid = str(d[1]),
                    uid = str(d[2]),
                    r_tid = str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()

def get_ret_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_quote_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_quoted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_quoted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_rep_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_replied_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_replied_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_mention_network(out_name):
    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_mentioned_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_mentioned_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(_d) for _d in d]) + "\n")
    conn.close()

def get_bigger_network():
    in_dir = "/media/alex/datums/elections_tweets/archives/hillary OR clinton OR hillaryclinton"
    for in_name in os.listdir(in_dir):
        if not in_name.endswith(".taj"):
            continue
        for line in open()
    
    



if __name__ == "__main__":
    # Lebron = analyze_IRA_in_network()
    # Lebron.run()
    print("rep")
    get_ret_network("disk/all-ret-links.txt")
    print("ret")
    get_rep_network("disk/all-rep-links.txt")
    print("quo")
    get_quote_network("disk/all-quo-links.txt")
    print("men")
    get_mention_network("disk/all-men-links.txt")

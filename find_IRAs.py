#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import sqlite3
import json
from tqdm import tqdm

def find_tweet(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c1.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v

    conn1.close()
    conn2.close()

    if not new_d:
        new_d = find_retweeted(_id)

    # if not new_d:
    #     print("找不到该tweet：", _id)

    return new_d


def find_retweeted(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c1.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        # else:
        #     print("没有找到原创tweet (retweet)", _id)

    conn1.close()
    conn2.close()

    return new_d


data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"])

"""
# 找出真实的user_id
count = 0
with open("data/IRAs_be_found.json", "w") as f:
    for i, row in tqdm(data.iterrows()):
        tid = row["tweetid"]
        tweet = find_tweet(tid)
        if tweet:
            count += 1
            if count % 1000 == 0:
                print(count)
            f.write(json.dumps(tweet, ensure_ascii=False) + "\n")
"""

"""
# 统计有多少个匿名用户
data = pd.read_csv("data/ira_users_csv_hashed.csv")
count = 0
for i, row in tqdm(data.iterrows()):
    uid = row["userid"]
    if len(uid) == len("b76861a43db67db9e6c51db1d37d1a622bb7a8fc14a17c880f10f7d1f16a7175"):
        count += 1

print(count)
"""

uids = []
for i, row in tqdm(data.iterrows()):
    uid = row["userid"]
    uids.append(uid)
from collections import Counter

print(Counter(uids).most_common(100))

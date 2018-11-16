#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import json
from tqdm import tqdm
from SQLite_handler import find_tweet


data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"])
user_match ={}


# 找出真实的user_id
with open("data/IRAs_be_found.json", "w") as f:
    for i, row in tqdm(data.iterrows()):
        tid = row["tweetid"]
        uid = int(row["userid"])
        tweet = find_tweet(tid)
        if tweet:
            f.write(json.dumps(tweet, ensure_ascii=False) + "\n")
            real_uid = int(tweet["user_id"])
            if uid != real_uid:
                user_match[uid] = real_uid

json.dump(user_match, open("data/user_match.json", "w"), indent=2)


# 统计有多少个匿名用户
# data = pd.read_csv("data/ira_users_csv_hashed.csv")
# count = 0
# for i, row in tqdm(data.iterrows()):
#     uid = row["userid"]
#     if len(uid) == len("b76861a43db67db9e6c51db1d37d1a622bb7a8fc14a17c880f10f7d1f16a7175"):
#         count += 1

# print(count)


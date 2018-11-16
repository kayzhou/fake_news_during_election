#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import json
from tqdm import tqdm

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

# 统计有多少个匿名用户
data = pd.read_csv("data/ira_users_csv_hashed.csv")
count = 0
for i, row in tqdm(data.iterrows()):
    uid = row["userid"]
    if len(uid) == len("b76861a43db67db9e6c51db1d37d1a622bb7a8fc14a17c880f10f7d1f16a7175"):
        count += 1

print(count)
"""

found_IRAs = {}

for line in open("data/IRAs_be_found.json"):
    d = json.loads(line.strip())
    found_IRAs[d["tweet_id"]] = d["user_id"]

data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"])

with open("data/users_match.txt", "w") as f:
    for i, row in tqdm(data.iterrows()):
        tid = row["tweetid"]
        uid = row["userid"]
        if tid in found_IRAs:
            real_uid = found_IRAs[tid]
            if real_uid != uid:
                f.write("{}\t{}\n".format(uid, real_uid))


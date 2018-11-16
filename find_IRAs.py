#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import json
from tqdm import tqdm


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


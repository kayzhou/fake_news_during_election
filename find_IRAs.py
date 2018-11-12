#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import sqlite3

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

    if not new_d:
        print("找不到该tweet：", _id)

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
        else:
            print("没有找到原创tweet", _id)

    conn1.close()
    conn2.close()

    return new_d


data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"])
for row in data.iterrows():
    print(row[0], row[1])

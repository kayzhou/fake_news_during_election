# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-02-24 16:42:55

from my_weapon import *
import SQLite_handler

def get_train_data():
    """
    获取训练文本
    """
    print("loading all tweets_csv ...")
    all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str, nrows=100, usecols=["tweet_id", "media_type"])
    print("finished!")

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
        tweets_id = all_tweets[all_tweets["media_type"] == _type].tweet_id
        rst = SQLite_handler.find_tweets(tweets_id)
        with open("disk/train_data_fake/{}.txt".format(_type), "w") as f:
            for d in rst:
                if d["text"].startswith("RT"):
                    continue
                f.write(d["text"] + "\n")
            

if __name__ == "__main__":
    get_train_data()

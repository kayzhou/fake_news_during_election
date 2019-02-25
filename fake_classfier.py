# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-02-24 16:42:55

from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split

import SQLite_handler
from my_weapon import *
from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer

class Fake_Classifer(object):
    def __init__(self):
        self.MAP_LABELS = {
            "0": "fake",
            "1": "extreme bias (right)",
            "2": "right",
            "3": "right leaning",
            "4": "center",
            "5": "left leaning",
            "6": "left",
            "7": "extreme bias (left)"
        }

    def get_train_data(self):
        """
        获取训练文本
        """
        print("loading all tweets_csv ...")
        all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str, usecols=["tweet_id", "media_type"])
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
            print(len(rst))
            with open("disk/train_data_fake/{}.txt".format(_type), "w") as f:
                for d in rst:
                    if "text" not in d:
                        continue
                    # elif d["text"].startswith("RT"):
                    #     continue
                    f.write(d["text"] + "\n")

    def get_tokens(self):
        """
        text > tokens
        """
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
        tokenizer = CustomTweetTokenizer()

        for _type, f_label in map_labels.items():
            with open("disk/tokens_fake/{}.txt".format(_type), "w") as f:
                for line in open("disk/train_data_fake/{}.txt".format(_type)):
                    words = tokenizer.tokenize(line.strip())
                    f.write(" ".join(words) + "\n")

    def train(self):
        """
        一共种分类方式
        fake, non-fake
        fake, left, center, right √ 优先
        left, center, right
        """
        # read data
        X = []
        y = []
        for _type, f_label in self.MAP_LABELS.items():
            if f_label == "fake":
                y_i = 0
            elif f_label in ["extreme bias (right)", "right", "right leaning"]:
                y_i = 1
            elif f_label == "center":
                y_i = 2
            elif f_label in ["extreme bias (left)", "left", "left leaning"]:
                y_i = 3

            for line in open("disk/tokens_fake/{}.txt".format(_type)):
                w = line.strip().split()
                X.append(w)
                y.append(y_i)

        # split train and test data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

        # build one hot embedding
        one_hot = OneHotEncoder()
        one_hot.fit(X_train)
        X_train = one_hot.transform(X_train)
        print(X_train[0])

        # machine learning model

        # train and cross validation

        # test 



if __name__ == "__main__":
    Lebron = Fake_Classifer()
    # get_train_data()
    Lebron.get_tokens()

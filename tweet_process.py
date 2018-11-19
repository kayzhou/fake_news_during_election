#-*- coding: utf-8 -*-

"""
Created on 2018-11-19 14:51:58
@author: https://kayzhou.github.io/
"""
import sqlite3
from TwProcess import CustomTweetTokenizer
from SQLite_handler import find_tweet
from collections import defaultdict
from tqdm import tqdm


class TwPro:

    def __init__(self):
        # self.I_am = "best"
        self.all_hashtags = set([line.strip() for line in open("data/all_hashtag.txt")])

    def process_tweet(self, tweet_text, tokenizer=CustomTweetTokenizer(preserve_case=False,
                                        reduce_len=True,
                                        strip_handles=False,
                                        normalize_usernames=False,
                                        normalize_urls=True,
                                        keep_allupper=False)):

        tokens = tokenizer.tokenize(tweet_text)
        tokens_to_keep = []
        for token in tokens:
            #remove hastags that are used for classifying
            if token[0] == '#':
                if token[1:] in self.all_hashtags:
                    continue
            tokens_to_keep.append(token)
        return tokens_to_keep


if __name__ == "__main__":

    data = defaultdict(list)

    match = {
        "ht_pro_trump": 1,
        "ht_anti_trump": 0,
        "ht_pro_hillary": 0,
        "ht_anti_hillary": 1
    }

    # remove
    for line in tqdm(open("data/train.csv")):
        tid, hashtag, userid, opinion = line.strip().split(",")
        data[tid].append(match[opinion])

    data_label = {}
    for k, v in data.items():
        if sum(v) == 0:
            data_label[k] = 0
        elif sum(v) == len(v):
            data_label[k] = 1

    f1 = open("data/1.txt", "w")
    f0 = open("data/0.txt", "w")

    tw = TwPro()

    for tid, opinion in tqdm(data_label.items()):
        line = find_tweet(tid)["text"].strip()
        if tweet.startswith("RT "):
            continue
        tweet = tw.process_tweet(line)

        if writen_line:
            if opinion == 1:
                f1.write(" ".join(tweet) + "\n")
            if opinion == 0:
                f0.write(" ".join(tweet) + "\n")
#-*- coding: utf-8 -*-

"""
Created on 2018-11-19 14:51:58
@author: https://kayzhou.github.io/
"""
import sqlite3
from TwProcess import CustomTweetTokenizer


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
    tw = TwPro()
    print(tw.process_tweet("RT @ManMet80: Best. Video. Ever. #ImWithHer @HillaryClinton 🇺🇸💙 Women in the World 2012: Meryl Streep's Tribute to Hillary Clinton https://…m"))
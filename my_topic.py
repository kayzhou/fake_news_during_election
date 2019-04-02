#-*- coding: utf-8 -*-

"""
Created on 2019-01-27 17:55:17
@author: https://kayzhou.github.io/
"""

import json
import sqlite3

import numpy as np
import pandas as pd
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from gensim.test.utils import datapath
from tqdm import tqdm

from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer

tokenizer = CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True,
                                 strip_handles=False,
                                 normalize_usernames=False,
                                 normalize_urls=True,
                                 keep_allupper=False)

# conn = sqlite3.connect(
#     "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
# c = conn.cursor()
# c.execute('''SELECT text FROM tweet''')

# texts = []
# for line in open("data/ira-tweets-ele.csv"):
#     d = json.loads(line.strip())
#     words = tokenizer.tokenize(d["tweet_text"])
#     # if words[0] == "RT":
#     #     continue
#     texts.append(words) 

# print("loaded!")
# conn.close()

texts = pd.read_csv("data/ira-tweets-ele.csv", usecols=["tweet_text"])["tweet_text"]
print(len(texts))

dictionary = Dictionary(texts)
corpus = [dictionary.doc2bow(t) for t in texts]
lda = LdaModel(corpus, num_topics=10)

x = lda.show_topics(num_topics=10, num_words=20,formatted=False)
topics_words = [(tp[0], [wd[0] for wd in tp[1]]) for tp in x]

dictionary.id2token = {v: k for k, v in dictionary.token2id.items()}
#Below Code Prints Topics and Words
for topic, words in topics_words:
    print(str(topic)+ "::" + str([dictionary.id2token[int(w)] for w in words]))
print()

#Below Code Prints Only Words 
# for topic, words in topics_words:
#     print(" ".join(words))

# conn = sqlite3.connect(
#     "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
# c = conn.cursor()
# c.execute('''SELECT text FROM tweet''')

# for d in tqdm(c.fetchall()):
#     words = tokenizer.tokenize(d[0])
#     if words[0] == "rt":
#         continue
#     f.write(" ".join(words) + "\n") 

# # texts = [tokenizer.tokenize(d[0]) for d in c.fetchall()]
# print("loaded!")
# conn.close()

# dictionary = Dictionary(texts)
# corpus = [dictionary.doc2bow(t) for t in texts]
# lda.update(corpus)

# print(texts[1])
# print(dictionary.doc2bow(texts[1]))
# print(lda[dictionary.doc2bow(texts[1])])

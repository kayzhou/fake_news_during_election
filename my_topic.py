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
from nltk.corpus import stopwords
stopWords = set(stopwords.words('english'))

import string
for w in string.punctuation:
    stopWords.add(w)

stopWords.add("rt")
stopWords.add("…")
stopWords.add("...")
stopWords.add("URL")
stopWords.add("“")
stopWords.add("”")
stopWords.add("‘")
stopWords.add("’")

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
# print("loaded!")
# conn.close()


# texts = []
# for line in open("data/ira-tweets-ele.csv"):
#     d = json.loads(line.strip())
#     words = tokenizer.tokenize(d["tweet_text"])
#     # if words[0] == "RT":
#     #     continue
#     texts.append(words) 



texts = []
data = pd.read_csv("data/ira-tweets-ele.csv", usecols=["tweet_text"])["tweet_text"]
for d in data:
    words = tokenizer.tokenize(d)
    words = [w for w in words if w not in stopWords]
    # if words[0] == "RT":
    #     continue
    texts.append(words)  
print(len(texts))

dictionary = Dictionary(texts)
corpus = [dictionary.doc2bow(t) for t in texts]


for n in range(3, 10):
    lda = LdaModel(corpus, num_topics=n)
    print(lda.get_topics())
    # show
    x = lda.show_topics(num_topics=n, num_words=16, formatted=False)
    topics_words = [(tp[0], [wd[0] for wd in tp[1]]) for tp in x]

    dictionary.id2token = {v: k for k, v in dictionary.token2id.items()}
    # Below Code Prints Topics and Words
    for topic, words in topics_words:
        print(str(topic), "::", str([dictionary.id2token[int(w)] for w in words]))
    print()


"""
Topics:

0::['', 'get', 'day', '#tofeelbetteri', 'game', 'like', "can't", 'people', '#myolympicsportwouldbe', 'stop', 'time', 'enough', 'morning', 'trying', "i'd", 'little']
1::['#news', 'police', 'man', '#world', 'u', 'says', '#sports', 'killed', '#topnews', 'new', 'shot', 'shooting', 'woman', 'state', 'n', 'city']
2::['“', '”', 'new', '’', '#foke', 'foke', 'via', 'debate', 'video', 'live', 'watch', 'us', 'report', 'black', 'california', 'speech']
3::["he's", '@talibkweli', 'listen', 'us', '#betteralternativetodebates', 'country', 'help', 'win', '#trumpsfavoriteheadline', 'name', 'times', 'read', 'people', 'supporters', '#sometimesitsokto', '#pjnet']
4::['@danageezus', 'get', 'money', 'people', 'school', '@chrixmorgan', 'use', 'go', '#mustbebanned', '#ihatepokemongobecause', '#thingsmoretrustedthanhillary', '#obamaswishlist', '#toavoidworki', 'work', '#reasonstogetdivorced', '#wheniwasyoung']
5::['😂', 'thanks', 'lives', 'matter', '#obamanextjob', 'black', 'yes', '2', 'wrong', 'hashtag', 'us', 'part', 'children', '🔥', 'charged', 'https']
6::["i'm", 'people', 'like', 'know', 'would', 'black', 'think', 'one', 'really', 'white', '@midnight', 'good', 'never', 'love', "that's", 'want']
7::['trump', '’', 'hillary', 'clinton', 'https', '‘', 'vote', 'donald', 'obama', '#politics', 'media', '@realdonaldtrump', 'via', 'campaign', 'president', 'america']

"""

# Below Code Prints Only Words 
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

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    my_topic.py                                        :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/14 11:08:14 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/06/07 13:56:24 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import sqlite3
import string
from collections import Counter

import numpy as np
import pandas as pd
import scipy
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from gensim.test.utils import datapath
from nltk.corpus import stopwords
from tqdm import tqdm
import pprint
from gensim.models import CoherenceModel

from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer

stopWords = set(stopwords.words('english'))

for w in string.punctuation:
    stopWords.add(w)

stops_words = [
    "rt", "…", "...", "URL", "http", "https", "“", "”", "‘", "’", "get", "2", "new", "one", "i'm", "make",
    "go", "good", "say", "says", "know", "day", "..", "take", "got", "1", "going", "4", "3", "two", "n",
    "like", "via", "u", "would", "still", "first", "really", "watch", "see", "even", "that's", "look", "way",
    "last", "said", "let", "twitter", "ever", "always", "another", "many", "things", "may", "big", "come", "keep",
    "5", "time", "much", "want", "think", "us", "love", "people", "need"
]

for w in stops_words:
    stopWords.add(w)


tokenizer = CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True,
                                 strip_handles=False,
                                 normalize_usernames=False,
                                 normalize_urls=True,
                                 keep_allupper=False)


cnt = Counter()
texts = []
# comm = json.load(open("data/louvain_rst.json"))
# users_comm = {str(u) for u in comm if comm[u] == 0}
# print(len(users_comm))

# loading data
data = pd.read_csv("data/ira-tweets-ele.csv", usecols=["tweet_text", "userid"])
for i, row in tqdm(data.iterrows()):
    # if row["userid"] not in users_comm:
    #     continue
    words = tokenizer.tokenize(row["tweet_text"])
    words = [w for w in words if w not in stopWords and w]
    # if words[0] == "RT":
    #     continue
    for w in words:
        cnt[w] += 1
    texts.append(words)
print(len(texts))
json.dump(cnt.most_common(), open("data/word_cloud.json", "w"), indent=2)


dictionary = Dictionary(texts)
corpus = [dictionary.doc2bow(t) for t in texts]

def average_distance(v_tops):
    _sum = 0
    _cnt = 0
    for i in range(len(v_tops)):
        for j in range(i+1, len(v_tops)):
            _sum += scipy.spatial.distance.cosine(v_tops[i], v_tops[j])
            _cnt += 1
    return _sum / _cnt


with open("data/IRA_topics.txt", "w") as f:
    for n in range(2, 12):
        print(f"N = {n}")
        lda = LdaModel(corpus, num_topics=n, random_state=42)
        v_topics = lda.get_topics()
        lda.save(f"model/lda-ira-{n}.mod")
        # pprint(lda.print_topics())

        f.write(f"Perplexity: {lda.log_perplexity(corpus)}")  # a measure of how good the model is. lower the better.

        # Compute Coherence Score
        coherence_model_lda = CoherenceModel(model=lda, texts=corpus, coherence='c_v')
        coherence_lda = coherence_model_lda.get_coherence()
        f.write(f"Coherence Score: {coherence_lda}")


        f.write(f"~Average distance: {average_distance(v_topics)}\n")
        # show
        x = lda.show_topics(num_topics=n, num_words=20, formatted=False)
        topics_words = [(tp[0], [wd[0] for wd in tp[1]]) for tp in x]
        dictionary.id2token = {v: k for k, v in dictionary.token2id.items()}
        # Below Code Prints Topics and Words
        for topic, words in topics_words:
            f.write(str(topic) + " :: " + str([dictionary.id2token[int(w)] for w in words]) + "\n")
        f.write("\n")

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    my_topic.py                                        :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/14 11:08:14 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/06/17 14:28:11 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import pprint
import re
import sqlite3
import string
from collections import Counter
from pprint import pprint

# Gensim
import gensim
import gensim.corpora as corpora
import matplotlib
import numpy as np
import pandas as pd
import scipy
# spacy for lemmatization
import spacy
from gensim.corpora import Dictionary
from gensim.models import CoherenceModel, LdaModel
from gensim.test.utils import datapath
from gensim.utils import simple_preprocess
from nltk.corpus import stopwords
from tqdm import tqdm
from tqdm import tqdm_notebook as tqdm

from fake_identify import Are_you_IRA
from my_weapon import *
from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer

matplotlib.rcParams["font.size"] = 14
sns.set_style("darkgrid")
ira_c = sns.color_palette("coolwarm", 8)[7]
all_c = sns.color_palette("coolwarm", 8)[0]

Putin = Are_you_IRA()

nlp = spacy.load('en', disable=['parser', 'ner'])

tokenizer = CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True,
                                 strip_handles=False,
                                 normalize_usernames=False,
                                 normalize_urls=True,
                                 keep_allupper=False)

from nltk.corpus import stopwords
stop_words = stopwords.words('english')
stop_words.extend([
    "rt", "…", "...", "URL", "http", "https", "“", "”", "‘", "’", "get", "2", "new", "one", "i'm", "make",
    "go", "good", "say", "says", "know", "day", "..", "take", "got", "1", "going", "4", "3", "two", "n",
    "like", "via", "u", "would", "still", "first", "that's", "look", "way", "last", "said", "let",
    "twitter", "ever", "always", "another", "many", "things", "may", "big", "come", "keep",
    "5", "time", "much", "_", "cound", "-", '"'
])

stop_words.extend([',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$', '%',
])


                                 
def abandon():
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


class KTopic(object):
    def __init__(self):
        print("你若相信，它便存在。")


    def load_text(self):
        print("Loading ...")
        texts_out = []

        with open("data/ira_texts.txt") as f:
            for line in f:
                w = [w for w in line.strip().split() if w != "" and w != "%"]
                if w:
                    texts_out.append(w)

        # Create Dictionary
        self.id2word = corpora.Dictionary(texts_out)

        # Create Corpus
        self.texts = texts_out

        # Term Document Frequency
        self.corpus = [self.id2word.doc2bow(text) for text in texts_out]


    def load_model(self):
        self.lda_model = LdaModel.load("model/lda-ira-78.mod")


    def lemmatization(self, sent, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV', 'PROPN']):
        """https://spacy.io/api/annotation"""
        sent = " ".join(sent)
        sent = re.sub(r'#(\w+)', r'itstopiczzz\1', sent)
        sent = re.sub(r'@(\w+)', r'itsmentionzzz\1', sent)
        doc = nlp(sent)
        
        _d = [token.lemma_ for token in doc if token.pos_ in allowed_postags and token.lemma_ not in stop_words and token.lemma_]
        
        _d = [x.replace('itstopiczzz', '#') for x in _d]
        _d = [x.replace('itsmentionzzz', '@') for x in _d]
        return _d
    

    def predict(self, text):
        text = text.replace("\n", " ").replace("\t", " ")
        words = tokenizer.tokenize(text)
        words = [w for w in words if w not in stop_words and w]
        text = self.lemmatization(words)
        text = self.id2word.doc2bow(text)
        return self.lda_model.get_document_topics(text)


    def run(self):
        for i in range(100):
            print(f"---------------------- {i} ----------------------")
            # Can take a long time to run.
            lda_model = gensim.models.ldamodel.LdaModel(corpus=self.corpus, id2word=self.id2word, num_topics=7, chunksize=1000)
            print(lda_model.print_topics())
            # Compute Perplexity
            print('Perplexity: ', lda_model.log_perplexity(self.corpus))  # a measure of how good the model is. lower the better.

            # Compute Coherence Score
            coherence_model_lda = CoherenceModel(model=lda_model, texts=self.texts, dictionary=self.id2word, coherence='c_v')
            coherence_lda = coherence_model_lda.get_coherence()
            print('Coherence Score: ', coherence_lda)
            
            lda_model.save(f"model/lda-ira-{i}.mod")


if __name__ == "__main__":
    Lebron = KTopic()
    Lebron.load_text()
    Lebron.run()

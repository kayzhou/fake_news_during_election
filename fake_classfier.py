# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-02-24 16:42:55

import gc
from itertools import chain
from random import sample

from nltk import ngrams
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split
from pathlib import Path

import SQLite_handler
from joblib import dump, load
from my_weapon import *
from myclf import *
from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer
from Trump_Clinton_Classifer.TwSentiment import (bag_of_words,
                                                 bag_of_words_and_bigrams)


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
        all_tweets = pd.read_csv("disk/all-tweets.csv", dtype=str, usecols=["tweet_id", "c_alex"])
        print("finished!")

        labels = [
            "fake",
            "extreme bias (right)",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",
            "extreme bias (left)"
        ]

        for label in labels:
            print(label, "...")
            tweets_id = all_tweets[all_tweets["c_alex"] == label].tweet_id
            rst = SQLite_handler.find_tweets(tweets_id)
            print(len(rst))
            with open("disk/train_data_fake/{}.txt".format(label), "w") as f:
                for d in rst:
                    if "text" not in d:
                        continue
                    elif d["text"].startswith("RT"):
                        continue
                    f.write(d["text"] + "\n")

    def get_tokens(self):
        """
        text > tokens
        """
        tokenizer = CustomTweetTokenizer()

        labels = [
            "fake",
            "extreme bias (right)",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",
            "extreme bias (left)"
        ]

        for label in labels:
            print(label, "...")
            with open("disk/tokens_fake/{}.txt".format(label), "w") as f:
                for line in open("disk/train_data_fake/{}.txt".format(label)):
                    words = tokenizer.tokenize(line.strip())
                    if len(words) > 0:
                        f.write(" ".join(words) + "\n")

    def train(self):
        """
        fake, non-fake
        fake, left, center, right √ 优先
        left, center, right
        """
        # read data
        X = []
        y = []

        labels = [
            "fake",
            "extreme bias (right)",
            "right",
            "right leaning",
            "center",
            "left leaning",
            "left",
            "extreme bias (left)"
        ]

        w_of_categories = [[], [], [], []]

        for label in labels:
            print(label, "...")

            if label == "fake":
                y_i = 0
            elif label in ["extreme bias (right)", "right", "right leaning"]:
                y_i = 1 
            elif label == "center":
                y_i = 2 
            elif label in ["extreme bias (left)", "left", "left leaning"]:
                y_i = 3

            for i, line in enumerate(open("disk/tokens_fake/{}.txt".format(label))):
                w = line.strip().split(" ")
                if len(w) > 0 and w[0] != "RT":
                    w_of_categories[y_i].append(w)
                    # X.append(bag_of_words_and_bigrams(w))
                    # # print(X[-1])
                    # y.append(y_i)

        for i in range(len(w_of_categories)):
            print("len of category:", len(w_of_categories[i]))
            w_of_categories[i] = sample(w_of_categories[i], 1000000)
            for w in w_of_categories[i]:
                X.append(bag_of_words_and_bigrams(w))
                y.append(i)

        print("Reading data finished! count:", len(y))

        # split train and test data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)
        
        print("Splitting data finished!")

        # build one hot embedding
        v = DictVectorizer(dtype=np.int8, sparse=True, sort=False)
        X_train = v.fit_transform(X_train)
        X_test = v.transform(X_test)
        
        dump(v, 'model/20190401-DictVectorizer.joblib')
        print("Building word embedding finished!")
        print(X_train[0].shape, X_train[1].shape)
        print(X_train.shape, X_test.shape)

        # machine learning model
        list_classifiers = ['LR', 'NB', 'SVC']
        # list_classifiers = ['GBDT']
        classifiers = {
            'NB': naive_bayes_classifier,
            'KNN': knn_classifier,
            'LR': logistic_regression_classifier,
            'RF': random_forest_classifier,
            'DT': decision_tree_classifier,
            'SVM': svm_classifier,
            'SVMCV': svm_cross_validation,
            'GBDT': gradient_boosting_classifier,
            'SVC': svm_linear_classifier,
        }

        for classifier in list_classifiers:
            print('******************* {} ********************'.format(classifier))
            if classifier == "LR":
                clf = LogisticRegression(penalty='l2', multi_class="multinomial", solver="sag", max_iter=10e8)
                clf.fit(X_train, y_train)
            elif classifier == "GBDT":
                clf = GradientBoostingClassifier(learning_rate=0.1, max_depth=3)
                clf.fit(X_train, y_train)
            else:
                clf = classifiers[classifier](X_train, y_train)
            # print("fitting finished! Lets evaluate!")
            self.evaluate(clf, X_train, y_train, X_test, y_test)
            dump(clf, 'model/20190401-{}.joblib'.format(classifier))


    def evaluate(self, clf, X_train, y_train, X_test, y_test):
        # CV
        print('accuracy of CV=10:', cross_val_score(clf, X_train, y_train, cv=5).mean())

        # 模型评估
        y_pred = clf.predict(X_test)
        print(classification_report(y_test, y_pred))


    def predict(self):
        tokenizer = CustomTweetTokenizer()
        v = load('model/20190401-DictVectorizer.joblib')
        clf = load('model/20190401-LR.joblib')
        ele_tweets = pd.read_csv('data/ira-tweets-ele.csv', dtype=str)

        X = []
        uids = []
        batch_size = 1000

        with open("data/ira_predicted_tweets.txt", "w") as f:
            for i, row in ele_tweets.iterrows():
                uids.append(row["userid"])
                text = d["tweet_text"].replace("\n", " ").replace("\t", " ")
                words = bag_of_words_and_bigrams(tokenizer.tokenize(text))
                y = clf.predict(words)
                X.append(words)

                if len(X) >= batch_size:
                    # print(X)
                    X = v.transform(X)
                    y = clf.predict_proba(X)
                    for i in range(len(y)):
                        f.write("{},{},{}\n".format(uids[i], y[i]))
                    X = []
                    uids = []


if __name__ == "__main__":
    Lebron = Fake_Classifer()
    # Lebron.get_train_data()
    # Lebron.get_tokens()
    # Lebron.train()
    Lebron.predict()

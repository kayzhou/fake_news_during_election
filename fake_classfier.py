# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-02-24 16:42:55

from itertools import chain

from nltk import ngrams
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder

import SQLite_handler
from my_weapon import *
from myclf import *
from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer
from Trump_Clinton_Classifer.TwSentiment import bag_of_words, bag_of_words_and_bigrams


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

            for i, line in enumerate(open("disk/tokens_fake/{}.txt".format(_type))):
                if i > 2:
                    break
                w = line.strip().split()
                print(bag_of_words_and_bigrams(w))
                X.append(np.array(w))
                y.append(y_i)
        print("reading data finished!")

        # split train and test data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)
        print("splitting data finished!")

        # build one hot embedding
        one_hot = OneHotEncoder()
        one_hot.fit(X_train)
        X_train = one_hot.transform(X_train)
        X_test = one_hot.transform(X_test)

        # machine learning model
        list_classifiers = ['LR', 'GBDT', 'NB', 'RF']
        classifiers = {
            'NB':naive_bayes_classifier,
            'KNN':knn_classifier,
            'LR':logistic_regression_classifier,
            'RF':random_forest_classifier,
            'DT':decision_tree_classifier,
            'SVM':svm_classifier,
            'SVMCV':svm_cross_validation,
            'GBDT':gradient_boosting_classifier
        }

        for classifier in list_classifiers:
            print('******************* {} ********************'.format(classifier))
            if classifier == "GBDT":
                clf = GradientBoostingClassifier(learning_rate=0.1, max_depth=5)
                clf.fit(X_train, y_train)
            if classifier == "LR":
                clf = LogisticRegression(penalty='l2')
                clf.fit(X_train, y_train)
            else:
                clf = classifiers[classifier](X_train, y_train)
            self.evaluate(clf, X, y, X_test, y_test)


        original_params = {'n_estimators': 1000, 'max_leaf_nodes': 4, 'max_depth': 3, 'random_state': 23,
                        'min_samples_split': 5}

        # for GDBT
        for i, setting in enumerate([{'learning_rate': 1.0, 'subsample': 1.0},
                        {'learning_rate': 0.1, 'subsample': 1.0},
                        {'learning_rate': 1.0, 'subsample': 0.5},
                        {'learning_rate': 0.1, 'subsample': 0.5},
                        {'learning_rate': 0.1, 'max_features': 2}]):
            print('******************* {} ********************'.format(i))
            params = dict(original_params)
            params.update(setting)

            clf = GradientBoostingClassifier(**params)
            clf.fit(X_train, y_train)
            self.evaluate(clf, X, y, X_test, y_test)

        original_params = {}

        # LinearSVC
        for i, setting in enumerate([{'C':0.125}, {'C': 0.25}, {'C':0.5}, {'C':1.0}, {'C':2.0}, {'C': 4.0}, {'C':8.0}]):
            print('******************* {} ********************'.format(i))
            print(setting)
            params = dict(original_params)
            params.update(setting)

            clf = LinearSVC(**params)
            clf.fit(X_train, y_train)
            self.evaluate(clf, X, y, X_test, y_test)


    def evaluate(self, clf, X, y, X_test, y_test):
        # CV
        print('accuracy of CV:', cross_val_score(clf, X, y, cv=5).mean())

        # 模型评估
        y_pred = []
        for i in range(len(X_test)):
            y_hat = clf.predict(X_test[i].reshape(1, -1))
            y_pred.append(y_hat[0])
        print(classification_report(y_test, y_pred))


if __name__ == "__main__":
    Lebron = Fake_Classifer()
    # get_train_data()
    # Lebron.get_tokens()
    Lebron.train()

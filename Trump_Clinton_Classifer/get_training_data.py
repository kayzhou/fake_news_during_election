#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/
"""

from sklearn.cross_validation import train_test_split


def to_file(_data, in_name):
    with open(in_name, "w") as f:
        for _d in _data:
            f.write(_d)


def load(in_name):
    X = []
    for line in open(in_name):
        X.append(line)
    return X

data = load("data/0.txt")
train, test = train_test_split(data, test_size=10000, random_state=41)
to_file(train, "data/0-train.txt")
to_file(test, "data/0-test.txt")

data = load("data/1.txt")
train, test = train_test_split(data, test_size=10000, random_state=41)
to_file(train, "data/1-train.txt")
to_file(test, "data/1-test.txt")

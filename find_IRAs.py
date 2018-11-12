#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 15:55:59
@author: https://kayzhou.github.io/
"""

import pandas as pd
import sqlite3

data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"])

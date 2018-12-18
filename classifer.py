# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 15:14:41 2016

@author: alex

Train and test a text classifier:

tokenization and preprocessing of the tweet's text is done with NLTK
and the classification is done with sklearn
Gridsearch to find the optimal parameters

"""

import sys
import os
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from sklearn.externals import joblib
from TwClassifier import TweetClassifier

from tqdm import tqdm
import pickle
import time
import numpy as np
import sqlite3
import pandas as pd
from datetime import datetime

#%% trump hillary june sep

# sqlite_file = '../databases_ssd/complete_trump_vs_hillary_db.sqlite'
## ~~ savedir = '/home/alex/network_workdir/elections/tweet_classification/trump_vs_hillary_june_sep_signi/trained_classifier/'

# suffix = '26sep_6nov'
# suffix = '26sep_9nov'

# suffix = '_initial_htgs'
suffix = '_final_htgs'

# sqlite_file_class_proba = '../databases_ssd/complete_trump_vs_hillary_class_proba' + suffix +'_db.sqlite'

# classify tweets or retweets
# CLASS_RETWEETS = True


#%% trump hillary sep nov

#sqlite_file = '../databases/complete_trump_vs_hillary_sep-nov_db.sqlite'
savedir = '/home/alex/network_workdir/elections/tweet_classification/trump_vs_hillary_sep-nov/trained_classifier/'
#
#suffix = '_final_htgs'
##
#sqlite_file_class_proba = '../databases_ssd/complete_trump_vs_hillary_sep-nov_class_proba' + suffix +'_db.sqlite'
#
## classify tweets or retweets
#CLASS_RETWEETS = True


#%% trump hillary sep nov noveau

#sqlite_file = '../databases_ssd/complete_trump_vs_hillary_sep-nov_db_noveau.sqlite'
#
#savedir = 'trump_vs_hillary_sep-nov/trained_classifier/'
#
#suffix = '_final_htgs'
##
#sqlite_file_class_proba = '../databases_ssd/complete_trump_vs_hillary_sep-nov_noveau_class_proba' + suffix +'_db.sqlite'
#
## classify tweets or retweets
#CLASS_RETWEETS = False

#%% trump hillary sep nov using june_sep classifier

#sqlite_file = '../databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite'
#
#savedir = 'trump_vs_hillary_june_sep_signi/trained_classifier/'
#
#suffix = '_final_htgs'
#
#sqlite_file_class_proba = '../databases/complete_trump_vs_hillary_sep-nov_class_proba' + suffix +'_june_sep_db.sqlite'
#
## classify tweets or retweets
#CLASS_RETWEETS = True


#%% climate change

#sqlite_file = '../databases_ssd/climate_change.sqlite'
#
#savedir = 'climate_change/trained_classifier/'
#
#suffix = '_final_htgs'
#
## where to store the classification probability results
#sqlite_file_class_proba = '../databases_ssd/climate_change_class_proba' + suffix +'_db.sqlite'
#
## classify tweets or retweets
#CLASS_RETWEETS = True


#%% trump hillary june sep rand

#sqlite_file = '../databases_ssd/complete_trump_vs_hillary_db.sqlite'
#savedir = 'trump_vs_hillary_june_sep_signi_rnd09_3/trained_classifier/'
#
#suffix = '_final_htgs_rnd09_3'
#
#sqlite_file_class_proba = '../databases_ssd/complete_trump_vs_hillary_class_proba' + suffix +'_db.sqlite'
#
#propa_col_name = 'p_pro_hillary_anti_trump'
#
#stop_date = None
#
# #classify tweets or retweets
#CLASS_RETWEETS = True

#%% trump hillary june sep no intercept

#sqlite_file = '../databases_ssd/complete_trump_vs_hillary_db.sqlite'
#savedir = 'trump_vs_hillary_june_sep_signi/trained_classifier/'
#
#suffix = '_final_htgs_no_int'
#
#sqlite_file_class_proba = '../databases_ssd/complete_trump_vs_hillary_class_proba' + suffix +'_db.sqlite'
#
#propa_col_name = 'p_pro_hillary_anti_trump'
#
#stop_date = None
#
# #classify tweets or retweets
#CLASS_RETWEETS = True

#%% climate change no intercept

# sqlite_file = '../databases/climate_change.sqlite'

# savedir = 'climate_change/trained_classifier/'

# suffix = '_final_htgs_no_int'

# where to store the classification probability results
# sqlite_file_class_proba = '../databases/climate_change_class_proba' + suffix +'_db.sqlite'

# classify tweets or retweets
# CLASS_RETWEETS = False


#%%
def is_discuss_trump_hillary(row):
    words = set(str(row["tweet_text"]).lower().strip().split())

    if "trump" in words or "realdonaldtrump" in words or "@realdonaldtrump" in words or "donaldtrump" in words:
        return True
    elif "hillary" in words or "clinton" in words or "hillaryclinton" in words or "@hillaryclinton" in words:
        return True
    else:
        return False

savedir = '/home/alex/network_workdir/elections/tweet_classification/trump_vs_hillary_june_sep_signi/trained_classifier/'
filename = 'sklearn_SGDLogReg_' + suffix + '.pickle'

print('loading ' + savedir + filename)
cls = joblib.load(savedir + filename)

classifier = cls['sklearn_pipeline']
label_inv_mapper = cls['label_inv_mapper']
TweetClass = TweetClassifier(classifier=classifier, label_inv_mapper=label_inv_mapper)

tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv', low_memory=False)
tws = tweets[tweets["tweet_time"] >= "2016-06-01 00:00"][tweets["tweet_time"] < "2016-09-01 00:00"]

with open("data/IRA-pro-trump.txt", "a") as f:
    for i, row in tqdm(tws.iterrows()):
        if is_discuss_trump_hillary(row):
            line = row["tweet_text"]
            predict_proba = TweetClass.classify_text(line, return_pred_labels=False)
            f.write("{},{}\n".format(row["tweetid"], predict_proba[0]))


savedir = '/home/alex/network_workdir/elections/tweet_classification/trump_vs_hillary_sep-nov/trained_classifier/'
filename = 'sklearn_SGDLogReg_' + suffix + '.pickle'

print('loading ' + savedir + filename)
cls = joblib.load(savedir + filename)

classifier = cls['sklearn_pipeline']
label_inv_mapper = cls['label_inv_mapper']
TweetClass = TweetClassifier(classifier=classifier, label_inv_mapper=label_inv_mapper)

# tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv', low_memory=False)
tws = tweets[tweets["tweet_time"] >= "2016-09-01 00:00"][tweets["tweet_time"] < "2016-11-09 00:00"]

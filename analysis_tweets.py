import pandas as pd
from urllib.parse import urlparse
import requests
import networkx as nx
import json
import sys, traceback
from multiprocessing

# short_url = set(['bit.ly', 'dlvr.it', 'goo.gl', 'j.mp', 'ift.tt', 'nyp.st', 'ln.is', 'trib.al', 'cnn.it', 'youtu.be'])



def task(_ids):
    # tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv', usecols=['urls', 'tweetid'], nrows=100)
    # # tweets['hostname'] = -1
    # print(len(tweets))
    # for i, row in tweets.iterrows():
    #     # print(i, row, type(row), row['urls'], type(row['urls']))
    #     if not isinstance(row['urls'], str):
    #         # tweets.drop(i, inplace=True)
    #         pass
    #     elif row['urls'][1: -1] == '':
    #         # tweets.drop(i, inplace=True)
    #         pass
    #     else:
    #         try:
    #             url = row['urls'][1: -1]
    #             hostname = urlparse(url).hostname
    #             # short url
    #             if len(hostname) <= 7:
    #                 res = requests.head(url)
    #                 hostname = urlparse(res.headers.get('location')).hostname
    #             if hostname:
    #                 # print(i, hostname)
    #                 tweets['hostname'] = hostname
    #                 print(i, url, hostname, sep='\t')
    #             # else:
    #             #     tweets.drop(i, inplace=True)
    #         except Exception as e:
    #             # pass
    #             traceback.print_exc(file=sys.stdout)
    #             # print(i, e)
    for d in _ids:
        if d['short']:
            res = requests.head(d['url'])
            hostname = urlparse(res.headers.get('location')).hostname
            d['host'] = hostname
        print(d['id'], d['url'], d['hostname'], d['short'], sep='\t')


def keep_url():
    tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv', usecols=['urls', 'tweetid'])
    # tweets['hostname'] = -1
    print(len(tweets))


    # for i, row in tweets.iterrows():
    #     if not isinstance(row['urls'], str):
    #         continue
    #     elif row['urls'][1: -1] == '':
    #         continue
    #     else:
    #         url = row['urls'][1: -1]
    #         hostname = urlparse(url).hostname
    #         print(i, url, hostname, sep='\t')

    dict_id_host = []
    for line in open('id_host-20181021.txt'):
        _id, url, hostname = line.strip().split('\t')
        if len(url) <= 10 and len(hostname) <=30 and len(hostname.split('.')[1]) == 2:
            d = {
                'id': _id,
                'url': url,
                'hostname': hostname,
                'short': True
            }
        else:
            d = {
                'id': _id,
                'url': url,
                'hostname': hostname,
                'short': False
            }

    task_cnt = 10
    step = int(len(dict_id_host) / task_cnt)
    for i in range(task_cnt):
        if i == task_cnt - 1:
            _ids = s[i * step:]
        else:
            _ids = dict_id_host[i * step: (i + 1) * step]
        t = multiprocessing.Process(target=task, args=(_ids,))
        t.start()

def build_retweet_network():
    # userid_id = {}
    # users = pd.read_csv('data/ira_users_csv_hashed.csv')
    # for i, row in users.iterrows():
    #     userid_id[row['userid']] = i
    # json.dump(userid_id, open("data/userid_mapping.json", "w"), indent=4)
    userid_map = json.load(open("data/userid_mapping.json"))
    G = nx.Graph()
    G.add_nodes_from(userid_map.values())


    # tweets = pd.read_csv('data/ira_tweets_head.csv', dtype=str)
    # for i, row in tweets.iterrows():
    #     print(i, row)


if __name__ == "__main__":
    keep_url()
    # build_retweet_network()


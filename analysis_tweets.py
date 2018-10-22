import pandas as pd
from urllib.parse import urlparse
import requests
import networkx as nx
import json
import sys, traceback
import multiprocessing
import time

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
            try:
                res = requests.head(d['url'])
                hostname = urlparse(res.headers.get('location')).hostname
                d['hostname'] = hostname
            except:
                d['error'] = 1
            time.sleep(0.1)
        print(json.dumps(d, ensure_ascii=False))


def keep_url():
    # tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv', usecols=['urls', 'tweetid'])
    # # tweets['hostname'] = -1
    # print(len(tweets))


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
    cnt = 0
    for line in open('id_host-20181021.txt'):
        _id, url, hostname = line.strip().split('\t')
        if len(url) <= 30 and len(hostname) <= 10:
            cnt += 1
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
        dict_id_host.append(d)

    # print(cnt); exit(0)

    task_cnt = 10
    step = int(len(dict_id_host) / task_cnt)
    for i in range(task_cnt):
        if i == task_cnt - 1:
            _ids = dict_id_host[i * step:]
        else:
            _ids = dict_id_host[i * step: (i + 1) * step]
        t = multiprocessing.Process(target=task, args=(_ids,))
        t.start()


def temp():
    """
    mutiprocess > solve
    """

    """
    ids = set([])
    for i, line in enumerate(open('id_host_short-20181021.txt')):
        # ids.add(json.loads(line.strip())['id'])
        line = line.strip()
        cnt += line.count("{")
        if line.count("{") == 1:
            out_file.write(line + '\n')
        else:
            print(line.count("{"), line)
            ws = line.split("}")
            for w in ws:
                if w != '':
                    out_file.write(w + '}\n')
    print("cnt:", cnt)
    exit(0)
    """

    ids = set([])
    with open('id_host_short.txt') as f:
        for line in f:
            if line != ' ':
                # print(i, line)
                ids.add(json.loads(line.strip())['id'])
 
    dict_id_host = []
    for line in open('id_host-20181021.txt'):
        _id, url, hostname = line.strip().split('\t')
        if _id in ids:
            continue
        if len(url) <= 30 and len(hostname) <= 10:
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
        dict_id_host.append(d)
    print(len(dict_id_host))

    with open('id_host_short.txt', 'a') as f:
        for d in dict_id_host:
            if d['short']:
                try:
                    res = requests.head(d['url'])
                    hostname = urlparse(res.headers.get('location')).hostname
                    d['hostname'] = hostname
                except:
                    d['error'] = 1
                time.sleep(0.1)
            f.write(json.dumps(d, ensure_ascii=False) + "\n")


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
    # keep_url()
    temp()
    # build_retweet_network()


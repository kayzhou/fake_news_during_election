import pandas as pd
from urllib.parse import urlparse
import requests
import networkx as nx
import json
import sys, traceback
import multiprocessing
import time
from unshortenit import UnshortenIt
from tqdm import tqdm
import sqlite3


short_url = set(['bit.ly', 'dlvr.it', 'goo.gl', 'j.mp', 'ift.tt', 'nyp.st', 'ln.is', 'trib.al', 'cnn.it', 'youtu.be'])


def get_urls():
    tweets = pd.read_csv('data/ira-tweets-ele.csv')
    print(len(tweets))

    for i, row in tweets.iterrows():
        # print(i, row, type(row), row['urls'], type(row['urls']))
        if not isinstance(row['urls'], str):
            # tweets.drop(i, inplace=True)
            pass
        elif row['urls'][1: -1] == '':
            # tweets.drop(i, inplace=True)
            pass
        else:
            try:
                url = row['urls'][1: -1]
                hostname = urlparse(url).hostname
                print(i, row["tweetid"], url, hostname, sep='\t')
            except Exception as e:
                # pass
                traceback.print_exc(file=sys.stdout)
                # print(i, e)

def task(_ids):
    unshortener = UnshortenIt()

    with open("IRA-final-url.json", "w") as f:
        for d in tqdm(_ids):
            # print("input", d)
            # if d['short']:
            if "error" in d and d["error"]:
                try:
                    d["error"] = False
                    url = unshortener.unshorten(d["url"])
                    d["real_url"] = url
                    hostname = urlparse(url).hostname
                    d['hostname'] = hostname
                except Exception as e:
                    d['error'] = True
            f.write(json.dumps(d, ensure_ascii=False) + "\n")


def keep_url():
    dict_id_host = []
    cnt = 0
    for line in open('data/id_url_hostname.csv'):
        _id, tweetid, url, hostname = line.strip().split('\t')
        if len(hostname) <= 10:
            cnt += 1
            d = {
                'id': _id,
                'tweetid': tweetid,
                'url': url,
                'hostname': hostname,
                'real_url': url,
                'short': True
            }
        else:
            d = {
                'id': _id,
                'url': url,
                'tweetid': tweetid,
                'hostname': hostname,
                'real_url': url,
                'short': False
            }
        dict_id_host.append(d)
    task(dict_id_host)

    """
    task_cnt = 5
    step = int(len(dict_id_host) / task_cnt)
    for i in range(task_cnt):
        if i == task_cnt - 1:
            _ids = dict_id_host[i * step:]
        else:
            _ids = dict_id_host[i * step: (i + 1) * step]
        t = multiprocessing.Process(target=task, args=(_ids,))
        t.start()
    """

def again():
    dict_id_host = []
    for line in open("IRA-final-url.json"):
        d = json.loads(line.strip())
        dict_id_host.append(d)
    task(dict_id_host)


if __name__ == "__main__":
    get_urls()
    # keep_url()
    # again()
    # temp()


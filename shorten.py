# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-03-08 17:01:02

import json
import multiprocessing
import os
import sqlite3
import sys
import time
import traceback
from urllib.parse import urlparse

import networkx as nx
import pandas as pd
import requests
from tqdm import tqdm

import tldextract
from unshortenit import UnshortenIt


def get_urls():
    """
    提取需要分析的URL
    """
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


def get_hostname_from_url(url):
    return ".".join(tldextract.extract(url)[1:])


def task(_ids):
    print(f"{os.getpid()} task starts ... ", len(_ids))
    unshortener = UnshortenIt(default_timeout=20)
    new_ids = []
    for d in tqdm(_ids):
        # if "error" in d and d["error"]:
        #     print(d)
        try:
            d["error"] = False
            if d["short"]:
                # print(d)
                url = unshortener.unshorten(d["url"])
                d["final_url"] = url
                d['hostname'] = get_hostname_from_url(url)
        except Exception as e:
            # print(e); traceback.print_exc(sys.stdout)
            d['error'] = True

        new_ids.append(d)
    write2json(new_ids)

    return new_ids


def write2json(new_ids):
    print("writing ... ...")
    with open("data/ira-urls-plus-20190423.json", "a") as f:
        for d in new_ids:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
    print("finished!")


def remove_duplication():
    tweets = { json.loads(line.strip())["tweetid"]: json.loads(line.strip()) for line in open("data/ira-final-urls-plus.json") }
    new_ids = tweets.values()
    write2json(new_ids)


def unshorten_url():
    """
    main
    """
    tweet_ids_have_dealed = set([json.loads(line.strip())["tweetid"] for line in open("data/ira-urls-plus-20190423.json")])
    ignore = set(["twitter.com", "youtube.com", "instagram.com", "facebook.com", "kron4.com"])

    dict_id_host = []
    # for line in open('data/ira-final-url.json'):
    for line in open("data/IRA-en-urls.json"):
        # _id, tweetid, url, hostname = line.strip().split('\t')
        r = json.loads(line.strip())
        tweetid = str(r["tweetid"])

        if tweetid in tweet_ids_have_dealed:
            continue

        url = r["urls"]
        d = {
            'tweetid': tweetid,
            'url': url,
            'hostname': get_hostname_from_url(url),
            'final_url': url,
            'short': False,
        }
        if d["hostname"] not in ignore:
            d["short"] = True
        if d["url"] in ["http://listen.radionomy.com/ny2la", "http://ht.ly/XKLW4"]:
            d["short"] = False

        dict_id_host.append(d)

    print("需要处理：", len(dict_id_host))

    # task(dict_id_host)
    # return 0

    # test
    # dict_id_host = dict_id_host[:80]

    task_cnt = 8
    step = int(len(dict_id_host) / task_cnt)
    pool = multiprocessing.Pool()
    for i in range(task_cnt):
        if i < task_cnt - 1:
            _ids = dict_id_host[i * step: (i + 1) * step]
        elif i == task_cnt - 1:
            _ids = dict_id_host[i * step:]
        pool.apply_async(task, (_ids,))

    pool.close()
    pool.join()


def deal_with_error():
    new_ids = []
    unshortener = UnshortenIt(default_timeout=20)
    for line in tqdm(open("data/ira-urls-plus-1.json")):
        d = json.loads(line.strip())
        if "error" in d and d["error"] and d["hostname"] not in ["blackmattersus.com", "blacktolive.org"]:
            try:
                url = unshortener.unshorten(d["url"])
                d["final_url"] = url
                d['hostname'] = get_hostname_from_url(url)
                del d["error"]
            except Exception as e:
                print(d["url"])
        
        new_ids.append(d)
    write2json(new_ids)


if __name__ == "__main__":
    # remove_duplication()
    # get_urls()
    unshorten_url()

    # deal_with_error()

#-*- coding: utf-8 -*-

"""
Created on 2018-11-12 16:09:49
@author: https://kayzhou.github.io/
"""

import sqlite3
import json
import queue
from tqdm import tqdm


def find_fake_tweets():
    host_label = json.load(open('data/host_label.json'))
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/urls_db.sqlite")
    c = conn.cursor()
    c.execute('''SELECT * FROM urls;''')
    col_names = [t[0] for t in c.description]
    data = c.fetchall()

    with open("data/tweets_fake_news.txt", "w") as f:
        for i, d in enumerate(data):
            if i % 10000 == 0:
                print(i, d)
            if d[8] in host_label and host_label[d[8]] == "fake":
                json_d = {k: v for k, v in zip(col_names, d)}
                json_d = json.dumps(json_d, ensure_ascii=False)
                f.write(json_d + '\n')


def load_all_nodes():

    tweets_ids = set([int(json.loads(line.strip())["tweet_id"]) for line in open("data/fake.txt")])
    print(len(tweets_ids))
    for line in open("data/retweet_network_1.txt"):
        n1, n2 = line.strip().split("\t")
        tweets_ids.add(int(n1))
        tweets_ids.add(int(n2))
    print(len(tweets_ids))
    for line in open("data/retweet_network_2.txt"):
        n1, n2 = line.strip().split("\t")
        tweets_ids.add(int(n1))
        tweets_ids.add(int(n2))
    print(len(tweets_ids))
    return tweets_ids


def find_links(tweets_ids):

    retweet_link = {}

    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    cnt = 0

    for _id in tqdm(tweets_ids):
        cnt += 1
        if cnt % 50000 == 0:
            print(cnt, "；边的数量：", len(retweet_link))

        # 找到谁转发了我？
        c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
        for next_d in c.fetchall():
            next_id = str(next_d[0])
            retweet_link[next_id] = str(_id)
    conn.close()

    # 下一个！
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    for _id in tqdm(tweets_ids):
        cnt += 1
        if cnt % 50000 == 0:
            print(cnt, "；边的数量：", len(retweet_link))

        # 找到谁转发了我？
        c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
        for next_d in c.fetchall():
            next_id = str(next_d[0])
            retweet_link[next_id] = str(_id)
    conn.close()

    json.dump(retweet_link, open("data/retweet_network_fake.json", "w"), ensure_ascii=False, indent=2)


def load_fake_news_source():
    data = json.load(open("data/retweet_network_fake.json"))
    sources = [v for v in data.values()]
    return sources


def load_fake_news():
    fake_news = set()
    data = json.load(open("data/retweet_network_fake.json"))
    for k, v in data.items():
        fake_news.add(k)
        fake_news.add(v)
    return list(fake_news)


def load_fake_news_not_original():
    fake_news = set()
    data = json.load(open("data/retweet_network_fake.json"))
    for k, v in data.items():
        fake_news.add(k)
        fake_news.add(v)
    return list(fake_news)


def get_tweets(tweets_ids):

    tweet_data = {}

    cnt = 0
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    for _id in tweets_ids:
        what_the_fuck = False
        if cnt % 10000 == 0:
            print(cnt)
        new_d = {}
        cnt += 1

        c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
        c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))

        d = c1.fetchone()
        if d:
            col_name = [t[0] for t in c1.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v

        else:
            c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
            c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))

            d = c2.fetchone()
            if d:
                new_d = {}
                col_name = [t[0] for t in c1.description]
                # print(d)
                for k, v in zip(col_name, d):
                    new_d[k] = v
            else:
                print("两个库里面都没有该tweet？？", _id)
                what_the_fuck = True

        if not what_the_fuck:
            c1.execute('''SELECT * FROM user WHERE user_id={};'''.format(new_d["user_id"]))
            d = c1.fetchone()
            if d:
                col_name = [t[0] for t in c1.description]
                # print(d)
                for k, v in zip(col_name, d):
                    new_d[k] = v

            else:
                c2.execute('''SELECT * FROM user WHERE user_id={};'''.format(new_d["user_id"]))
                d = c2.fetchone()
                if d:
                    col_name = [t[0] for t in c1.description]
                    # print(d)
                    for k, v in zip(col_name, d):
                        new_d[k] = v
                # else:
                    # print("两个库里面都没有该user！", new_d["user_id"])
        tweet_data[_id] = new_d

    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/urls_db.sqlite")
    c = conn.cursor()
    for _id in tqdm(tweets_ids):
        c.execute("select * from urls where tweet_id={}".format(_id))
        col_name = [t[0] for t in c.description]
        d = c.fetchone()
        if d:
            for k, v in zip(col_name, d):
                tweet_data[_id][k] = v
        else:
            print("居然没有这条tweet的信息！说明这条tweet并没有入库？", _id)


    with open("fake_news_tweets.txt", "w") as f:
        for _id in tweets_ids:
            line = json.dumps(tweet_data[_id], ensure_ascii=False)
            f.write(line + "\n")


if __name__ == "__main__":

    # 找出所有fake_news
    # find_fake_tweets()

    t_ids = set([int(json.loads(line.strip())["tweet_id"]) for line in open("data/tweets_fake.txt")])
    # print(len(t_ids))
    find_links(t_ids)

    # tids = load_fake_news_source()
    # get_tweets(tids)

    # 获取所有fake news相关的信息
    # tids = load_fake_news()
    # print(len(tids))
    # get_tweets(tids)
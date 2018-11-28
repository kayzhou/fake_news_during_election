#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:03:07
@author: https://kayzhou.github.io/
"""

import sqlite3
from tqdm import tqdm
import pendulum


def find_tweet(_id):
    new_d = {}

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v
        conn1.close()
        return new_d

    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        conn2.close()

    if not new_d:
        new_d = find_retweeted(_id)


    return new_d


def find_retweeted(_id):
    new_d = {}

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        conn2.close()

    conn1.close()
    return new_d


def find_user(_id):
    new_d = {}

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT * FROM user WHERE user_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
        for k, v in zip(col_name, d):
            new_d[k] = v


    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT * FROM user WHERE user_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        conn2.close()

    conn1.close()
    return new_d


def find_original_tweetid(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM tweet_to_retweeted_uid WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM tweet_to_retweeted_uid WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        else:
            print("没有转发关系！", _id)

    conn1.close()
    conn2.close()

    return new_d


def find_source(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM source_content WHERE id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM source_content WHERE id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v

    conn1.close()
    conn2.close()

    if not new_d:
        print("找不到该source：", _id)

    return new_d


def opinion(_id):
    """
    1 是支持trump
    """

    def get_class_proba(_id):
        """
        >= 0.5 支持希拉里
        """

        re = -1
        conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
        c1 = conn1.cursor()
        c1.execute('''SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}'''.format(_id))
        d = c1.fetchone()
        if d:
            re = d[0]

        else:
            conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
            c2 = conn2.cursor()
            c2.execute('''SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}'''.format(_id))
            d = c2.fetchone()
            if d:
                re = d[0]
            conn2.close()

        conn1.close()
        return re

    v = get_class_proba(_id)
    if v == -1:
        print("LOST tweet: ", _id)
        exit(-1)
        return -1
    return 1 if v < 0.5 else 0


def get_class_proba(_id):
    """
    >= 0.5 支持希拉里
    """

    re = -1
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    conn1.close()
    if d:
        re = d[0]

    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        conn2.close()
        if d:
            re = d[0]
        conn2.close()

    conn1.close()
    return re


def get_hashtag_tweet_user():
    """
    获取训练数据
    """
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    '''
    ['tweet_id', 'hashtag', 'user_id',
     'ht_group', 'ht_group_filt', 'ht_group_labprop', 'ht_group_labprop_filt',
     'ht_signi_initial', 'ht_signi_final', 'ht_signi_final_rnd09_1',
     'ht_signi_final_rnd09_2', 'ht_signi_final_rnd09_3']
    '''

    with open("data/train.csv", "w") as f:
        c1.execute('''SELECT tweet_id, hashtag, user_id, ht_signi_final FROM hashtag_tweet_user WHERE ht_signi_final is not null''')
        for d in tqdm(c1.fetchall()):
            f.write(",".join([str(v) for v in d]) + "\n")

        c2.execute('''SELECT tweet_id, hashtag, user_id, ht_signi_final FROM hashtag_tweet_user WHERE ht_signi_final is not null''')
        for d in tqdm(c2.fetchall()):
            f.write(",".join([str(v) for v in d]) + "\n")

    conn1.close()
    conn2.close()


def create_db():
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    c.execute("CREATE TABLE user (tweet_id INT, user_id INT PRIMARY KEY, info text)")
    conn.commit()
    conn.close()


def get_user(user_id):
    """
    获取用户
    """
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    c.execute("SELECT tweet_id from user WHERE user_id={}".format(user_id))
    d = c.fetchone()
    conn.close()
    # print(d)
    if d:
        return d[0]
    else:
        return 0


def insert_user(tweet_id, user_id, info_json):
    '''
    插入用户
    '''
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    try:
        c.execute("INSERT INTO user VALUES (?, ?, ?)", (tweet_id, user_id, info_json))
    except sqlite3.IntegrityError:
        print('"{}" exists in the list.'.format(user_id))
    conn.commit()
    conn.close()


def insert_many_users(user_list):
    '''
    插入用户
    '''
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    try:
        c.executemany("INSERT INTO user VALUES (?, ?, ?)", user_list)
    except sqlite3.IntegrityError:
        print('"{}" exists in the list.'.format(user_id))
    conn.commit()
    conn.close()


def update_user(tweet_id, user_id, info_json):
    '''
    更新用户
    '''
    conn = sqlite3.connect("tweets.db")
    c = conn.cursor()
    c.execute("UPDATE user SET tweet_id=?, info=? WHERE user_id=?", (tweet_id, info_json, user_id))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # get_hashtag_tweet_user()
    create_db()

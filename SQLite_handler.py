#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:03:07
@author: https://kayzhou.github.io/
"""

import sqlite3
from tqdm import tqdm
import pendulum
import json
from collections import defaultdict


def find_tweet(_id):
    """
    不仅找tweet，还要找retweet
    """
    new_d = {}

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
        for k, v in zip(col_name, d):
            new_d[k] = v
        new_d["from_db"] = "1-tweet"
        conn1.close()
        return new_d

    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            for k, v in zip(col_name, d):
                new_d[k] = v
            new_d["from_db"] = "2-tweet"
            conn1.close()
            conn2.close()
            return new_d

    conn1.close()
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
        new_d["from_db"] = "1-retweeted"

        conn1.close()
        return new_d

    else:
        conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
        c2 = conn2.cursor()
        c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            for k, v in zip(col_name, d):
                new_d[k] = v
            new_d["from_db"] = "2-retweeted"
            conn1.close()
            conn2.close()
            return new_d

    conn1.close()
    conn2.close()
    return new_d


def find_tweets(tweet_ids):

    new_ds = []

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    for _id in tweet_ids:
        new_d = {}
        c1.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
        d = c1.fetchone()

        if d:
            col_name = [t[0] for t in c1.description]
            for k, v in zip(col_name, d):
                new_d[k] = v
        else:
            c2.execute('''SELECT * FROM tweet WHERE tweet_id=={}'''.format(_id))
            d = c2.fetchone()
            if d:
                col_name = [t[0] for t in c2.description]
                for k, v in zip(col_name, d):
                    new_d[k] = v

        if not new_d:
            c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
            d = c1.fetchone()
            if d:
                col_name = [t[0] for t in c1.description]
                for k, v in zip(col_name, d):
                    new_d[k] = v

            else:
                c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
                d = c2.fetchone()
                if d:
                    col_name = [t[0] for t in c2.description]
                    for k, v in zip(col_name, d):
                        new_d[k] = v

        if not new_d:
            new_d = {"tweet_id": _id, "error": "not found"}

        new_ds.append(new_d)

    conn2.close()
    conn1.close()
    return new_ds


def find_tweets_by_users(uids):
    """
    通过用户id找到与之相关的全部tweets
    """
    dict_uids_tweetids = defaultdict(list)
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    for _id in uids:
        c1.execute('''SELECT tweet_id FROM retweeted_status WHERE user_id={}'''.format(_id))
        for d in c1.fetchall():
            dict_uids_tweetids[_id].append(str(d[0]) + "-retweet")
        c1.execute('''SELECT tweet_id FROM tweet_id WHERE user_id={}'''.format(_id))
        for d in c1.fetchall():
            dict_uids_tweetids[_id].append(str(d[0]))

        c2.execute('''SELECT tweet_id FROM retweeted_status WHERE user_id={}'''.format(_id))
        for d in c1.fetchall():
            dict_uids_tweetids[_id].append(str(d[0]) + "-retweet")
        c2.execute('''SELECT tweet_id FROM tweet_id WHERE user_id={}'''.format(_id))
        for d in c2.fetchall():
            dict_uids_tweetids[_id].append(str(d[0]))
    conn2.close()
    conn1.close()
    return dict_uids_tweetids


def find_all_uids():
    '''
    找到所有的uids
    '''
    uids = []

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()
    c1.execute('''SELECT user_id FROM user''')
    for d in c1.fetchall():
        uids.append(str(d[0]))
    conn1.close()

    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c2 = conn2.cursor()
    c2.execute('''SELECT user_id FROM user''')
    for d in c2.fetchall():
        uids.append(str(d[0]))
    conn2.close()

    return uids


def find_users(uids):
    new_ds = []

    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    for _id in uids:
        new_d = {}
        c1.execute('''SELECT * FROM user WHERE user_id={}'''.format(_id))
        d = c1.fetchone()
        if d:
            col_name = [t[0] for t in c1.description]
            for k, v in zip(col_name, d):
                new_d[k] = v
        else:
            c2.execute('''SELECT * FROM user WHERE user_id={}'''.format(_id))
            d = c2.fetchone()
            if d:
                col_name = [t[0] for t in c2.description]
                for k, v in zip(col_name, d):
                    new_d[k] = v

        if not new_d:
            new_d = {"user_id": _id, "error": "not found"}
        new_ds.append(new_d)

    conn2.close()
    conn1.close()
    return new_ds


def find_users_from_large(uids):
    """
    较大的库
    """
    users_info = []

    conn1 = sqlite3.connect("/media/alex/data/election_data/users.db")
    c1 = conn1.cursor()

    for _id in uids:
        c1.execute('''SELECT info FROM user WHERE user_id={}'''.format(_id))
        d = c1.fetchone()
        if d:
            d = json.loads(d[0])
        else:
            d = {"user_id": _id, "error": "not found"}
        users_info.append(d)
    conn1.close()

    return users_info


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


def find_source(tweet_ids):
    """
    仅仅是找到对应的source_content_id，然后再映射name！
    """
    rsts = []
    source_content_id_map = {}
    for _id in tweet_ids:
        # print(_id)
        tweet = find_tweet(_id)
        if tweet:
            from_db = tweet["from_db"][0]
            # print(type(tweet["source_content_id"]))
            tmp = from_db + "-" + str(tweet["source_content_id"])

            if tmp in source_content_id_map:
                source_content = source_content_id_map[tmp]
            else:
                source_content = find_source_name(from_db, tweet["source_content_id"])
                source_content_id_map[tmp] = source_content

            rsts.append({"tweet_id": _id, "source_content": source_content})
        else:
            rsts.append({"tweet_id": _id, "source_content": -1})
    return rsts


def find_source_name(from_db, _id):
    if from_db == "1":
        conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    elif from_db == "2":
        conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    else:
        return "error"

    c = conn.cursor()
    c.execute("SELECT source_content from source_content where id=?", (_id,))
    d = c.fetchone()[0]
    conn.close()

    return d


def get_tweet_opinion(_id):
    """
    1 是支持trump
    """

    def get_class_proba(_id):
        """
        >= 0.5 支持希拉里
        """

        re = -1

        conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/complete_trump_vs_hillary_class_proba_final_htgs_db.sqlite")
        c = conn.cursor()
        c.execute("SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}".format(_id))
        d = c.fetchone()
        if d:
            re = float(d[0])
        else:
            c.execute("SELECT p_pro_hillary_anti_trump FROM retweet_class_proba WHERE tweet_id={}".format(_id))
            d = c.fetchone()
            if d:
                re = float(d[0])

        conn.close()
        if re != -1:
            return re

        conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/complete_trump_vs_hillary_sep-nov_class_proba_final_htgs_june_sep_db.sqlite")
        c = conn.cursor()
        c.execute("SELECT p_pro_hillary_anti_trump FROM class_proba WHERE tweet_id={}".format(_id))
        d = c.fetchone()
        if d:
            re = float(d[0])
        else:
            c.execute("SELECT p_pro_hillary_anti_trump FROM retweet_class_proba WHERE tweet_id={}".format(_id))
            d = c.fetchone()
            if d:
                re = float(d[0])

        conn.close()

        return re

    v = get_class_proba(_id)
    if v == -1:
        return -1
    return 1 if v < 0.5 else 0


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


# ------------------- make a large db!! ----------------------
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


def get_user_id(tweet_id):
    """
    获取用户
    """
    conn = sqlite3.connect("/media/alex/data/election_data/tweetid.db")
    c = conn.cursor()
    c.execute("SELECT user_id from user_tweetid WHERE tweet_id=?", (tweet_id,))
    d = c.fetchone()
    conn.close()
    if d:
        return d[0]
    else:
        return None


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
    except sqlite3.IntegrityError as e:
        print(e)
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
    # create_db()
    print(find_tweet("742417158429233152"))
    opinion("742417158429233152")

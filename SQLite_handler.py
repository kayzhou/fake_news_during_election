#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:03:07
@author: https://kayzhou.github.io/
"""

import sqlite3


def find_tweet(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM tweet WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v

    conn1.close()
    conn2.close()

    if not new_d:
        new_d = find_retweeted(_id)

    # if not new_d:
    #     print("找不到该tweet：", _id)

    return new_d


def find_retweeted(_id):
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c1 = conn1.cursor()
    c2 = conn2.cursor()

    new_d = {}
    c1.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
    d = c1.fetchone()
    if d:
        col_name = [t[0] for t in c1.description]
            # print(d)
        for k, v in zip(col_name, d):
            new_d[k] = v

    else:
        c2.execute('''SELECT * FROM retweeted_status WHERE tweet_id={}'''.format(_id))
        d = c2.fetchone()
        if d:
            col_name = [t[0] for t in c2.description]
            # print(d)
            for k, v in zip(col_name, d):
                new_d[k] = v
        # else:
        #     print("没有找到原创tweet (retweet)", _id)

    conn1.close()
    conn2.close()

    return new_d


def get_hashtag_tweet_user():
    conn1 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c1 = conn1.cursor()

    '''
    ['tweet_id', 'hashtag', 'user_id',
     'ht_group', 'ht_group_filt', 'ht_group_labprop', 'ht_group_labprop_filt',
     'ht_signi_initial', 'ht_signi_final', 'ht_signi_final_rnd09_1',
     'ht_signi_final_rnd09_2', 'ht_signi_final_rnd09_3']
    '''

    c1.execute('''SELECT * FROM hashtag_tweet_user''')
    col_name = [t[0] for t in c1.description]
    print(col_name)
    cnt = 0
    print(c1.fetchone())
    # for d in c1.fetchall():
    #     print(d)
    #     cnt += 1
    #     if cnt > 100:
    #         break

    # conn2 = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    # c2 = conn2.cursor()


if __name__ == "__main__":
    get_hashtag_tweet_user()
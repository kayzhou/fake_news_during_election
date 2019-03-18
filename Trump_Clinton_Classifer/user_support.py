# -*- coding: utf-8 -*-
# Author: Kay Zhou
# Date: 2019-03-18 15:59:33

import sqlite3
import json

set_tweet_id = set()
userdata = {}

def update_user(_d):
    t_id, u_id, pro_hill = _d
    if t_id not in set_tweet_id:
        if u_id not in userdata:
            userdata[u_id] = {
                "num_tweets": 0,
                "num_pro_hill": 0,
                "sum_pro_hill": 0,
            }
        userdata[u_id]["num_tweets"] += 1
        userdata[u_id]["sum_pro_hill"] += float(pro_hill)
        if float(pro_hill) > 0.5:
            userdata[u_id]["num_pro_hill"] += 1
        set_tweet_id.add(t_id)


conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/complete_trump_vs_hillary_class_proba_final_htgs_db.sqlite")
c = conn.cursor()
c.execute('''SELECT * FROM class_proba limit 100;''')
for d in c.fetchall():
    update_user(d)
c.execute('''SELECT * FROM retweet_class_proba limit 100;''')
for d in c.fetchall():
    update_user(d)

conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/complete_trump_vs_hillary_sep-nov_class_proba_final_htgs_june_sep_db.sqlite")
c = conn.cursor()
c.execute('''SELECT * FROM class_proba limit 100;''')
for d in c.fetchall():
    update_user(d)
c.execute('''SELECT * FROM retweet_class_proba limit 100;''')
for d in c.fetchall():
    update_user(d)

with open("../disk/user_support.txt", "w") as f:
    for uid, v in userdata.items():
        f.write("{},{},{},{}\n".format(uid, v["num_pro_hill"], v["num_tweets"], v["sum_pro_hill"]))

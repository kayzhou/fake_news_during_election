#-*- coding: utf-8 -*-

"""
Created on 2019-01-07 10:15:09
@author: https://kayzhou.github.io/
"""

import os
import json
import pandas as pd
from tqdm import tqdm

"""
uids = pd.read_csv("tweets_id_user_not_found.csv")["user_id"]
set_uids = set(uids.tolist())
print(len(set_uids))
"""

in_dirs = ["../hillary OR clinton OR hillaryclinton/",
           "../trump OR donaldtrump OR realdonaldtrump/"]

"""
cnt = 0
with open("tweets_by_users.txt", "w") as f:
    for in_dir in in_dirs:
        for in_name in os.listdir(in_dir):
            in_name = os.path.join(in_dir, in_name)
            if not in_name.endswith(".taj"):
                continue
            cnt += 1
            print(cnt, in_name)
            for line in open(in_name):
                # print(line)
                d = json.loads(line.strip())
                if d["id"] in set_uids:
                    f.write(line)
"""

# uids = set()

# with open("users_info.txt", "w") as f:
#     for in_dir in in_dirs:
#         for in_name in tqdm(os.listdir(in_dir)):
#             in_name = os.path.join(in_dir, in_name)
#             if not in_name.endswith(".taj"):
#                 continue
#             for line in open(in_name):
#                 d = json.loads(line.strip())
#                 user = d["user"]
#                 if user["id"] not in uids:
#                     f.write(json.dumps(user) + "\n")
#                     uids.add(user["id"])

# target_uids = {line.strip().split(",")[0] for line in open("CI_clique.txt")}

# uid_name = {}
# for line in tqdm(open("users_info.txt", "w")):
#     user = json.loads(line.strip())
#     uid = user["id"]
#     if uid in target_uids:
#         uid_name[uid] = user["screen_name"]

# json.dump(uid_name, open("user_name.txt", "w"), indent=2)


target_uids = {line.strip().split(",")[0] for line in open("CI_clique.txt")}
print("N of targeted uids", target_uids)

uid_name = {}

for in_dir in in_dirs:
    for in_name in tqdm(os.listdir(in_dir)):
        in_name = os.path.join(in_dir, in_name)
        if not in_name.endswith(".taj"):
            continue
        for line in open(in_name):
            d = json.loads(line.strip())
            user = d["user"]
            if user["id_str"] in target_uids:
                uid_name[uid] = user["screen_name"]

json.dump(uid_name, open("user_name.txt", "w"), indent=2)

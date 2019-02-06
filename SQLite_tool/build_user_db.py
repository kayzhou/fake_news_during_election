import os

from tqdm import tqdm

import ujson as json
from SQLite_handler import *

in_dir = "/mnt/kay_data/alex_avocado/data"
user_ids_set = set()
user_list = []

for in_name in os.listdir(in_dir):
    in_name = os.path.join(in_dir, in_name)
    if not in_name.endswith(".taj"):
        continue

    print(in_name)
    for line in tqdm(open(in_name)):
        d = json.loads(line.strip())
        try:
            tweet_id = int(d["id_str"])
        except KeyError as e:
            # print(e)
            continue

        user = d["user"]
        user_id = user["id"]
        if user_id not in user_ids_set:
            info_json = json.dumps(user, ensure_ascii=False)
            user_list.append((tweet_id, user_id, info_json))
            user_ids_set.add(user_id)

        if len(user_list) == 1000:
            insert_many_users(user_list)
            user_list = []

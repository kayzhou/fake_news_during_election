#-*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/

12th week in NY
"""

from my_weapon import *
from SQLite_handler import get_user_id


class analyze_IRA_in_network:
    def __init__(self):
        self.author = "kay"
        self.user_id_map = {}

    def find_user_id_map(self):
        data = pd.read_csv("data/ira_tweets_csv_hashed.csv", usecols=["tweetid", "userid"], dtype=str)
        for _, row in tqdm(data.iterrows()):
            user_id = row["userid"]
            if len(user_id) == 64:
                if user_id in self.user_id_map:
                    continue
                real_user_id = get_user_id(row["tweetid"])
                if real_user_id:
                    self.user_id_map[user_id] = real_user_id

        # save
        json.dump(self.user_id_map, open("data/IRA_map.json", "w"), indent=2)


if __name__ == "__main__":
    Lebron = analyze_IRA_in_network()
    Lebron.find_user_id_map()

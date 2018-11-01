import sqlite3
import json
import Queue


def find_fake_tweets(): host_label = json.load(open('data/host_label.json'))
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/urls_db.sqlite")
    c = conn.cursor()
    c.execute('''SELECT * FROM urls;''')
    col_names = [t[0] for t in c.description]
    data = c.fetchall()


    with open("fake.txt", "w") as f:
        for i, d in enumerate(data):
            if i % 10000 == 0:
                print(i, d)
            if d[8] in host_label and host_label[d[8]] == "fake":
                json_d = {k: v for k, v in zip(col_names, d)}
                json_d = json.dumps(json_d, ensure_ascii=False)
                f.write(json_d + '\n')


def find_retweets(tweets_ids):

    q = Queue.Queue()
    for _id in tweets_ids:
        q.put(_id)
    dealed = set([])

    # retweet_network = {}
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    with open("data/network_fake.txt", "w") as f:
        while not q.empty():
            _id = q.get()
            c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
            data = c.fetchall()
            dealed.add(_id)
            for line in data:
                tid = line[0]
                f.write("{}\t{}\n".format(_id, tid))
                if tid not in dealed:
                    q.put(tid)


if __name__ == "__main__":
    find_retweets





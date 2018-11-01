import sqlite3
import json
import queue


def find_fake_tweets():
    host_label = json.load(open('data/host_label.json'))
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


def find_retweets(tweets_ids, out_name):
    q = queue.Queue()
    for _id in tweets_ids:
        q.put(_id)
    dealed = set([])
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    cnt = 0
    with open(out_name, "w") as f:
        while not q.empty():
            _id = q.get()
            cnt += 1
            if cnt % 1000 == 0:
                print(cnt, q.qsize())
            c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
            data = c.fetchall()
            dealed.add(_id)
            for line in data:
                tid = line[0]
                f.write("{}\t{}\n".format(_id, tid))
                if tid not in dealed:
                    q.put(tid)


if __name__ == "__main__":
    # tweets_ids = set([json.loads(line.strip())["tweet_id"] for line in open("data/fake.txt")])
    tweets_ids = set([])
    for line in open("data/network_fake.txt"):
        n1, n2 = line.strip().split("\t")
        tweets_id.add(n1); tweets_ids.add(n2)

    find_retweets(tweets_ids, "data/retweet_network_2.txt")

    # union





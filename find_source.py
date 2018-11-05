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

    with open("fake.txt", "w") as f:
        for i, d in enumerate(data):
            if i % 10000 == 0:
                print(i, d)
            if d[8] in host_label and host_label[d[8]] == "fake":
                json_d = {k: v for k, v in zip(col_names, d)}
                json_d = json.dumps(json_d, ensure_ascii=False)
                f.write(json_d + '\n')


def find_retweets(tweets_ids, out_name):
    q = set()
    new_nodes = set()
    for _id in tweets_ids:
        q.add(_id)

    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    cnt = 0
    edge_cnt = 0
    with open(out_name, "w") as f:
        while q:
            _id = q.pop()
            cnt += 1
            if cnt % 20000 == 0:
                # print(_id, _id in dealed)
                print(cnt, "；边的数量：", edge_cnt, "；等待处理队列：", len(q))

            c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))

            for line in c.fetchall():
                tid = line[0]
                edge_cnt += 1
                f.write("{}\t{}\n".format(_id, tid))
                # 新添加的之前肯定没有添加过，是唯一的
                q.add(tid)

def load_all_nodes_v1():
    tweets_ids = set([])
    for line in open("data/retweet_network_1.txt"):
        n1, n2 = line.strip().split("\t")
        tweets_ids.add(int(n1))
        tweets_ids.add(int(n2))
    print(len(tweets_ids))

    t2 = set([int(json.loads(line.strip())["tweet_id"]) for line in open("data/fake.txt")])
    tweets_ids = tweets_ids | t2
    print(len(tweets_ids))
    return tweets_ids


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


def union_retweet_line():
    retween_lines = set()
    for line in open("data/retweet_network_1.txt"):
        retween_lines.add(line)
    for line in open("data/retweet_network_2.txt"):
        retween_lines.add(line)
    with open("data/edge-tid-fake-news.txt", "w") as f:
        for line in retween_lines:
            f.write(line)


def find_all_links(tweets_ids):
    have_dealed = set()
    q = queue.Queue()
    for _id in tweets_ids:
        q.put(_id)
    retweet_link = {}

    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    cnt = 0

    while not q.empty():
        _id = q.get()
        cnt += 1
        if cnt % 50000 == 0:
            # print(_id, _id in dealed)
            print(cnt, len(have_dealed), "；边的数量：", len(retweet_link), "；等待处理队列：", q.qsize())

        have_next = False
        have_last = False
        c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for next_d in c.fetchall():
            have_next = True
            next_id = str(next_d[0])
            retweet_link[next_id] = str(_id)
            if next_id not in have_dealed:
                q.put(next_id)

        c.execute('''SELECT retweet_id FROM tweet_to_retweeted_uid WHERE tweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for last_d in c.fetchall():
            have_last = True
            last_id = str(last_d[0])
            retweet_link[str(_id)] = last_id
            if last_id not in have_dealed:
                q.put(last_id)

        if have_next and have_last:
            print("我找到了！")

    conn.close()

    # 下一个！
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    q = queue.Queue()
    for _id in tweets_ids:
        q.put(_id)
    for k, v in retweet_link.items():
        q.put(v)

    while not q.empty():
        _id = q.get()
        cnt += 1
        if cnt % 50000 == 0:
            # print(_id, _id in dealed)
            print(cnt, len(have_dealed), "；边的数量：", len(retweet_link), "；等待处理队列：", q.qsize())

        have_next = False
        have_last = False
        c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for next_d in c.fetchall():
            have_next = True
            next_id = str(next_d[0])
            retweet_link[next_id] = str(_id)
            if next_id not in have_dealed:
                q.put(next_id)

        c.execute('''SELECT retweet_id FROM tweet_to_retweeted_uid WHERE tweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for last_d in c.fetchall():
            have_last = True
            last_id = str(last_d[0])
            retweet_link[str(_id)] = last_id
            if last_id not in have_dealed:
                q.put(last_id)

        if have_next and have_last:
            print("我找到了！")
    conn.close()

    # 下一个！
    conn = sqlite3.connect("/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    q = queue.Queue()
    for _id in tweets_ids:
        q.put(_id)
    for k, v in retweet_link.items():
        q.put(v)

    while not q.empty():
        _id = q.get()
        cnt += 1
        if cnt % 50000 == 0:
            # print(_id, _id in dealed)
            print(cnt, len(have_dealed), "；边的数量：", len(retweet_link), "；等待处理队列：", q.qsize())

        have_next = False
        have_last = False
        c.execute('''SELECT tweet_id FROM tweet_to_retweeted_uid WHERE retweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for next_d in c.fetchall():
            have_next = True
            next_id = str(next_d[0])
            retweet_link[next_id] = str(_id)
            if next_id not in have_dealed:
                q.put(next_id)

        c.execute('''SELECT retweet_id FROM tweet_to_retweeted_uid WHERE tweet_id={};'''.format(_id))
        have_dealed.add(str(_id))
        for last_d in c.fetchall():
            have_last = True
            last_id = str(last_d[0])
            retweet_link[str(_id)] = last_id
            if last_id not in have_dealed:
                q.put(last_id)

        if have_next and have_last:
            print("我找到了！")
    conn.close()

    # json.dump(retweet_link, open("data/retweet_network_fake.json", "w"), ensure_ascii=False, indent=2)

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
    # t_ids = set([int(json.loads(line.strip())["tweet_id"]) for line in open("data/fake.txt")])
    # print(len(tweets_ids))
    # tweets_ids = load_all_nodes_v1()
    # find_retweets(tweets_ids, "data/retweet_network_2.txt")

    # union
    # tweets_ids = load_all_nodes()
    # with open("data/node-tid-fake-news.txt", "w") as f:
    #     for tid in tweets_ids:
    #         f.write(str(tid) + "\n")

    # union_retweet_line()

    # ----------------~~~-------------------
    # find_all_links(t_ids)

    # tids = load_fake_news_source()
    # get_tweets(tids)

    # 获取所有fake news相关的信息
    tids = load_fake_news()
    print(len(tids))
    # get_tweets(tids)
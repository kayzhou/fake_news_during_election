# -*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/

12th week in NY
"""

"""
------------------------------ TABLE tweet_to_quoted_uid ------------------------------
count： (2712599,)
tweet_id		771131463383285761
quoted_uid		457984599
author_uid		3192506298

------------------------------ TABLE tweet_to_replied_uid ------------------------------
count： (10064719,)
tweet_id		771131465287528449
replied_uid		1339835893
author_uid		754080446078676993

------------------------------ TABLE tweet_to_retweeted_uid ------------------------------
count： (71935828,)
tweet_id		771131463924199424
retweeted_uid		1339835893
author_uid		55956218
retweet_id		771037992811163648

------------------------------ TABLE tweet_to_mentioned_uid ------------------------------
count： (40694821,)
tweet_id		771131463345565696
mentioned_uid		73657657
author_uid		2429107224

"""


import gc
import os
import sqlite3
from collections import defaultdict

from pathlib import Path
# import graph_tool.all as gt

import pendulum
from my_weapon import *
import ujson as json
from fake_identify import Are_you_IRA
from SQLite_handler import find_all_uids, find_tweet, get_user_id


def get_whole_network():
    out_file = open("disk/all-links.json", "w")

    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid=str(d[0]),
                    r_uid=str(d[1]),
                    uid=str(d[2]),
                    r_tid=str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid=str(d[0]),
                    r_uid=str(d[1]),
                    uid=str(d[2]),
                    r_tid=str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()


def get_whole_network():
    out_file = open("disk/all-links.json", "w")

    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid=str(d[0]),
                    r_uid=str(d[1]),
                    uid=str(d[2]),
                    r_tid=str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        link = dict(tid=str(d[0]),
                    r_uid=str(d[1]),
                    uid=str(d[2]),
                    r_tid=str(d[3])
                    )
        out_file.write(json.dumps(link) + "\n")
    conn.close()


def get_ret_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_retweeted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_quote_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_quoted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_quoted_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_rep_network(out_name):

    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_replied_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_replied_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()


def get_mention_network(out_name):
    out_file = open(out_name, "w")
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_mentioned_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(i) for i in d]) + "\n")
    conn.close()

    # 下一个！
    conn = sqlite3.connect(
        "/home/alex/network_workdir/elections/databases_ssd/complete_trump_vs_hillary_sep-nov_db.sqlite")
    c = conn.cursor()

    c.execute('''SELECT * FROM tweet_to_mentioned_uid''')

    for d in c.fetchall():
        out_file.write(" ".join([str(_d) for _d in d]) + "\n")
    conn.close()


def get_all_network_by_tweet(tweet_ids, out_file_pre):

    # IRA
    set_tweet_ids = set(tweet_ids)

    # retweet
    net_1 = []
    for line in tqdm(open("disk/all-ret-links.txt")):
        w = line.strip().split()
        if w[0] in set_tweet_ids:
            net_1.append((w[1], w[2]))
        if w[3] in set_tweet_ids:
            net_1.append((w[2], w[1]))

    # quote
    net_2 = []
    for line in tqdm(open("disk/all-quo-links.txt")):
        w = line.strip().split()
        if w[0] in set_tweet_ids:
            net_2.append((w[1], w[2]))

    # reply
    net_3 = []
    for line in tqdm(open("disk/all-rep-links.txt")):
        w = line.strip().split()
        if w[0] in set_tweet_ids:
            net_3.append((w[1], w[2]))

    # mention
    net_4 = []
    for line in tqdm(open("disk/all-men-links.txt")):
        w = line.strip().split()
        if w[0] in set_tweet_ids:
            net_4.append((w[2], w[1]))

    # json.dump(net_1, open(out_file_pre + "-ret.txt"))
    # json.dump(net_2, open(out_file_pre + "-quo.txt"))
    # json.dump(net_3, open(out_file_pre + "-rep.txt"))
    # json.dump(net_4, open(out_file_pre + "-men.txt"))

    n_all = nx.DiGraph()
    n_all.add_edges_from(net_1)
    n_all.add_edges_from(net_2)
    n_all.add_edges_from(net_3)
    n_all.add_edges_from(net_4)
    nx.write_gpickle(n_all, out_file_pre + '-all.gpickle')

    n1 = nx.DiGraph()
    n1.add_edges_from(net_1)
    nx.write_gpickle(n1, out_file_pre + '-ret.gpickle')

    n2 = nx.DiGraph()
    n2.add_edges_from(net_2)
    nx.write_gpickle(n2, out_file_pre + '-quo.gpickle')

    n3 = nx.DiGraph()
    n3.add_edges_from(net_3)
    nx.write_gpickle(n3, out_file_pre + '-rep.gpickle')

    n4 = nx.DiGraph()
    n4.add_edges_from(net_4)
    nx.write_gpickle(n4, out_file_pre + '-men.gpickle')


def get_all_network_by_user(user_ids, out_file_pre):
    # IRAs 针对用户id而构造网络

    set_user_ids = set(user_ids)
    # retweet
    net_1 = []
    for line in tqdm(open("disk/all-ret-links.txt")):
        w = line.strip().split()
        if w[1] in set_user_ids or w[2] in set_user_ids:
            net_1.append((w[1], w[2]))

    # quote
    net_2 = []
    for line in tqdm(open("disk/all-quo-links.txt")):
        w = line.strip().split()
        if w[1] in set_user_ids or w[2] in set_user_ids:
            net_2.append((w[1], w[2]))

    # reply
    net_3 = []
    for line in tqdm(open("disk/all-rep-links.txt")):
        w = line.strip().split()
        if w[1] in set_user_ids or w[2] in set_user_ids:
            net_3.append((w[1], w[2]))

    # mention
    net_4 = []
    for line in tqdm(open("disk/all-men-links.txt")):
        w = line.strip().split()
        if w[1] in set_user_ids or w[2] in set_user_ids:
            net_4.append((w[2], w[1]))

    # json.dump(net_1, open(out_file_pre + "-ret.txt"))
    # json.dump(net_2, open(out_file_pre + "-quo.txt"))
    # json.dump(net_3, open(out_file_pre + "-rep.txt"))
    # json.dump(net_4, open(out_file_pre + "-men.txt"))

    n_all = nx.DiGraph()
    n_all.add_edges_from(net_1)
    n_all.add_edges_from(net_2)
    n_all.add_edges_from(net_3)
    n_all.add_edges_from(net_4)
    nx.write_gpickle(n_all, out_file_pre + '-all.gpickle')

    n1 = nx.DiGraph()
    n1.add_edges_from(net_1)
    nx.write_gpickle(n1, out_file_pre + '-ret.gpickle')

    n2 = nx.DiGraph()
    n2.add_edges_from(net_2)
    nx.write_gpickle(n2, out_file_pre + '-quo.gpickle')

    n3 = nx.DiGraph()
    n3.add_edges_from(net_3)
    nx.write_gpickle(n3, out_file_pre + '-rep.gpickle')

    n4 = nx.DiGraph()
    n4.add_edges_from(net_4)
    nx.write_gpickle(n4, out_file_pre + '-men.gpickle')


def make_all_network(out_file_pre):
    # for all users
    """
    # retweet
    net_1 = []
    for line in tqdm(open("disk/all-ret-links.txt")):
        w = line.strip().split()
        net_1.append((int(w[1]), int(w[2])))

    """
    # quote
    net_2 = []
    for line in tqdm(open("disk/all-quo-links.txt")):
        w = line.strip().split()
        net_2.append((int(w[1]), int(w[2])))

    # reply
    net_3 = []
    for line in tqdm(open("disk/all-rep-links.txt")):
        w = line.strip().split()
        net_3.append((int(w[1]), int(w[2])))

    # mention
    net_4 = []
    for line in tqdm(open("disk/all-men-links.txt")):
        w = line.strip().split()
        net_4.append((int(w[2]), int(w[1])))

    # json.dump(net_1, open(out_file_pre + "-ret.txt"))
    # json.dump(net_2, open(out_file_pre + "-quo.txt"))
    # json.dump(net_3, open(out_file_pre + "-rep.txt"))
    # json.dump(net_4, open(out_file_pre + "-men.txt"))

    """
    n1 = nx.DiGraph()
    n1.add_edges_from(net_1)
    nx.write_gpickle(n1, out_file_pre + '-ret.gpickle')

    """
    n2 = nx.DiGraph()
    n2.add_edges_from(net_2)
    nx.write_gpickle(n2, out_file_pre + '-quo.gpickle')

    n3 = nx.DiGraph()
    n3.add_edges_from(net_3)
    nx.write_gpickle(n3, out_file_pre + '-rep.gpickle')

    n4 = nx.DiGraph()
    n4.add_edges_from(net_4)
    nx.write_gpickle(n4, out_file_pre + '-men.gpickle')


def get_prop_type(value, key=None):
    """
    Performs typing and value conversion for the graph_tool PropertyMap class.
    If a key is provided, it also ensures the key is in a format that can be
    used with the PropertyMap. Returns a tuple, (type name, value, key)
    """
    if isinstance(key, unicode):
        # Encode the key as ASCII
        key = key.encode('ascii', errors='replace')

    # Deal with the value
    if isinstance(value, bool):
        tname = 'bool'

    elif isinstance(value, int):
        tname = 'float'
        value = float(value)

    elif isinstance(value, float):
        tname = 'float'

    elif isinstance(value, unicode):
        tname = 'string'
        value = value.encode('ascii', errors='replace')

    elif isinstance(value, dict):
        tname = 'object'

    else:
        tname = 'string'
        value = str(value)

    return tname, value, key


def nx2gt(nxG):
    """
    Converts a networkx graph to a graph-tool graph.
    """
    # Phase 0: Create a directed or undirected graph-tool Graph
    print("converting ...")
    gtG = gt.Graph(directed=nxG.is_directed())

    # Add the Graph properties as "internal properties"
    for key, value in nxG.graph.items():
        # Convert the value and key into a type for graph-tool
        tname, value, key = get_prop_type(value, key)

        prop = gtG.new_graph_property(tname)  # Create the PropertyMap
        gtG.graph_properties[key] = prop     # Set the PropertyMap
        gtG.graph_properties[key] = value    # Set the actual value

    # Phase 1: Add the vertex and edge property maps
    # Go through all nodes and edges and add seen properties
    # Add the node properties first
    nprops = set()  # cache keys to only add properties once
    for node, data in list(nxG.nodes(data=True)):

        # Go through all the properties if not seen and add them.
        for key, val in data.items():
            if key in nprops:
                continue  # Skip properties already added

            # Convert the value and key into a type for graph-tool
            tname, _, key = get_prop_type(val, key)

            prop = gtG.new_vertex_property(tname)  # Create the PropertyMap
            gtG.vertex_properties[key] = prop     # Set the PropertyMap

            # Add the key to the already seen properties
            nprops.add(key)

    # Also add the node id: in NetworkX a node can be any hashable type, but
    # in graph-tool node are defined as indices. So we capture any strings
    # in a special PropertyMap called 'id' -- modify as needed!
    gtG.vertex_properties['id'] = gtG.new_vertex_property('string')

    # Add the edge properties second
    eprops = set()  # cache keys to only add properties once
    for src, dst, data in list(nxG.edges(data=True)):

        # Go through all the edge properties if not seen and add them.
        for key, val in data.items():
            if key in eprops:
                continue  # Skip properties already added

            # Convert the value and key into a type for graph-tool
            tname, _, key = get_prop_type(val, key)

            prop = gtG.new_edge_property(tname)  # Create the PropertyMap
            gtG.edge_properties[key] = prop     # Set the PropertyMap

            # Add the key to the already seen properties
            eprops.add(key)

    # Phase 2: Actually add all the nodes and vertices with their properties
    # Add the nodes
    vertices = {}  # vertex mapping for tracking edges later
    for node, data in list(nxG.nodes(data=True)):

        # Create the vertex and annotate for our edges later
        v = gtG.add_vertex()
        vertices[node] = v

        # Set the vertex properties, not forgetting the id property
        data['id'] = str(node)
        for key, value in data.items():
            gtG.vp[key][v] = value  # vp is short for vertex_properties

    # Add the edges
    for src, dst, data in list(nxG.edges(data=True)):

        # Look up the vertex structs from our vertices mapping and add edge.
        e = gtG.add_edge(vertices[src], vertices[dst])

        # Add the edge properties
        for key, value in data.items():
            gtG.ep[key][e] = value  # ep is short for edge_properties

    # Done, finally!
    return gtG


def change_network(out_file_pre):
    print("chaning networks ... ")
    n = nx.read_gpickle(out_file_pre + '-all.gpickle')
    nx2gt(n).save(out_file_pre + '-all.gt')

    n = nx.read_gpickle(out_file_pre + '-ret.gpickle')
    nx2gt(n).save(out_file_pre + '-ret.gt')

    n = nx.read_gpickle(out_file_pre + '-quo.gpickle')
    nx2gt(n).save(out_file_pre + '-quo.gt')

    n = nx.read_gpickle(out_file_pre + '-rep.gpickle')
    nx2gt(n).save(out_file_pre + '-rep.gt')

    n = nx.read_gpickle(out_file_pre + '-men.gpickle')
    nx2gt(n).save(out_file_pre + '-men.gt')


def save_network_gt():

    set_net = set()
    node_map = {}
    for line in tqdm(open("disk/all-ret-links.txt")):
        w = line.strip().split()
        set_net.add("{}-{}".format(w[1], w[2]))
        if w[1] not in node_map:
            node_map[w[1]] = len(node_map)
        if w[2] not in node_map:
            node_map[w[2]] = len(node_map)
    # quote
    for line in tqdm(open("disk/all-quo-links.txt")):
        w = line.strip().split()
        set_net.add("{}-{}".format(w[1], w[2]))
        if w[1] not in node_map:
            node_map[w[1]] = len(node_map)
        if w[2] not in node_map:
            node_map[w[2]] = len(node_map)
    # reply
    for line in tqdm(open("disk/all-rep-links.txt")):
        w = line.strip().split()
        set_net.add("{}-{}".format(w[1], w[2]))
        if w[1] not in node_map:
            node_map[w[1]] = len(node_map)
        if w[2] not in node_map:
            node_map[w[2]] = len(node_map)

    # mention
    for line in tqdm(open("disk/all-men-links.txt")):
        w = line.strip().split()
        set_net.add("{}-{}".format(w[2], w[1]))
        if w[1] not in node_map:
            node_map[w[1]] = len(node_map)
        if w[2] not in node_map:
            node_map[w[2]] = len(node_map)

    g = gt.Graph()

    print("add nodes from ...")
    vlist = g.add_vertex(len(node_map))

    vprop = g.new_vertex_property("int")
    g.vp.name = vprop

    print("add edge from ...")
    for n in tqdm(set_net):
        n1, n2 = n.split("-")
        u1 = node_map[n1]
        u2 = node_map[n2]

        node1 = g.vertex(u1)
        node2 = g.vertex(u2)

        g.vp.name[node1] = int(u1)
        g.vp.name[node2] = int(u2)

        g.add_edge(node1, node2)

    print("saving the graph ...")
    g.save("disk/whole-all.gt")
    print("finished!")


def get_network_with_ira():
    ira_tweets = pd.read_csv("data/ira-tweets-ele.csv", dtype=str)
    print("loaded ", len(ira_tweets))

    Putin = Are_you_IRA()

    """
    Index(['tweetid', 'userid', 'user_display_name', 'user_screen_name',
       'user_reported_location', 'user_profile_description',
       'user_profile_url', 'follower_count', 'following_count',
       'account_creation_date', 'account_language', 'tweet_language',
       'tweet_text', 'tweet_time', 'tweet_client_name', 'in_reply_to_tweetid',
       'in_reply_to_userid', 'quoted_tweet_tweetid', 'is_retweet',
       'retweet_userid', 'retweet_tweetid', 'latitude', 'longitude',
       'quote_count', 'reply_count', 'like_count', 'retweet_count', 'hashtags',
       'urls', 'user_mentions', 'poll_choices'],
      dtype='object')
    """
    men_file = open("disk/ira-men.txt", "w")
    ret_file = open("disk/ira-ret.txt", "w")
    rep_file = open("disk/ira-rep.txt", "w")
    # quo_file = open("disk/ira-quo.txt", "w")

    rep_ira_tweets = ira_tweets[ira_tweets.in_reply_to_tweetid.notnull()]
    quo_ira_tweets = ira_tweets[ira_tweets.quoted_tweet_tweetid.notnull()]
    ret_ira_tweets = ira_tweets[ira_tweets.retweet_tweetid.notnull()]
    men_ira_tweets = ira_tweets[ira_tweets.user_mentions.notnull()]

    rep_file.write("tweet_id,user_id,o_tweet_id,o_user_id\n")
    for i, row in rep_ira_tweets.iterrows():
        rep_file.write(",".join([
            row["tweetid"],
            Putin.uncover(row["userid"]),
            row["in_reply_to_tweetid"],
            Putin.uncover(row["in_reply_to_userid"])
        ]) + "\n")

    # cnt = 0
    # quo_file.write("tweet_id,user_id,o_tweet_id,o_user_id\n")
    # for i, row in quo_ira_tweets.iterrows():
    #     try:
    #         quo_file.write(",".join([
    #             row["tweetid"],
    #             Putin.uncover(row["userid"]),
    #             row["quoted_tweet_tweetid"],
    #             Putin.uncover(row["retweet_userid"])
    #         ]) + "\n")
    #     except:
    #         print(row["retweet_userid"])
    #         print(row["in_reply_to_userid"])
    #         cnt += 1
    # print(len(quo_ira_tweets), cnt)

    ret_file.write("tweet_id,user_id,o_tweet_id,o_user_id\n")
    for i, row in ret_ira_tweets.iterrows():
        ret_file.write(",".join([
            row["tweetid"],
            Putin.uncover(row["userid"]),
            row["retweet_tweetid"],
            Putin.uncover(row["retweet_userid"])
        ]) + "\n")

    men_file.write("tweet_id,user_id,to_tweet_id,to_user_id\n")
    for i, row in men_ira_tweets.iterrows():
        mentions = row["user_mentions"]
        us = mentions[1:-1].split(", ")
        for u in us:
            men_file.write(",".join([
                row["tweetid"],
                Putin.uncover(row["userid"]),
                Putin.uncover(u)
            ]) + "\n")


def get_ira_network_with_big_networks():
    Putin = Are_you_IRA()
    def search_IRA(in_name, out_name):
        with open(out_name, "w") as f:
            for line in tqdm(open(in_name)):
                w = line.strip().split()
                if Putin.fuck(w[1]) or Putin.fuck(w[2]):
                    f.write(line)
    search_IRA("disk/all-men-links.txt", "disk/ira-men-links.txt")
    search_IRA("disk/all-ret-links.txt", "disk/ira-ret-links.txt")
    search_IRA("disk/all-rep-links.txt", "disk/ira-rep-links.txt")


def merge_two_groups_link_to_graph():
    from collections import Counter
    men_tweets = {}
    rep_tweets = {}
    ret_tweets = {}

    G = nx.DiGraph()
    men_rec = set()
    men_graph = Counter()
    for line in open("disk/ira-men-links.txt"):
        w = line.strip().split()
        t_id = w[0]
        n1 = w[2]
        n2 = w[1]
        if t_id + "-" + n2 in men_rec:
            continue
        men_graph[(n1, n2)] += 1

    for i, line in enumerate(open("disk/ira-men.txt")):
        if i == 0:
            continue
        w = line.strip().split(",")
        t_id = w[0]
        n1 = w[1]
        n2 = w[2]
        if t_id + "-" + n2 in men_rec:
            continue
        men_graph[(n1, n2)] += 1
    
    for e in men_graph:
        w = men_graph[e]
        G.add_edge(*e, weight=w)

    nx.write_gpickle(G, "disk/ira-men.gp")


    G = nx.DiGraph()
    rep_rec = set()
    rep_graph = Counter()
    for line in open("disk/ira-rep-links.txt"):
        w = line.strip().split()
        t_id = w[0]
        n1 = w[1]
        n2 = w[2]
        if t_id in rep_rec:
            continue
        rep_graph[(n1, n2)] += 1

    for i, line in enumerate(open("disk/ira-rep.txt")):
        if i == 0:
            continue
        w = line.strip().split(",")
        t_id = w[0]
        n1 = w[3]
        n2 = w[1]
        if t_id in rep_rec:
            continue
        rep_graph[(n1, n2)] += 1
    
    for e in rep_graph:
        w = rep_graph[e]
        G.add_edge(*e, weight=w)
        
    nx.write_gpickle(G, "disk/ira-rep.gp")


    G = nx.DiGraph()
    ret_rec = set()
    ret_graph = Counter()
    for line in open("disk/ira-ret-links.txt"):
        w = line.strip().split()
        t_id = w[0]
        n1 = w[1]
        n2 = w[2]
        if t_id in ret_rec:
            continue
        ret_graph[(n1, n2)] += 1

    for i, line in enumerate(open("disk/ira-ret.txt")):
        if i == 0:
            continue
        w = line.strip().split(",")
        t_id = w[0]
        n1 = w[3]
        n2 = w[1]
        if t_id in ret_rec:
            continue
        ret_graph[(n1, n2)] += 1
    
    for e in ret_graph:
        w = ret_graph[e]
        G.add_edge(*e, weight=w)
        
    nx.write_gpickle(G, "disk/ira-ret.gp")


# abandon
def build_networks_within_ira():
    ira_tweets = set(pd.read_csv("data/ira-tweets-ele.csv").tweetid)
    print("loaded ", len(ira_tweets))
    
    ira_file = open("disk/ira-mapping-plus.txt", "w")
    ret_file = open("disk/ira-retweet-network.txt", "w")
    rep_file = open("disk/ira-reply-network.txt", "w")

    in_files = [f for f in Path("/media/alex/datums/elections_tweets/archives/trump OR donaldtrump OR realdonaldtrump").glob("*.taj")]
    in_files.extend([f for f in Path("/media/alex/datums/elections_tweets/archives/hillary OR clinton OR hillaryclinton").glob("*.taj")])
    # in_files = ["data/tweets-c3cbeb35-27a6-4dc9-9cc0-9c47e1ad5cf2.taj"]

    start = pendulum.parse("2016-06-01").int_timestamp
    end = pendulum.parse("2016-11-09").int_timestamp

    for in_name in tqdm(in_files):
        for line in tqdm(open(in_name)):
            d = json.loads(line)
            dt = pendulum.from_format(d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY').int_timestamp
            if dt > end or dt < start:
                continue
            tweet_id = int(d["id"])
            user_id = int(d["user"]["id"])

            if tweet_id in ira_tweets:
                ira_file.write("{}\t{}\n".format(
                    tweet_id, user_id))

            if d["retweeted"]:
                if d["retweeted_status"]["id"] in ira_tweets:
                    ret_file.write("{}\t{}\t{}\t{}\n".format(
                        tweet_id, user_id, d["retweeted_status"]["id"], d["retweeted_status"]["user"]["id"]))

            if d["in_reply_to_status_id"]:
                if d["in_reply_to_status_id"] in ira_tweets:
                    rep_file.write("{}\t{}\t{}\t{}\n".format(
                        tweet_id, user_id, d["in_reply_to_status_id"], d["in_reply_to_user_id"]))

    ira_file.close()
    ret_file.close()
    rep_file.close()



if __name__ == "__main__":
    # Lebron = analyze_IRA_in_network()
    # Lebron.run()

    # print("rep")
    # get_ret_network("disk/all-ret-links.txt")
    # print("ret")
    # get_rep_network("disk/all-rep-links.txt")
    # print("quo")
    # get_quote_network("disk/all-quo-links.txt")
    # print("men")
    # get_mention_network("disk/all-men-links.txt")

    # media network~
    # map_labels = {
    #     "0": "fake",
    #     "1": "extreme bias (right)",
    #     "2": "right",
    #     "3": "right leaning",
    #     "4": "center",
    #     "5": "left leaning",
    #     "6": "left",
    #     "7": "extreme bias (left)"
    # }

    # for _type, f_label in map_labels.items():
    #     print(_type, "...")
    #     nt = nx.read_gpickle("disk/network_{}.gpickle".format(f_label))
    #     print("type(n) =", type(nt))
    #     _gt = nx2gt(nt)
    #     _gt.save("disk/network_{}.gt".format(f_label))

    # build IRA network
    """
    putin = Are_you_IRA()
    ira_user_set = set()
    for uid in putin.IRA_user_set:
        try:
            if len(putin._map[uid]) != 64:
                ira_user_set.add(str(putin._map[uid]))
        except:
            if len(uid) != 64:
                ira_user_set.add(uid)

    get_all_network_by_user(ira_user_set, "data/network/ira")
    change_network("data/network/ira")
    """

    # make_all_network("disk/whole")
    # change_network("disk/whole")
    # save_network_gt()

    # build_networks_within_ira()
    # get_network_with_ira()
    # get_ira_network_with_big_networks()
    merge_two_groups_link_to_graph()
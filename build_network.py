# -*- coding: utf-8 -*-

"""
Created on 2018-11-16 09:01:28
@author: https://kayzhou.github.io/

12th week in NY
"""

import gc
import os
import sqlite3
from collections import defaultdict

import graph_tool.all as gt

from my_weapon import *
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


def get_all_network(tweet_ids, out_file_pre):

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


def get_all_network(user_ids, out_file_pre):
    # IRAs

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
    """
    n = nx.read_gpickle(out_file_pre + '-ret.gpickle')
    nx2gt(n).save(out_file_pre + '-ret.gt')

    n = nx.read_gpickle(out_file_pre + '-quo.gpickle')
    nx2gt(n).save(out_file_pre + '-quo.gt')

    n = nx.read_gpickle(out_file_pre + '-rep.gpickle')
    nx2gt(n).save(out_file_pre + '-rep.gt')
    """

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
    map_labels = {
        "0": "fake",
        "1": "extreme bias (right)",
        "2": "right",
        "3": "right leaning",
        "4": "center",
        "5": "left leaning",
        "6": "left",
        "7": "extreme bias (left)"
    }

    for _type, f_label in map_labels.items():
        print(_type, "...")
        nt = nx.read_gpickle("disk/network_{}.gpickle".format(f_label))
        print("type(n) =", type(nt))
        _gt = nx2gt(nt)
        _gt.save("disk/network_{}.gt".format(f_label))

    # build IRA all network
    # ira_tweet_ids = []
    # for line in open("data/IRA-tweets.json"):
    #     tweet_id = str(json.loads(line.strip())["user_id"])
    #     ira_tweet_ids.append(tweet_id)
    # get_all_network(ira_tweet_ids, "disk/ira")

    # make_all_network("disk/whole")
    # change_network("disk/whole")
    # save_network_gt()

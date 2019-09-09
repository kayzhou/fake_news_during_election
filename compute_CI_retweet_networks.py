# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    compute_CI_retweet_networks.py                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:27:34 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/02 18:01:09 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #


import json
import os
# import pickle
import sys
import time
from functools import partial
from pathlib import Path, PurePath

import graph_tool.all as gt
import numpy as np
from joblib import Parallel, delayed

PACKAGE_PARENT = '.'
SCRIPT_DIR = os.path.dirname(os.path.realpath(
    os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
import CIcython

#%% add CI values to graph
def add_CI_to_graph(graph_file):
    print('pid ' + str(os.getpid()) + ' loading graph ' + graph_file)
    graph = gt.load_graph(graph_file)

    # for direction in ['out', 'both', 'in']:
    # for direction in ['out', 'undir', 'in', 'both']:
    for direction in ['undir', 'out', 'in']:
        print('pid ' + str(os.getpid()) + ' -- ' + direction)
        t0 = time.time()
        CIranks, CImap = CIcython.compute_graph_CI(graph, rad=2,
                                                   direction=direction,
                                                   verbose=True)
        # print(CImap)
        t1 = time.time() - t0
        # print('pid ' + str(os.getpid()) + ' -- ' + str(t1))
        # print('pid ' + str(os.getpid()) + ' saving CIranks ' + direction + '_' + graph_file)
        # CI_file_name = 'data/CI_rst/CI_{}_{}.rst'.format(
        #     direction, os.path.split(graph_file)[1][:-3])
        # np.save(CI_file_name, np.array([CIranks, CImap]))

        graph.vp['CI_' + direction] = graph.new_vertex_property('int64_t', vals=0)
        graph.vp['CI_' + direction].a = CImap

    # print('pid ' + str(os.getpid()) + ' -- adding katz centrality')
    # graph.vp['katz'] = gt.katz(graph)
    # graph.set_reversed(True)
    # graph.vp['katz_rev'] = gt.katz(graph)
    # graph.set_reversed(False)

    print('pid ' + str(os.getpid()) + ' saving graph ' + graph_file)
    graph.save(graph_file)


if __name__ == "__main__":
    # save_dir = '/home/alex/kayzhou/Argentina_election/disk/data'
    # for in_name in os.listdir(save_dir):
    #     # print(in_name)
    #     if in_name.endswith("ALL.gt"):
    #         add_CI_to_graph(os.path.join(save_dir, in_name))

    # name_labels = [
    #     "fake",
    #     "extreme bias (right)",
    #     "right",
    #     "right leaning",
    #     "center",
    #     "left leaning",
    #     "left",
    #     "extreme bias (left)",
    #     "local",
    #     "radio",
    # ]

    # save_dir = 'disk/network'
    # for in_name in name_labels:
    #     add_CI_to_graph(os.path.join(save_dir, in_name + ".gt"))

    # add_CI_to_graph("data/IRA_two_layers.gt")

    print("Lets go!")
    for in_name in Path().rglob("data/graph/*.gt"):
        add_CI_to_graph(str(in_name))


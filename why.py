from my_weapon import *
from collections import defaultdict, Counter
from IPython.display import display
from tqdm import tqdm_notebook as tqdm

labels = [
    "fake",
    "extreme bias (right)",
    "right",
    "right leaning",
    "center",
    "left leaning",
    "left",
    "extreme bias (left)"
]

# 方向 both undir out in

import graph_tool.all as gt

def build_CI_rank(graph_file):
    rst = {}
    g = gt.load_graph(graph_file)
    user_CI = {g.vp.id[v]: g.vp.CI_out[v] for v in g.vertices()}
    rst["out_CI"] = user_CI
    st_user_CI = sorted(user_CI.items(), key=lambda d: d[1], reverse=True)
    rank = {d[0]: i for i, d in enumerate(st_user_CI)}
    rst["out_id"] = st_user_CI
    rst["out_rank"] = rank
    
#     user_CI = {g.vp.id[v]: g.vp.CI_undir[v] for v in g.vertices()}
#     rst["undir_CI"] = user_CI
#     st_user_CI = sorted(user_CI.items(), key=lambda d: d[1], reverse=True)
#     rst["undir_id"] = st_user_CI
    
#     user_CI = {g.vp.id[v]: g.vp.CI_both[v] for v in g.vertices()}
#     rst["both_CI"] = user_CI
#     st_user_CI = sorted(user_CI.items(), key=lambda d: d[1], reverse=True)
#     rst["both_id"] = st_user_CI

    user_CI = {g.vp.id[v]: g.vp.CI_in[v] for v in g.vertices()}
    rst["in_CI"] = user_CI
    st_user_CI = sorted(user_CI.items(), key=lambda d: d[1], reverse=True)
    rank = {d[0]: i for i, d in enumerate(st_user_CI)}
    rst["in_id"] = st_user_CI
    rst["in_rank"] = rank
    
    return rst

# all_users = pd.read_csv("data/all-users-mbfc.csv", index_col="user_id", dtype={"user_id": str})
users = pd.read_csv("data/all-users-nc.csv", index_col="user_id",
                    usecols =["user_id", "is_IRA"], dtype={"user_id": str, "is_IRA": int})
IRA_users = users[users.is_IRA > 0]
print(len(IRA_users))

top_num = 1000

len_intersection = {}
dict_CI = defaultdict(dict)

for label in labels:
    print(label, "...")
    rst = build_CI_rank("disk/network/{}_nc.gt".format(label))
#     for dire in ["out", "undir", "both", "in"]:
    for dire in ["out", "in"]:
        user_CI = rst[dire + "_CI"]
        rank = rst[dire + "_rank"]
        ira_rank = {}
        # top list
        set_CI_users = set([d[0] for d in rst[dire + "_id"][:top_num]])

        set_source_users = set()
        IRA_CI = []
        for user_id, row in IRA_users.iterrows():
            set_source_users.add(user_id)
            try:
                IRA_CI.append(user_CI[user_id])
                ira_rank[user_id] = rank[user_id]
            except:
                pass
            
        print("---- IRA rank ----")
        ira_rank = sorted(ira_rank.items(), key=lambda d: d[1])
#         print(ira_rank)
        
        len_intersection[dire + "_" + label] = len(set_CI_users & set_source_users)
        # print("参与人数：", len(IRA_CI), len(sort_user_CI))
        IRA_CI = pd.Series(IRA_CI)
        IRA_CI_sum = IRA_CI.sum()
        
        print("IRA sum", np.log10(IRA_CI_sum))
        for i, d in enumerate(rst[dire + "_id"]):
            dlog = np.log10(d[1])
            if dlog > IRA_CI_sum:
                print(i, d[0], dlog)
        
#         IRA_CI_mean = IRA_CI.mean()
        IRA_CI_mean = IRA_CI.mean()
        all_CI = pd.Series(list(user_CI.values()))
#         all_CI_mean = all_CI.mean()
        all_CI_mean = all_CI.mean()

        dict_CI[label][dire + "_IRA"] = IRA_CI_mean
        dict_CI[label][dire + "_All users"] = all_CI_mean
        dict_CI[label][dire + "_IRA dist"] = IRA_CI
        dict_CI[label][dire + "_All dist"] = all_CI

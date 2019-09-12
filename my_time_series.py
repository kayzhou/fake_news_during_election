import pendulum

from my_weapon import *

"""
针对 disk/all-url-tweets.json 进行分析

默认15分钟的粒度
"""

from pyloess import stl
    
# LOESS smoothing
def get_day(dt):
    return pendulum.parse(dt).format("YYYY-MM-DD 00:00:00")


def get_hour(dt):
    return pendulum.parse(dt).format("YYYY-MM-DD HH:00:00")


def get_15min(dt):
    _dt = pendulum.parse(dt)
    t0 = pendulum.parse(_dt.format("YYYY-MM-DD HH:00:00"))
    t1 = t0.add(minutes=15)
    t2 = t0.add(minutes=30)
    t3 = t0.add(minutes=45)

    if t0 <= _dt < t1:
        return t0
    elif _dt < t2:
        return t1
    elif _dt < t3:
        return t2
    else:
        return t3


def get_15min_file():
    with open("disk/user_time_15Mins.txt", "w") as f:
        for line in tqdm(open('disk/user_time.txt')):
            w = line.strip().split()
            u = w[1]
            _dt = w[2] + " " + w[3]
            _dt = get_15min(_dt).to_datetime_string()
            f.write(f"{w[0]} {w[1]} {_dt}\n")
    

def cal_ts(dts, resolution="15Min"):
    """
    真的牛逼！
    """
    ts = pd.to_datetime(dts)
    ts = ts.value_counts()
    ts = ts.resample(resolution).sum()
    return ts


def cal_ts_day(dts):
    start = get_day(dts[0])
    end = get_day(dts[-1])
    rng = pd.date_range(start, end, freq='D')
    ts = pd.Series(0, rng)
    for dt in dts:
        now = get_day(dt)
        ts[now] += 1
    return ts


def cal_ts_48hours(dts):
    start = get_hour(dts[0])
    rng = pd.date_range(start, periods=48, freq="H")
    ts = pd.Series(0, rng)
    for dt in dts:
        now = get_hour(dt)
        if now in ts:
            ts[now] += 1
    return ts


def plot_day(i, url, sorted_dts, sorted_dts2=None, save=False):
    """
    包含了两条线！
    """

    plt.figure(figsize=(10, 6))
    ts = cal_ts_day(sorted_dts)
    ts.plot()

    if sorted_dts2:
        ts2 = cal_ts_day(sorted_dts2)
        ts2.plot()

    # configure
    plt.ylabel('N of tweets with this fake news', fontsize=15)
    plt.xticks(fontsize=11); plt.yticks(fontsize=11)
#     plt.xlabel('$Date$', fontsize=15)
#     plt.title(url)

    if save:
        plt.savefig('fig/{}-{}-overall-spread.pdf'.format(i, url), dpi=300)
    else:
        plt.show()

    plt.close()


def plot_48hours(i, url, sorted_dts, sorted_dts2=None, save=False):
    """
    包含了两条线！
    """

#     print(url)
#     print("实际传播开始和结束时间：", sorted_dts[0], sorted_dts[-1])

    plt.figure(figsize=(10, 6))
    ts = cal_ts_48hours(sorted_dts)
    ts.plot()

    if sorted_dts2:
        ts2 = cal_ts_48hours(sorted_dts2)
        ts2.plot()


    # configure
    plt.ylabel('N of tweets with this fake news', fontsize=15)
    plt.xticks(fontsize=11); plt.yticks(fontsize=11)
#     plt.xlabel('$Date$', fontsize=15)
#     plt.title(url)

    if save:
        plt.savefig('fig/{}-{}-first-48-hours.pdf'.format(i, url), dpi=300)
    else:
        plt.show()

    plt.close()


# from stldecompose import decompose, forecast
import matplotlib.ticker as ticker
IRA_data = pd.read_csv("data/ira-tweets-ele.csv")

def format_date(x, pos=None):
    #保证下标不越界,很重要,越界会导致最终plot坐标轴label无显示
    # thisind = np.clip(int(x+0.5), 0, N-1)
    print(type(x), x)
    return x.strftime('%Y-%m-%d')
    
import pendulum
from datetime import datetime

def load_should_remove():
    should_remove_15Min = []
    
    for line in open("data/should_be_removed_in_timeseries.txt"):
        _dt = line.strip()
        _start = datetime.strptime(_dt, '%Y-%m-%d %H:%M:%S')
        for _dt in pd.date_range(start=_start, periods=4 * 24, freq="15Min"):
            should_remove_15Min.append(pd.to_datetime(_dt))

    return should_remove_15Min

# 载入一些需要的数据
should_remove_15Min = load_should_remove()
user_support = json.load(open("disk/user_hillary_trump.json"))
users_opinion = {}
opinion = Counter()

for uid, v in tqdm(user_support.items()):
    if v[0] > v[1]:
        users_opinion[uid] = "C"
        opinion["C"] += 1
    elif v[0] < v[1]:
        users_opinion[uid] = "T"
        opinion["T"] += 1
    else:
        users_opinion[uid] = "U"
        opinion["U"] += 1


from fake_identify import Are_you_IRA

Putin = Are_you_IRA()

        
def get_tsss(cN, layer="one"):
    """
    获取IRA和non-IRA的活动时间序列
    """
    def get_ts(IRA_nodes):
        dts = []
        for i, row in tqdm(IRA_data.iterrows()):
            u = Putin.uncover(row.userid)
            if u in IRA_nodes:
                _dt = row.tweet_time
                # Move to EST
                _dt = pendulum.parse(_dt).add(hours=-4).to_datetime_string()
                dts.append(_dt)
        ts = pd.to_datetime(dts)
        ts = ts.value_counts()
        ts = ts.resample("15Min").sum()
        ts = ts[(ts.index >= "2016-06-01") & (ts.index < "2016-11-09")]
        ts = ts[~ts.index.isin(should_remove_15Min)]
        return ts
    
    def get_non(non_IRA_nodes):
        non_dts = []
        for line in tqdm(open('disk/user_time.txt')):
            w = line.strip().split()
            uid = w[1]
            _dt = w[2] + " " + w[3]
            if uid in non_IRA_nodes:
                non_dts.append(_dt)

        non_ts = pd.to_datetime(non_dts)
        non_ts = non_ts.value_counts()
        non_ts = non_ts.resample("15Min").sum()
        non_ts = non_ts[(non_ts.index >= "2016-06-01") & (non_ts.index < "2016-11-09")]
        non_ts = non_ts[~non_ts.index.isin(should_remove_15Min)]
        return non_ts

    G = nx.read_gpickle(f"data/graph/C{cN}-{layer}-layer.gpickle")
    IRA_nodes = set([n for n in G.nodes if Putin.check(n)])
    non_IRA_nodes = set([n for n in G.nodes if not Putin.check(n)])
    print("IRA and non-IRA:", len(IRA_nodes), len(non_IRA_nodes))
    
    influ = {line.strip() for line in open(f"data/influencers/C{cN}-uid.txt")}
    non_IRA_influ_nodes = set([n for n in G.nodes 
                              if not Putin.check(n) and n in influ])
    non_IRA_non_influ_nodes = set([n for n in G.nodes 
                                  if not Putin.check(n) and n not in influ])
    print("non-IRA influ and non-influ:", len(non_IRA_influ_nodes), len(non_IRA_non_influ_nodes))
    
    T_IRA_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                       and users_opinion[n] == "T"])
    C_IRA_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                       and users_opinion[n] == "C"])
    print("Trump & Clinton:", len(T_IRA_nodes), len(C_IRA_nodes))

    T_flu_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                       and users_opinion[n] == "T" and n in influ])
    C_flu_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                       and users_opinion[n] == "C" and n in influ])
    print("Trump & Clinton (flu):", len(T_flu_nodes), len(C_flu_nodes))
    
    T_nonflu_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                          and users_opinion[n] == "T" and n not in influ])
    C_nonflu_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion
                          and users_opinion[n] == "C" and n not in influ])
    print("Trump & Clinton (non flu):", len(T_nonflu_nodes), len(C_nonflu_nodes))    
    
    ts = get_ts(IRA_nodes)
    non_ts = get_non(non_IRA_nodes)
    influ_ts = get_non(non_IRA_influ_nodes)
    non_influ_ts = get_non(non_IRA_non_influ_nodes)
    
    T_ts = get_non(T_IRA_nodes)
    C_ts = get_non(C_IRA_nodes)
    T_flu_ts = get_non(T_flu_nodes)
    C_flu_ts = get_non(C_flu_nodes)    
    T_nonflu_ts = get_non(T_nonflu_nodes)
    C_nonflu_ts = get_non(C_nonflu_nodes)
    
    tsts = pd.DataFrame({
        "ts": ts, "non_ts": non_ts, "T_ts": T_ts, "C_ts": C_ts,
        "influ_ts": influ_ts, "non_influ_ts": non_influ_ts,
        "T_flu_ts": T_flu_ts, "C_flu_ts": C_flu_ts,
        "T_nonflu_ts": T_nonflu_ts, "C_nonflu_ts": C_nonflu_ts,
    })
    tsts.to_pickle(f"data/tsts/C{cN}-{layer}-layer.pl")


def get_tsss_user(cN, layer="one"):
    def get_15min(dt):
        _dt = pendulum.parse(dt)
    t0 = pendulum.parse(_dt.format("YYYY-MM-DD HH:00:00"))
    t1 = t0.add(minutes=15)
    t2 = t0.add(minutes=30)
    t3 = t0.add(minutes=45)

    if t0 <= _dt < t1:
        return t0
    elif _dt < t2:
        return t1
    elif _dt < t3:
        return t2
    else:
        return t3
    
    def get_ts(IRA_nodes):
        user_set = set()
        dts = []
        for i, row in tqdm(IRA_data.iterrows()):
            u = Putin.uncover(row.userid)
            if u in IRA_nodes:
                _dt = row.tweet_time
                _dt = pendulum.parse(_dt).add(hours=-4).to_datetime_string()
                _dt = get_15min(_dt).to_datetime_string()
                if u + "~" + _dt not in user_set:
                    user_set.add(u + "~" + _dt)
                    dts.append(_dt)
                    
        ts = pd.to_datetime(dts)
        ts = ts.value_counts()
        ts = ts.resample("15Min").sum()
        ts = ts[(ts.index >= "2016-06-01") & (ts.index < "2016-11-09")]
        ts = ts[~ts.index.isin(should_remove_15Min)]
        return ts
    
    def get_non(non_IRA_nodes):
        user_set = set()
        non_dts = []
        for line in tqdm(open('disk/user_time_15Mins.txt')):
            w = line.strip().split()
            u = w[1]
            _dt = w[2] + " " + w[3]
            if uid in non_IRA_nodes:
                if u + "~" + _dt not in user_set:
                    user_set.add(u + "~" + _dt)
                    non_dts.append(_dt)
                    
        non_ts = pd.to_datetime(non_dts)
        non_ts = non_ts.value_counts()
        non_ts = non_ts.resample("15Min").sum()
        non_ts = non_ts[(non_ts.index >= "2016-06-01") & (non_ts.index < "2016-11-09")]
        non_ts = non_ts[~non_ts.index.isin(should_remove_15Min)]
        return non_ts

    G = nx.read_gpickle(f"data/graph/C{cN}-{layer}-layer.gpickle")
    IRA_nodes = set([n for n in G.nodes if Putin.check(n)])
    non_IRA_nodes = set([n for n in G.nodes if not Putin.check(n)])
    T_IRA_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion and users_opinion[n] == "T"])
    C_IRA_nodes = set([n for n in G.nodes if not Putin.check(n) and n in users_opinion and users_opinion[n] == "C"])
    
    ts = get_ts(IRA_nodes)
    non_ts = get_non(non_IRA_nodes)
    T_ts = get_non(T_IRA_nodes)
    C_ts = get_non(C_IRA_nodes)

    tsts = pd.DataFrame({"ts": ts, "non_ts": non_ts, "T_ts": T_ts, "C_ts": C_ts})
    tsts.to_pickle(f"data/tsts/C{cN}-{layer}-layer-user.pl")
    

def calculate_resid(cN, layer="two"):
    """
    消灭季节性特征
    """
    # remove seasonality and trend
    stl_params = dict(
        np = 96, # period of season
        ns = 95, # seasonal smoothing
        nt = None, # trend smooting int((1.5*np)/(1-(1.5/ns)))
        nl = None, # low-pass filter leat odd integer >= np
        isdeg=1,
        itdeg=1,
        ildeg=1,
        robust=True,
        ni = 1,
        no = 5)
         
    tsts = pd.read_pickle(f"data/tsts/C{cN}-{layer}-layer.pl")
    resid = pd.DataFrame(index=tsts.index)
    print("Loaded!")
    for col in ["ts", "non_ts", "T_ts", "C_ts", "influ_ts", "non_influ_ts",
                "T_flu_ts", "C_flu_ts", "T_nonflu_ts", "C_nonflu_ts"]:
        print(col)
        tsts[col] = tsts[col].fillna(0)
        resid[col] = stl(tsts[col].values, **stl_params).residuals

    resid.to_pickle(f"data/tsts/resid_C{cN}-{layer}-layer.pl")
    print("saved!")
        
        
def analyze_ts_of_communities(cN, layer="one", user=False):
    if user:
        tsts = pd.read_pickle(f"data/tsts/C{cN}-{layer}-layer-user.pl")
    else:
        tsts = pd.read_pickle(f"data/tsts/C{cN}-{layer}-layer.pl")
    
    # print(tsts)
    sns.set(style="white", font_scale=1.2)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    color = 'tab:red'
    ax1.set_ylabel('IRA', color=color)  # we already handled the x-label with ax1
    ax1.plot("ts", data=tsts, color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('non-IRA', color=color)  # we already handled the x-label with ax1
    ax2.plot("non_ts", data=tsts, color=color, lw=0.8)
    ax2.tick_params(axis='y', labelcolor=color)
    
    # ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    # fig.autofmt_xdate()
    
    plt.savefig(f"fig/c{cN}-{layer}-layer-ts.pdf", dpi=300)
    # plt.show()
    plt.close()
    
    if user:
        tsts_resid = pd.read_pickle(f"data/tsts/resid_C{cN}-{layer}-layer-user.pl")
    else:
        tsts_resid = pd.read_pickle(f"data/tsts/resid_C{cN}-{layer}-layer.pl")
        
    # print(tsts_resid)
    sns.set(style="white", font_scale=1.2)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    color = 'tab:red'
    ax1.set_ylabel('IRA (residuals)', color=color)  # we already handled the x-label with ax1
    # ax1.set_ylim((-600, 600))
    ax1.plot("ts", data=tsts_resid, color=color, lw=1)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel("non-IRA (residuals)", color=color)  # we already handled the x-label with ax1
    # ax2.set_ylim((-20000, 20000))
    ax2.plot("non_ts", data=tsts_resid, color=color, lw=1)
    ax2.tick_params(axis='y', labelcolor=color)
    plt.savefig(f"fig/c{cN}-{layer}-layer-resid.pdf", dpi=300)
    # plt.show()
    plt.close()
    
    # sns.set(style="white")
    # 相关性分析
#     f = plt.figure(figsize=(7, 6))
#     plt.matshow(tsts.corr(), fignum=f.number, cmap='Purples')
#     plt.xticks(range(tsts.shape[1]), tsts.columns, fontsize=11, rotation=45)
#     plt.yticks(range(tsts.shape[1]), tsts.columns, fontsize=11)
#     cb = plt.colorbar()
#     cb.ax.tick_params(labelsize=14)
#     # plt.title('Correlation Matrix', fontsize=16)

    print(tsts.corr())
#     ax = sns.heatmap(tsts.corr())
#     plt.close()
    print(tsts_resid.corr())
#     ax = sns.heatmap(tsts_resid.corr())
#     plt.show()
#     plt.close()
    
    # 因果分析
    from statsmodels.tsa.stattools import grangercausalitytests
    
    # print(tsts_resid.non_ts.dropna(), tsts_resid.ts.dropna())
    # for_gra = np.array([tsts_resid.non_ts.dropna(), tsts_resid.ts.dropna()]).T
    # # print(for_gra)
    # print(" ---------------------- IRA causes non-IRA ----------------------")
    # r1 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    # for _k, v in r1.items():
    #     print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
              
    # for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.non_ts.dropna()]).T
    # print(" ---------------------- non-IRA causes IRA ----------------------")
    # r2 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    # for _k, v in r2.items():
    #     print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")

    # "ts", "non_ts", "T_ts", "C_ts", "influ_ts", "non_influ_ts",
    # "T_flu_ts", "C_flu_ts", "T_nonflu_ts", "C_nonflu_ts"
    for_gra = np.array([tsts_resid.T_flu_ts.dropna(), tsts_resid.ts.dropna()]).T
    print(" ---------------------- IRA causes T flu ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
            
    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.T_flu_ts.dropna()]).T
    print(" ---------------------- T flu causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")        

    for_gra = np.array([tsts_resid.C_flu_ts.dropna(), tsts_resid.ts.dropna()]).T
    print(" ---------------------- IRA causes C flu ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
            
    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.C_flu_ts.dropna()]).T
    print(" ---------------------- C flu causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")  
        
    for_gra = np.array([tsts_resid.T_nonflu_ts.dropna(), tsts_resid.ts.dropna()]).T
    print(" ---------------------- IRA causes T nonflu ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
            
    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.T_nonflu_ts.dropna()]).T
    print(" ---------------------- T nonflu causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")  
        
    for_gra = np.array([tsts_resid.C_nonflu_ts.dropna(), tsts_resid.ts.dropna()]).T
    print(" ---------------------- IRA causes C nonflu ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
            
    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.C_nonflu_ts.dropna()]).T
    print(" ---------------------- C nonflu causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=24, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k} *15Mins\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")  
        

def analyze_ts_TC(cN, layer):
    # read
    tsts = pd.read_pickle(f"data/tsts/C{cN}-{layer}-layer.pl")

    sns.set(style="white", font_scale=1.2)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    color = 'tab:red'
    ax1.set_ylabel('IRA', color=color)  # we already handled the x-label with ax1
    ax1.plot("ts", data=tsts, color=color, label="IRA")
    ax1.tick_params(axis='y', labelcolor=color)
    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    ax2.set_ylabel('non-IRA')  # we already handled the x-label with ax1
    ax2.plot("T_ts", data=tsts, color='tab:green', alpha=0.7, lw=0.8, label="Trump supporters")
    ax2.plot("C_ts", data=tsts, color='tab:blue', alpha=0.7, lw=0.8, label="Clinton supporters")
    plt.savefig(f"fig/c{cN}-{layer}-layer-TC-ts.pdf", dpi=300)
    plt.legend()
    # plt.show()
    plt.close()
    
    # read

    tsts_resid = pd.read_pickle(f"data/tsts/resid_C{cN}-{layer}-layer.pl")
    sns.set(style="white", font_scale=1.2)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    color = 'tab:red'
    ax1.set_ylabel('IRA (residuals)', color=color)  # we already handled the x-label with ax1
    # ax1.set_ylim((-600, 600))
    ax1.plot("ts", data=tsts_resid, color=color, lw=1, label="IRA")
    ax1.tick_params(axis='y', labelcolor=color)
#     ax1.legend()
    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    ax2.set_ylabel("non-IRA (residuals)")  # we already handled the x-label with ax1
    # ax2.set_ylim((-20000, 20000))
    ax2.plot("T_ts", data=tsts_resid, color='tab:green', alpha=0.7, lw=1, label="Trump supporters")
    ax2.plot("C_ts", data=tsts_resid, color='tab:blue', alpha=0.7,  lw=1, label="Clinton supporters")
#     ax2.legend()
    plt.legend()
    plt.savefig(f"fig/c{cN}-{layer}-layer-TC-resid.pdf", dpi=300)
    # plt.show()
    plt.close()
    
    # 因果分析
    from statsmodels.tsa.stattools import grangercausalitytests
    
    # print(tsts_resid.non_ts.dropna(), tsts_resid.ts.dropna())
    
    for_gra = np.array([tsts_resid.T_ts.dropna(), tsts_resid.ts.dropna()]).T
    # print(for_gra)
    print(" ---------------------- IRA causes Trump supporters ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=16, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k}\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")

    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.T_ts.dropna()]).T
    print(" ---------------------- Trump supporters causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=16, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k}\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
              
    for_gra = np.array([tsts_resid.C_ts.dropna(), tsts_resid.ts.dropna()]).T
    # print(for_gra)
    print(" ---------------------- IRA causes Clinton supporters ----------------------")
    r1 = grangercausalitytests(for_gra, maxlag=16, verbose=False)
    for _k, v in r1.items():
        print(f"lag={_k}\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")

    for_gra = np.array([tsts_resid.ts.dropna(), tsts_resid.C_ts.dropna()]).T
    print(" ---------------------- Clinton supporters causes IRA ----------------------")
    r2 = grangercausalitytests(for_gra, maxlag=16, verbose=False)
    for _k, v in r2.items():
        print(f"lag={_k}\tF={v[0]['ssr_ftest'][0]:.4f}\tp-value={v[0]['ssr_ftest'][1]:.4f}")
        
        
if __name__ == "__main__":
    for i in range(1, 4):
        get_tsss(i, "two")
        calculate_resid(i, "two")
        # analyze_ts_of_communities(i, "two")
        # analyze_ts_TC(i, "two")
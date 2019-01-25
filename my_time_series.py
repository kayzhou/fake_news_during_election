import pendulum

from my_weapon import *

"""
针对 disk/all-url-tweets.json 进行分析

默认15分钟的粒度
"""

def get_day(dt):
    return pendulum.parse(dt).format("YYYY-MM-DD 00:00:00")


def get_hour(dt):
    return pendulum.parse(dt).format("YYYY-MM-DD HH:00:00")


def get_15min(dt):
    t0 = pendulum.parse(dt).format("YYYY-MM-DD HH:00:00")
    t1 = t0.add(minutes=15)
    t2 = t1.add(minutes=15)
    t3 = t2.add(minutes=15)

    if t0 <= dt < t1:
        return t0
    elif dt < t2:
        return t1
    elif dt < t3:
        return t2
    else:
        return t3


def cal_ts(dts, resolution="1Min"):
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


class Kay_best_ts(object):
    def __init__(self):
        pass

    def load_url_ts(self):
        pass
        # self.url_ts = json.loads()


if __name__ == "__main__":
    Lebron = Kay_best_ts()
import pandas as pd
from urllib.parse import urlparse
import requests


def keep_url():
    tweets = pd.read_csv('data/ira_tweets_head.csv')
    tweets['hostname'] = -1
    print(len(tweets))
    for i, row in tweets.iterrows():
        if not isinstance(row['urls'], str):
            # tweets.drop(i, inplace=True)
            pass
        elif row['urls'][1: -1] == '':
            # tweets.drop(i, inplace=True)
            pass
        else:
            try:
                url = row['urls'][1: -1]
                res = requests.head(url)
                hostname = urlparse(res.headers.get('location')).hostname
                if hostname:
                    # print(i, hostname)
                    tweets['hostname'] = hostname
                    print(i, url, hostname, sep='\t')
                # else:
                #     tweets.drop(i, inplace=True)
            except Exception as e:
                print(i, e)


    print(len(tweets))
    tweets.to_csv("data/ira_tweets_url.csv")


if __name__ == "__main__":
    keep_url()


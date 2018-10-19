import pandas as pd
from urllib.parse import urlparse
import requests

short_url = set(['bit.ly', 'dlvr.it', 'goo.gl', 'j.mp', 'ift.tt', 'nyp.st', 'ln.is', 'trib.al', 'cnn.it'])

def keep_url():
    tweets = pd.read_csv('data/ira_tweets_csv_hashed.csv')
    # tweets['hostname'] = -1
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
                hostname = urlparse(res).hostname
                # short url
                if hostname in short_url:
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


if __name__ == "__main__":
    keep_url()


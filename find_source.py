import sqlite3
import json

host_label = json.load(open('data/host_label.json'))

conn = sqlite3.connect("/home/alex/network_workdir/elections/databases/urls_db.sqlite")
c = conn.cursor()
col_names = [t[0] for t in c.description]
c.execute('''SELECT * FROM urls;''')
data = c.fetchall()

for i, d in enumerate(data):
    if host_label[d[8]] == "fake":
        json_d = {k: v for k, v in zip(col_names, d)}
        print(json_d)
import sqlite3
import json

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

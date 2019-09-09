import pandas as pd
from pathlib import Path

# in_dir = "../data"

# for in_name in Path(in_dir).glob('*.csv'):
#     print(in_name.name)
#     for line in in_name.open():
#         print(" & ".join(line.strip().split(",")) + "\\")
        
in_dir = "../data/influencers"

for in_name in Path(in_dir).glob('*.txt'):
    print(in_name.name)
    for line in in_name.open():
        w = line.strip().split(" ")
        name = w[1]
        print(name)
        # print(" & ".join(line.strip().split(" ")) + "\\")
        

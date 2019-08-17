import pandas as pd
from pathlib import Path

in_dir = "../data"

for in_name in Path(in_dir).glob('*.csv'):
    print(in_name.name)
    for line in in_name.open():
        print(" & ".join(line.strip().split(",")) + "\\")
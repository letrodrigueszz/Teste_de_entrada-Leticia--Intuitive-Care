import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
import pandas as pd
p = Path('data/raw/caderno_jun23.zip')
print('File:', p)
try:
    x = pd.read_excel(p, sheet_name=None)
    print('Sheets:', list(x.keys()))
    for name, df in x.items():
        print('\nSheet:', name)
        print('Columns:', list(df.columns))
        print('Sample rows:')
        print(df.head(5).to_string(index=False))
        break
except Exception as e:
    print('Error reading excel:', e)

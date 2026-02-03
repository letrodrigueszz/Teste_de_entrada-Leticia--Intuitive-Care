import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
import processor
p = Path('data/raw/caderno_jun23.zip')
df = processor.process_file(p, 'unknown', 'jun23')
print('DF len:', len(df))
print('Cols:', list(df.columns))
print(df.head(10).to_string(index=False))

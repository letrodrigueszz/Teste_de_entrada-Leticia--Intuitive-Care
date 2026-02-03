import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
import pandas as pd
import processor

p = Path('data/raw/caderno_jun23.zip')
print('File:', p)
try:
    sheets = pd.read_excel(p, sheet_name=None)
    scores = []
    for name, df in sheets.items():
        try:
            cnpj_col = processor.detect_cnpj_col_by_values(df)
            valor_col = processor.find_valor_col_by_content(df)
            # compute some simple stats
            cnpj_count = 0
            if cnpj_col:
                for v in df[cnpj_col].head(500):
                    s = ''.join([c for c in str(v) if c.isdigit()])
                    if len(s)==14:
                        cnpj_count += 1
            valor_count = 0
            if valor_col:
                for v in df[valor_col].head(500):
                    if processor.parse_valor(v) is not None:
                        valor_count += 1
            print(f"Sheet '{name}': cnpj_col={cnpj_col}, cnpj_count={cnpj_count}, valor_col={valor_col}, valor_count={valor_count}")
        except Exception as e:
            print('error sheet', name, e)
except Exception as e:
    print('Error reading excel:', e)

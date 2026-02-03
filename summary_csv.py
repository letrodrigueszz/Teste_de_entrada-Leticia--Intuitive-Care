import pandas as pd, os
p='data/processed/consolidado_despesas.csv'
if not os.path.exists(p):
    print('CSV ausente')
else:
    try:
        df = pd.read_csv(p, dtype=str, encoding='utf-8')
    except Exception:
        df = pd.read_csv(p, dtype=str, encoding='latin1')
    print('shape:', df.shape)
    print('\ncolumns:', list(df.columns))
    print('\nfirst 8 rows:\n')
    print(df.head(8).to_string(index=False))

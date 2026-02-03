import pandas as pd
p='data/raw/caderno_jun23.zip'
print('file',p)
try:
    x = pd.read_excel(p, sheet_name=None)
    print('sheets:', list(x.keys()))
    for name, df in x.items():
        print('sheet', name, 'shape', df.shape)
        print('columns:', list(df.columns))
        print(df.head(3).to_string())
except Exception as e:
    print('error', e)

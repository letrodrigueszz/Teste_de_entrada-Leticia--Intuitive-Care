import processor as pd
p='data/raw/caderno_jun23.zip'
try:
    df = pd.read_excel(p, sheet_name='tab 10')
    print('shape', df.shape)
    print('columns:', list(df.columns))
    print(df.head(20).to_string())
except Exception as e:
    print('error', e)

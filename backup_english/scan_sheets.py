import processor as pd
import os

KEYS=['cnpj','raz','razao','razão','valor','despesa','sinistro']
for fname in os.listdir('data/raw'):
    if not fname.lower().endswith('.zip'):
        continue
    p=os.path.join('data/raw',fname)
    print('\nFILE',fname)
    try:
        x = pd.read_excel(p, sheet_name=None)
    except Exception as e:
        print('  read error', e)
        continue
    for sname, df in x.items():
        # check columns
        cols = ' '.join([str(c).lower() for c in df.columns])
        found = [k for k in KEYS if k in cols]
        if found:
            print('  sheet',sname,'found in columns ->', found)
            continue
        # check first 5 rows
        sample_parts = []
        for col in df.columns:
            try:
                vals = df[col].head(5).astype(str).str.lower().tolist()
            except Exception:
                vals = df[col].head(5).astype(str).tolist()
                vals = [str(v).lower() for v in vals]
            sample_parts.append(' '.join([str(v) for v in vals]))
        sample = ' '.join(sample_parts)
        found2=[k for k in KEYS if k in sample]
        if found2:
            print('  sheet',sname,'found in sample ->', found2)

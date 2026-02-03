import zipfile, os
from io import BytesIO
import pandas as pd

raw='data/raw'
for fname in os.listdir(raw):
    if not fname.lower().endswith('.zip'):
        continue
    p=os.path.join(raw,fname)
    print('ZIP:', fname, 'size', os.path.getsize(p))
    try:
        with zipfile.ZipFile(p) as z:
            names = z.namelist()
            if not names:
                print('  empty zip')
            for name in names:
                print('  member:', name)
                if name.lower().endswith(('.csv','.txt')):
                    with z.open(name) as f:
                        head = f.read(1024).decode('latin1',errors='ignore')
                        print('    head:', head.splitlines()[:5])
                elif name.lower().endswith(('.xls','.xlsx')):
                    try:
                        data = z.read(name)
                        df = pd.read_excel(BytesIO(data), nrows=0)
                        print('    columns:', list(df.columns))
                    except Exception as e:
                        print('    excel read error:', e)
    except Exception as e:
        print('  zip error', e)

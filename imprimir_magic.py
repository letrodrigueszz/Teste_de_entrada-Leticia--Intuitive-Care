import os
raw='data/raw'
for fname in os.listdir(raw):
    p=os.path.join(raw,fname)
    with open(p,'rb') as f:
        b=f.read(8)
    print(fname, b.hex(), b)

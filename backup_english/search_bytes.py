import os
KEYS=[b'cnpj', b'valor', b'despesa', b'despesas', b'sinistro', b'razao', b'razao social', b'rao']
for fname in os.listdir('data/raw'):
    p=os.path.join('data/raw',fname)
    print('\nFILE',fname)
    with open(p,'rb') as f:
        b=f.read()
    for k in KEYS:
        if k in b.lower():
            print(' contains',k)
    # print some sample text
    s=b[:2000]
    try:
        print(s.decode('latin1')[:500])
    except Exception:
        pass

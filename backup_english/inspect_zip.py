import zipfile, os
p='data/processed/consolidado_despesas.zip'
if os.path.exists(p):
    print('ZIP exists, size:', os.path.getsize(p))
    try:
        with zipfile.ZipFile(p) as z:
            names = z.namelist()
            print('members:', names)
            if 'consolidado_despesas.csv' in names:
                data = z.read('consolidado_despesas.csv').decode('utf-8', errors='replace')
                print('\n---CSV sample (first 50 lines)---')
                for i, l in enumerate(data.splitlines()[:50], 1):
                    print(f'{i}: {l}')
            elif names:
                name = names[0]
                try:
                    data = z.read(name).decode('utf-8', errors='replace')
                    print(f"\n---Sample from {name} (first 50 lines)---")
                    for i, l in enumerate(data.splitlines()[:50], 1):
                        print(f'{i}: {l}')
                except Exception as e:
                    print('Could not read member', name, 'error:', e)
    except Exception as e:
        print('Invalid zip or error:', e)
else:
    print('ZIP not found')

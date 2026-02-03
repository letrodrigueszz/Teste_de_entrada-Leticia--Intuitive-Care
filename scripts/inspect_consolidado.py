import zipfile
import os
p = os.path.join('data','processed','consolidado_despesas.zip')
if not os.path.exists(p):
    print('ZIP não encontrado:', p)
    raise SystemExit(1)
with zipfile.ZipFile(p,'r') as z:
    names = z.namelist()
    print('Conteúdo do ZIP: ({} entradas)'.format(len(names)))
    for n in names:
        print('-', repr(n))
    csvs = [n for n in names if n.lower().endswith('.csv')]
    if not csvs:
        print('Nenhum CSV encontrado no ZIP')
        raise SystemExit(0)
    csvname = csvs[0]
    z.extract(csvname, 'data/processed')
    outpath = os.path.join('data','processed', csvname)
    print('\nExtraído:', outpath)
    print('\nPrimeiras 40 linhas do CSV:')
    with open(outpath, 'r', encoding='utf-8', errors='replace') as f:
        for i, line in enumerate(f):
            if i>=40:
                break
            print(line.rstrip())
print('\nOK')

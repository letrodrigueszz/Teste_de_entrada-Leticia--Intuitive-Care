import os, zipfile
p = 'data/processed/consolidado_despesas.csv'
z = 'data/processed/consolidado_despesas.zip'
print('csv_path:', p)
print('csv_exists:', os.path.exists(p))
if os.path.exists(p):
    print('csv_size:', os.path.getsize(p))
else:
    print('csv missing')
print('zip_path:', z)
print('zip_exists:', os.path.exists(z))
if os.path.exists(z):
    print('zip_size:', os.path.getsize(z))
    try:
        with zipfile.ZipFile(z) as zz:
            print('zip_namelist:', zz.namelist())
    except Exception as e:
        print('zip open error:', e)
else:
    print('zip missing')

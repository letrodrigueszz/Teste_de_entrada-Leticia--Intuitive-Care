from pathlib import Path
import zipfile
import sys
import os
# ensure project root is on sys.path so we can import processor
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import processor

RAW = Path('data/raw')
for f in sorted(RAW.iterdir()):
    if not f.is_file():
        continue
    print('---', f.name)
    try:
        if zipfile.is_zipfile(f):
            print('is_zipfile = True')
            # check if it's actually an OLE masquerading as zip
            if processor.is_ole_file(f):
                print('detected OLE signature inside .zip file')
                df = processor.process_file(f, 'unknown', f.stem)
                print('rows:', len(df))
            else:
                print('real zip, entries:', len(zipfile.ZipFile(f).namelist()))
        else:
            print('is_zipfile = False')
            if processor.is_ole_file(f):
                print('OLE signature detected')
            df = processor.process_file(f, 'unknown', f.stem)
            print('rows:', len(df))
    except Exception as e:
        print('error processing:', e)

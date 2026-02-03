import re
import csv
import zipfile
import io
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd


BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
RAW_DIR = Path("data/raw")
EXTRACT_DIR = Path("data/extracted")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = ["despesa", "despesas", "evento", "sinistro", "sinistros"]

def listar_trimestres():
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    pastas = [a["href"] for a in soup.find_all("a") if a.get("href","").endswith("/")]

    trimestres = []
    for ano in pastas:
        if ano.isdigit():
            ano_url = urljoin(BASE_URL, ano)
            ano_resp = requests.get(ano_url)
            ano_resp.raise_for_status()
            soup_ano = BeautifulSoup(ano_resp.text, "html.parser")
            for tri in soup_ano.find_all("a"):
                href = tri.get("href")
                if href and href.startswith("Q"):
                    trimestres.append((ano, href))

    return sorted(trimestres, reverse=True)[:3]


def download_zips(trimestres):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = []
    if trimestres:
        for ano, trimestre in trimestres:
            url_trimestre = f"{BASE_URL}{ano}{trimestre}"
            resp = requests.get(url_trimestre)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a"):
                href = a.get("href")
                if href and href.lower().endswith('.zip'):
                    file_url = urljoin(url_trimestre, href)
                    destino = RAW_DIR / f"{ano}_{trimestre}_{href}"
                    if destino.exists():
                        downloaded.append((destino, ano, trimestre))
                        continue
                    with requests.get(file_url, stream=True) as r:
                        r.raise_for_status()
                        with open(destino, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    downloaded.append((destino, ano, trimestre))
    else:
        # Fallback: scan base URL for any zip links (filter by keywords in filename)
        resp = requests.get(BASE_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # First, try direct zip links
        for a in soup.find_all("a"):
            href = a.get("href")
            if href and href.lower().endswith('.zip') and any(k in href.lower() for k in KEYWORDS):
                file_url = urljoin(BASE_URL, href)
                destino = RAW_DIR / href
                if not destino.exists():
                    with requests.get(file_url, stream=True) as r:
                        r.raise_for_status()
                        with open(destino, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                downloaded.append((destino, 'unknown', 'unknown'))

        # If still nothing, crawl one level deep into directories and look for zips
        if not downloaded:
            dirs = [a.get('href') for a in soup.find_all('a') if a.get('href','').endswith('/')]
            for d in dirs:
                try:
                    url_d = urljoin(BASE_URL, d)
                    r2 = requests.get(url_d, timeout=15)
                    r2.raise_for_status()
                    s2 = BeautifulSoup(r2.text, 'html.parser')
                    for a2 in s2.find_all('a'):
                        href2 = a2.get('href')
                        if href2 and href2.lower().endswith('.zip') and any(k in href2.lower() for k in KEYWORDS):
                            file_url = urljoin(url_d, href2)
                            destino = RAW_DIR / f"{d.strip('/')}_{href2}"
                            if not destino.exists():
                                with requests.get(file_url, stream=True) as r3:
                                    r3.raise_for_status()
                                    with open(destino, 'wb') as f3:
                                        for chunk in r3.iter_content(chunk_size=8192):
                                            f3.write(chunk)
                            downloaded.append((destino, 'unknown', d))
                except Exception:
                    continue
    return downloaded


def safe_extract_zip(zip_path: Path, ano: str, trimestre: str):
    dest = EXTRACT_DIR / f"{ano}_{trimestre}"
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        for member in z.namelist():
            # avoid path traversal
            member_path = Path(member)
            target = dest / member_path.name
            with z.open(member) as src, open(target, 'wb') as dst:
                dst.write(src.read())
    return dest


def detect_target_file(path: Path):
    name = path.name.lower()
    if any(k in name for k in KEYWORDS):
        return True
    # peek into file for header/first lines
    try:
        if path.suffix.lower() in ['.csv', '.txt']:
            with open(path, encoding='latin1', errors='ignore') as f:
                head = ''.join([next(f) for _ in range(5)])
                head = head.lower()
                return any(k in head for k in KEYWORDS) or 'cnpj' in head
        elif path.suffix.lower() in ['.xls', '.xlsx']:
            df = pd.read_excel(path, nrows=5)
            cols = ' '.join(map(str, df.columns)).lower()
            return any(k in cols for k in KEYWORDS) or 'cnpj' in cols
    except Exception:
        return False
    return False


def normalize_cnpj(cnpj_raw: str):
    if pd.isna(cnpj_raw):
        return ''
    s = re.sub(r"\D", "", str(cnpj_raw))
    return s.zfill(14) if s else ''


def is_ole_file(path: Path):
    """Detecta arquivos OLE (Excel .xls) pelo header mágico mesmo que a extensão seja .zip."""
    try:
        with open(path, 'rb') as fh:
            head = fh.read(8)
            return head.startswith(b'\xD0\xCF\x11\xE0')
    except Exception:
        return False


def parse_valor(v):
    if pd.isna(v):
        return None
    s = str(v)
    # remove currency symbols
    s = re.sub(r"[^0-9,.-]", "", s)
    # if has both '.' and ',', assume '.' thousands and ',' decimal
    if s.count(',') == 1 and s.count('.') > 0:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None


def map_columns(df: pd.DataFrame):
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    # CNPJ
    for k in cols:
        if 'cnpj' in k:
            mapping['CNPJ'] = cols[k]
            break
    # RazaoSocial
    for k in cols:
        if 'razao' in k or 'nome' in k or 'razão' in k:
            mapping['RazaoSocial'] = cols[k]
            break
    # Valor
    for k in cols:
        if 'valor' in k or 'vlr' in k or 'desp' in k:
            mapping['ValorDespesas'] = cols[k]
            break
    return mapping


def detect_cnpj_col_by_values(df: pd.DataFrame):
    # look for a column where many values become 14-digit numbers after stripping non-digits
    best = None
    best_ratio = 0.0
    for col in df.columns:
        try:
            vals = df[col].astype(str).fillna('')
        except Exception:
            vals = df[col].astype(str).fillna('')
        total = 0
        good = 0
        for v in vals.head(200):
            s = re.sub(r"\D", "", str(v))
            if s:
                total += 1
                if len(s) == 14:
                    good += 1
        if total > 0:
            ratio = good / total
            if ratio > best_ratio and ratio >= 0.2:
                best_ratio = ratio
                best = col
    return best


def find_valor_col_by_content(df: pd.DataFrame):
    # choose column with most parsable numeric values
    best = None
    best_count = 0
    for col in df.columns:
        count = 0
        for v in df[col].head(200):
            if parse_valor(v) is not None:
                count += 1
        if count > best_count and count >= 2:
            best_count = count
            best = col
    return best


def process_file(path: Path, ano: str, trimestre: str):
    ext = path.suffix.lower()
    # if file has .zip extension but is actually an OLE Excel (.xls) file, treat as .xls
    if ext not in ['.xls', '.xlsx'] and is_ole_file(path):
        ext = '.xls'
    try:
        print(f"[debug] process_file: {path} (ext override={ext})")
        if ext in ['.csv', '.txt']:
            # try common separators
            for sep in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(path, sep=sep, encoding='latin1', nrows=0)
                    # if read with 0 rows succeeded, re-read full
                    df = pd.read_csv(path, sep=sep, encoding='latin1')
                    break
                except Exception:
                    df = None
            if df is None:
                df = pd.read_csv(path, encoding='latin1', engine='python')
        elif ext in ['.xls', '.xlsx']:
                # try all sheets and pick those that contain our target columns or infer them by content
                try:
                    sheets = pd.read_excel(path, sheet_name=None)
                    print(f"[debug] excel: loaded {len(sheets)} sheets from {path}")
                    dfs = []
                    for sname, sdf in sheets.items():
                        mapping_try = map_columns(sdf)
                        print(f"[debug] sheet={sname} cols={list(sdf.columns)[:5]} mapping_try(init)={mapping_try}")
                        # if CNPJ header missing, try to detect by values
                        if 'CNPJ' not in mapping_try:
                            cnpj_col = detect_cnpj_col_by_values(sdf)
                            if cnpj_col:
                                mapping_try['CNPJ'] = cnpj_col
                        # if Valor missing, try to infer
                        if 'ValorDespesas' not in mapping_try:
                            valor_col = find_valor_col_by_content(sdf)
                            if valor_col:
                                mapping_try['ValorDespesas'] = valor_col
                        print(f"[debug] sheet={sname} mapping_try(after detection)={mapping_try}")

                        if 'CNPJ' in mapping_try and 'ValorDespesas' in mapping_try:
                            # keep only relevant columns and normalize their names for later processing
                            try:
                                keep_cols = []
                                # CNPJ
                                cnpj_col = mapping_try['CNPJ']
                                valor_col = mapping_try['ValorDespesas']
                                keep_cols.append(cnpj_col)
                                # RazaoSocial: prefer detected mapping, else try to pick a plausible column
                                razao_col = mapping_try.get('RazaoSocial')
                                if not razao_col:
                                    for c in sdf.columns:
                                        if c != cnpj_col and c != valor_col:
                                            razao_col = c
                                            break
                                # only add razao_col if distinct
                                if razao_col and razao_col not in keep_cols and razao_col != valor_col:
                                    keep_cols.append(razao_col)
                                # Valor (ensure not duplicated)
                                if valor_col not in keep_cols:
                                    keep_cols.append(valor_col)
                                sub = sdf.loc[:, keep_cols].copy()
                                # rename to expected names
                                rename_map = {cnpj_col: 'CNPJ', valor_col: 'ValorDespesas'}
                                if razao_col:
                                    rename_map[razao_col] = 'RazaoSocial'
                                sub = sub.rename(columns=rename_map)
                                print(f"[debug] sheet={sname} sub shape={sub.shape} keep_cols={keep_cols}")
                                dfs.append(sub)
                            except Exception:
                                # fallback: append original sheet
                                dfs.append(sdf)
                    if dfs:
                        print(f"[debug] about to concat {len(dfs)} dfs")
                        for i,dd in enumerate(dfs[:5]):
                            print(f"[debug] dfs[{i}].shape={dd.shape} cols={list(dd.columns)}")
                        df = pd.concat(dfs, ignore_index=True)
                    else:
                        # fallback to first sheet
                        df = pd.read_excel(path)
                except Exception:
                    df = pd.read_excel(path)
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    mapping = map_columns(df)
    print(f"[debug] after concat df.shape={df.shape} df.columns={list(df.columns)[:10]} mapping={mapping}")
    if 'CNPJ' not in mapping or 'ValorDespesas' not in mapping:
        return pd.DataFrame()

    out = pd.DataFrame()
    out['CNPJ'] = df[mapping['CNPJ']].apply(normalize_cnpj)
    out['RazaoSocial'] = df[mapping.get('RazaoSocial', df.columns[0])].astype(str).fillna('')
    out['ValorDespesas'] = df[mapping['ValorDespesas']].apply(parse_valor)
    out['Trimestre'] = trimestre.strip('/').upper()
    out['Ano'] = ano
    # drop rows without CNPJ or Valor
    out = out[out['CNPJ'] != '']
    return out[['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']]


def consolidate(trimestres):
    # Ensure we have files available: download remote zips if needed and also process local raw files
    _ = download_zips(trimestres)
    consolidated_path = OUT_DIR / 'consolidado_despesas.csv'
    inconsist_path = OUT_DIR / 'inconsistencias_report.csv'
    if consolidated_path.exists():
        consolidated_path.unlink()
    inconsist = []
    seen_razoes = {}
    first_write = True

    # Helper to handle a dataframe: record inconsistencies and append
    def handle_df(df, source_name, ano='unknown', trimestre='unknown'):
        nonlocal first_write
        nonlocal seen_razoes
        nonlocal inconsist
        if df.empty:
            return
        for _, row in df.iterrows():
            cnpj = row['CNPJ']
            razao = row['RazaoSocial']
            valor = row['ValorDespesas']
            if cnpj in seen_razoes and seen_razoes[cnpj] != razao:
                inconsist.append({'tipo': 'CNPJ_Razao_Conflito', 'CNPJ': cnpj, 'RazaoAnterior': seen_razoes[cnpj], 'RazaoAtual': razao, 'arquivo': source_name})
            else:
                seen_razoes.setdefault(cnpj, razao)

            if valor is None:
                inconsist.append({'tipo': 'Valor_Invalido', 'CNPJ': cnpj, 'valor': str(row['ValorDespesas']), 'arquivo': source_name})
            elif valor <= 0:
                inconsist.append({'tipo': 'Valor_NaoPositivo', 'CNPJ': cnpj, 'valor': valor, 'arquivo': source_name})

        if first_write:
            df.to_csv(consolidated_path, index=False, mode='w', quoting=csv.QUOTE_MINIMAL)
            first_write = False
        else:
            df.to_csv(consolidated_path, index=False, mode='a', header=False, quoting=csv.QUOTE_MINIMAL)

    # Process files in data/raw: if a ZIP, extract members; else treat as data file
    for f in RAW_DIR.iterdir():
        if not f.is_file():
            continue
        try:
            if zipfile.is_zipfile(f):
                # extract to temp folder and process members
                extracted = safe_extract_zip(f, 'unknown', f.stem)
                for member in extracted.iterdir():
                    if not member.is_file():
                        continue
                    if not detect_target_file(member):
                        continue
                    df = process_file(member, 'unknown', f.stem)
                    handle_df(df, member.name)
            else:
                # direct data file (e.g., old-style xls renamed .zip) or OLE Excel disguised as .zip
                if detect_target_file(f) or f.suffix.lower() in ['.xls', '.xlsx', '.csv', '.txt'] or is_ole_file(f):
                    df = process_file(f, 'unknown', 'unknown')
                    handle_df(df, f.name)
        except Exception:
            continue

    # write inconsistencies
    if inconsist:
        keys = ['tipo', 'CNPJ', 'RazaoAnterior', 'RazaoAtual', 'valor', 'arquivo']
        with open(inconsist_path, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            for row in inconsist:
                writer.writerow({k: row.get(k, '') for k in keys})

    # zip final
    zip_path = OUT_DIR / 'consolidado_despesas.zip'
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        if consolidated_path.exists():
            z.write(consolidated_path, arcname='consolidado_despesas.csv')

    return consolidated_path, zip_path, inconsist_path if inconsist else None


def main():
    trimestres = listar_trimestres()
    consolidated_path, zip_path, inconsist_path = consolidate(trimestres)
    print('Consolidado:', consolidated_path)
    print('Zip:', zip_path)
    if inconsist_path:
        print('Inconsistências registradas em', inconsist_path)


if __name__ == '__main__':
    main()

import os
import duckdb
import pandas as pd
from rapidfuzz import process, fuzz

BASE = os.path.abspath(os.path.dirname(__file__))
CONSOLIDADO = os.path.join(BASE, 'data', 'processed', 'consolidado_despesas.csv')
CADASTRO = os.path.join(BASE, 'data', 'cadastro', 'cadastro.csv')  # optional
OUT_FINAL = os.path.join(BASE, 'data', 'processed', 'consolidado_despesas_final.csv')
OUT_ZIP = os.path.join(BASE, 'data', 'processed', 'consolidado_despesas_final.zip')
INCONS = os.path.join(BASE, 'data', 'processed', 'inconsistencias_report.csv')


def ensure_paths():
    os.makedirs(os.path.join(BASE, 'data', 'processed'), exist_ok=True)
    os.makedirs(os.path.join(BASE, 'data', 'cadastro'), exist_ok=True)


def load_tables(conn):
    if not os.path.exists(CONSOLIDADO):
        raise SystemExit(f"Arquivo consolidado não encontrado em {CONSOLIDADO}. Rode o processor primeiro.")

    conn.execute(f"CREATE TABLE consolidated AS SELECT * FROM read_csv_auto('{CONSOLIDADO}')")

    if os.path.exists(CADASTRO):
        conn.execute(f"CREATE TABLE cadastro_raw AS SELECT * FROM read_csv_auto('{CADASTRO}')")
        # normalize column names in DuckDB if needed
        conn.execute("PRAGMA enable_progress=0")
    else:
        conn.execute("CREATE TABLE cadastro_raw AS SELECT NULL::VARCHAR AS CNPJ, NULL::VARCHAR AS RazaoSocial WHERE FALSE")


def dedup_cadastro(conn):
    # prefer RazaoSocial with max length (heuristic)
    conn.execute(
        "CREATE TABLE cadastro_dedup AS "
        "SELECT CNPJ, max_by(RazaoSocial, length(coalesce(RazaoSocial,''))) as RazaoSocial "
        "FROM cadastro_raw GROUP BY CNPJ"
    )


def left_join(conn):
    conn.execute(
        "CREATE TABLE joined AS SELECT a.*, b.RazaoSocial AS CadastroRazao "
        "FROM consolidated a LEFT JOIN cadastro_dedup b ON a.CNPJ = b.CNPJ"
    )


def export_joined(conn):
    # export to pandas for fuzzy matching step
    df = conn.execute("SELECT * FROM joined").fetchdf()
    return df


def fuzzy_match(df):
    # if cadastro_dedup exists, load its razoes
    cadastro_path = CADASTRO
    if not os.path.exists(cadastro_path):
        df['MatchStatus'] = df['CadastroRazao'].apply(lambda x: 'SemCadastro' if pd.isna(x) else 'Matched')
        df['MatchCandidate'] = None
        df['MatchScore'] = None
        return df

    cad = pd.read_csv(cadastro_path, dtype=str, encoding='utf-8', errors='replace')
    cad['RazaoSocial'] = cad.get('RazaoSocial', cad.columns[1] if len(cad.columns) > 1 else '').astype(str)
    choices = cad['RazaoSocial'].dropna().unique().tolist()

    statuses = []
    candidates = []
    scores = []
    for _, row in df.iterrows():
        if pd.notna(row.get('CadastroRazao')) and str(row.get('CadastroRazao')).strip() != '':
            statuses.append('Matched')
            candidates.append(None)
            scores.append(None)
            continue

        rs = row.get('RazaoSocial')
        if pd.isna(rs) or str(rs).strip() == '':
            statuses.append('SemCadastro')
            candidates.append(None)
            scores.append(None)
            continue

        cand = process.extractOne(str(rs), choices, scorer=fuzz.token_sort_ratio)
        if cand:
            match_val, score, idx = cand[0], cand[1], cand[2] if len(cand) > 2 else cand[1]
            candidates.append(match_val)
            scores.append(score)
            if score >= 95:
                statuses.append('AutoMatched')
            else:
                statuses.append('SemCadastro')
        else:
            candidates.append(None)
            scores.append(None)
            statuses.append('SemCadastro')

    df['MatchStatus'] = statuses
    df['MatchCandidate'] = candidates
    df['MatchScore'] = scores
    return df


def save_and_zip(df):
    df.to_csv(OUT_FINAL, index=False, encoding='utf-8')
    # create zip
    import zipfile
    with zipfile.ZipFile(OUT_ZIP, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        z.write(OUT_FINAL, arcname=os.path.basename(OUT_FINAL))


def main():
    ensure_paths()
    conn = duckdb.connect(database=':memory:')
    load_tables(conn)
    dedup_cadastro(conn)
    left_join(conn)
    df = export_joined(conn)
    df2 = fuzzy_match(df)
    save_and_zip(df2)
    print('Final consolidated saved to', OUT_FINAL)
    print('Zipped to', OUT_ZIP)


if __name__ == '__main__':
    main()

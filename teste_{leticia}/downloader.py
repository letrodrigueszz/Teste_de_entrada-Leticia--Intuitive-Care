import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
DEST_DIR = Path("data/raw")

def listar_trimestres():
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    pastas = [a["href"] for a in soup.find_all("a") if a["href"].endswith("/")]

    trimestres = []
    for ano in pastas:
        if ano.isdigit():
            ano_url = urljoin(BASE_URL, ano)
            ano_resp = requests.get(ano_url)
            soup_ano = BeautifulSoup(ano_resp.text, "html.parser")
            for tri in soup_ano.find_all("a"):
                href = tri.get("href")
                if href and href.startswith("Q"):
                    trimestres.append((ano, href))

    return sorted(trimestres, reverse=True)[:3]

def baixar_arquivos():
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    trimestres = listar_trimestres()

    for ano, trimestre in trimestres:
        url_trimestre = f"{BASE_URL}{ano}{trimestre}"
        resp = requests.get(url_trimestre)
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a"):
            arquivo = link.get("href")
            if arquivo and not arquivo.endswith("/"):
                file_url = urljoin(url_trimestre, arquivo)
                destino = DEST_DIR / f"{ano}_{trimestre}_{arquivo}"

                with requests.get(file_url, stream=True) as r:
                    r.raise_for_status()
                    with open(destino, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
import pandas as pd
from validador import (
    validar_cnpj,
    validar_valor_positivo,
    validar_razao_social
)

CSV_PATH = "data/processed/consolidado_despesas.csv"

def processar_csv():
    df = pd.read_csv(CSV_PATH)

    erros = []

    for idx, row in df.iterrows():
        if not validar_cnpj(row.get("CNPJ", "")):
            erros.append((idx, "CNPJ inválido"))
        elif not validar_valor_positivo(row.get("ValorDespesas", 0)):
            erros.append((idx, "Valor não positivo"))
        elif not validar_razao_social(row.get("RazaoSocial", "")):
            erros.append((idx, "Razão Social vazia"))

    df_erros = pd.DataFrame(erros, columns=["linha", "erro"]) if erros else pd.DataFrame(columns=["linha","erro"])
    df_valido = df.drop([r[0] for r in erros]) if erros else df

    df_valido.to_csv("data/processed/validos.csv", index=False)
    df_erros.to_csv("data/processed/erros.csv", index=False)

if __name__ == "__main__":
    processar_csv()

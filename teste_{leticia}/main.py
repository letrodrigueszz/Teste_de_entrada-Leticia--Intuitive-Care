import processor as pd
from validador import (
    validar_cnpj,
    validar_valor_positivo,
    validar_razao_social
)

CSV_PATH = "data/processed/consolidado.csv"

def processar_csv():
    df = pd.read_csv(CSV_PATH)

    erros = []

    for idx, row in df.iterrows():
        if not validar_cnpj(row["CNPJ"]):
            erros.append((idx, "CNPJ inválido"))
        elif not validar_valor_positivo(row["VALOR"]):
            erros.append((idx, "Valor não positivo"))
        elif not validar_razao_social(row["RAZAO_SOCIAL"]):
            erros.append((idx, "Razão Social vazia"))

    df_erros = pd.DataFrame(erros, columns=["linha", "erro"])
    df_valido = df.drop(df_erros["linha"])

    df_valido.to_csv("data/processed/validos.csv", index=False)
    df_erros.to_csv("data/processed/erros.csv", index=False)

if __name__ == "__main__":
    processar_csv()

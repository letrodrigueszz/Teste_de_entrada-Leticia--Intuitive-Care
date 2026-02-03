import re

def validar_cnpj(cnpj: str) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)

    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    def calc_digito(cnpj, pesos):
        soma = sum(int(d) * p for d, p in zip(cnpj, pesos))
        resto = soma % 11
        return '0' if resto < 2 else str(11 - resto)

    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    pesos2 = [6] + pesos1

    dig1 = calc_digito(cnpj[:12], pesos1)
    dig2 = calc_digito(cnpj[:12] + dig1, pesos2)

    return cnpj[-2:] == dig1 + dig2

def validar_valor_positivo(valor):
    try:
        return float(valor) > 0
    except:
        return False

def validar_razao_social(razao):
    return bool(razao and razao.strip())

# Pipeline de Despesas

## Resumo

Pipeline para baixar, extrair, normalizar e consolidar dados de “Despesas com Eventos/Sinistros” (ANS), produzindo um CSV consolidado e um relatório de inconsistências. Suporta arquivos remotos (FTP/ZIP), planilhas (XLS/XLSX), CSV/TXT e arquivos OLE renomeados.

## Requisitos

- Python 3.8+
- Instale dependências:

```bash
pip install -r requirements.txt
```

## Instalação

1. Crie e ative o ambiente virtual:

```bash
python -m venv .venv
# Windows PowerShell
& .venv\Scripts\Activate.ps1
# Windows cmd
.\.venv\Scripts\activate.bat
```

## Execução rápida

Execute o processador principal:

```bash
python processador.py
```

## Estrutura (principais arquivos)

Recomenda-se a seguinte organização mínima no repositório:

- processador.py — fluxo principal (download, extração, normalização, consolidação).
- src/ — código fonte modular (opcional: mover lógicas para aqui).
- data/raw/ — entrada bruta (downloads, zips).
- data/processed/ — saídas geradas.
- tests/ — testes automatizados (pytest).
- utilitários e scripts:
  - inspecionar_bruto_debug.py — inspeção rápida de ZIPs brutos.
  - inspecionar_um.py, inspecionar_zip_pt.py — inspeção auxiliar.
  - juntar_e_verificar.py — junção com cadastro e correspondência fuzzy (opcional).
  - escanear_planilhas.py, ver_aba10.py, buscar_bytes.py, imprimir_magic.py — utilitários diagnósticos.
  - teste_leticia/ — scripts de teste/validação (mover para tests/ se apropriado).

## Saídas geradas

- data/processed/consolidado_despesas.csv — CSV final com colunas: CNPJ, RazaoSocial, Trimestre, Ano, ValorDespesas.
- data/processed/consolidado_despesas.zip — ZIP contendo o CSV consolidado.
- data/processed/inconsistencias_report.csv — relatório de inconsistências detectadas.

## Critérios de avaliação

- Funcionalidade: produz o CSV consolidado e o relatório de inconsistências.
- Robustez: tolerância a formatos variados e detecção de arquivos OLE renomeados.
- Documentação: README claro com instruções de execução.
- Qualidade do código: legibilidade, organização e tratamento de erros.
- Pensamento crítico: tratamento de casos extremos e validações.
- Praticidade: soluções simples e eficientes (KISS).

## Como validar rapidamente

1. Rode `python processador.py`.
2. Confirme que `data/processed/consolidado_despesas.csv` existe e contém linhas relevantes.
3. Verifique que `data/processed/consolidado_despesas.zip` contém o CSV.
4. Abra `data/processed/inconsistencias_report.csv` e revise problemas detectados.
5. Execute a suíte de testes (se presente):

```bash
pytest -q
```

## Checagens sugeridas (implementação recomendada)

- Suporte a encodings (latin1, utf-8) com autodetecção.
- Normalização de valores monetários (`1.234,56`, `1234.56`) para float.
- Detecção de arquivos OLE renomeados (verificar bytes iniciais / magic numbers).
- Validações de schema e tipos antes da consolidação.
- Logs claros e relatório de erros; falhas não devem interromper o processamento de outros arquivos.

## Recomendações para organização de código (curto prazo)

- Mover funcionalidades para `src/` com módulos: download.py, extractor.py, normalizer.py, consolidator.py.
- Criar `tests/test_basic_flow.py` com casos: leitura CSV, leitura XLSX, parse de valores monetários, detecção OLE.
- Adicionar logging (nivel INFO/ERROR) e um arquivo de configuração de logging.

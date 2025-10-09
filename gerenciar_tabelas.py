# date_checker_from_json.py

import json  # Adicionado para ler o arquivo de configuração
import mariadb
import pandas as pd
import sys
import warnings

# --- 1. CARREGAR CONFIGURAÇÕES DO ARQUIVO JSON ---
# Este era o bloco que estava faltando. Ele lê o config.json e cria a variável db_config.
try:
    with open('config.json', 'r') as f:
        DB_CONFIG = json.load(f)
    print("INFO: Arquivo de configuração 'config.json' carregado com sucesso.")
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo 'config.json' não encontrado. Crie o arquivo com suas credenciais.")
    sys.exit(1)
except json.JSONDecodeError:
    print("ERRO CRÍTICO: O arquivo 'config.json' está mal formatado e não pôde ser lido.")
    sys.exit(1)

# --- 2. LÓGICA DE CHECAGEM DE DATAS ---

def checar_atualidade_chamados():
    """
    Conecta ao banco MariaDB e compara APENAS A DATA da última modificação
    entre as tabelas bronze e silver.
    """
    status_geral = "SUCESSO"
    mensagem = ""
    conn = None

    try:
        print("INFO: Conectando ao banco de dados MariaDB...")
        conn = mariadb.connect(**DB_CONFIG)
        print("INFO: Conexão bem-sucedida.")
        print("-" * 50)
        print("INFO: Executando Checagem de Atualidade...")

        query_bronze_date = "SELECT MAX(ultimaAlteracao) FROM bronze_sults_chamados"
        query_silver_date = "SELECT MAX(data_referencia) FROM silver_sults_chamados"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            
            max_bronze_date = pd.to_datetime(pd.read_sql(query_bronze_date, conn).iloc[0, 0])
            max_silver_date = pd.to_datetime(pd.read_sql(query_silver_date, conn).iloc[0, 0])

        if pd.isna(max_bronze_date) or pd.isna(max_silver_date):
            mensagem = "AVISO: Uma das tabelas está vazia. Não foi possível comparar as datas."
            status_geral = "AVISO"
        # --- A LÓGICA DE COMPARAÇÃO FOI AJUSTADA AQUI ---
        # Usamos .date() em ambos os lados para comparar apenas o dia, ignorando a hora.
        elif max_silver_date.date() >= max_bronze_date.date():
            mensagem = f"SUCESSO: Silver está atualizada. (Bronze: {max_bronze_date.date()}, Silver: {max_silver_date.date()})"
            status_geral = "SUCESSO"
        else:
            mensagem = f"FALHA: Silver está DESATUALIZADA. (Bronze: {max_bronze_date.date()}, Silver: {max_silver_date.date()})"
            status_geral = "FALHA"

    except mariadb.Error as ex:
        mensagem = f"ERRO DE BANCO DE DADOS: (Erro nº {ex.errno}) {ex.errmsg}"
        status_geral = "ERRO"
    except Exception as e:
        mensagem = f"ERRO INESPERADO: {e}"
        status_geral = "ERRO"
    finally:
        if conn:
            conn.close()
            print("INFO: Checagem concluída. Conexão fechada.")

    # --- 3. EXIBIR RESULTADO FINAL ---
    print("\n" + "="*50)
    print("--- RELATÓRIO DE ATUALIDADE 'CHAMADOS' ---")
    print("="*50)
    print(f"- {mensagem}")
    print("-" * 50)
    print(f"STATUS GERAL: {status_geral}")
    print("="*50)

if __name__ == "__main__":
    checar_atualidade_chamados()
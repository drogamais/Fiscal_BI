# check_closeup.py (Lógica de Sincronia)

import pandas as pd
import warnings

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_closeup_sync():
    """
    Verifica se a data máxima de inserção das tabelas silver e gold de
    Close-Up são iguais, indicando que estão sincronizadas.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE SINCRONIA 'CLOSE-UP' ---")
    print("="*50)

    conn = get_db_connection()
    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    # --- Nomes das tabelas e colunas ---
    # Usando o nome corrigido da tabela gold que descobrimos anteriormente
    silver_table = {'nome': 'silver_close_up', 'tipo': 'TABELA SILVER', 'coluna': 'data_insercao'}
    gold_table = {'nome': 'gold_closeUp_estoque_vendas_analise_de_mercado', 'tipo': 'TABELA GOLD', 'coluna': 'data_insercao'}
    
    all_logs = []
    status_geral = "Não Definido"
    date_silver = None
    date_gold = None

    try:
        # Pega a data máxima da tabela Silver
        print(f"Buscando data da tabela: '{silver_table['nome']}'...")
        query_silver = f"SELECT MAX(`{silver_table['coluna']}`) FROM `{silver_table['nome']}`"
        df_silver = pd.read_sql(query_silver, conn)
        date_silver = pd.to_datetime(df_silver.iloc[0, 0])

        # Pega a data máxima da tabela Gold
        print(f"Buscando data da tabela: '{gold_table['nome']}'...")
        query_gold = f"SELECT MAX(`{gold_table['coluna']}`) FROM `{gold_table['nome']}`"
        df_gold = pd.read_sql(query_gold, conn)
        date_gold = pd.to_datetime(df_gold.iloc[0, 0])
        print("-" * 20)

        # --- LÓGICA DE COMPARAÇÃO ---
        if pd.isna(date_silver) or pd.isna(date_gold):
            status_geral = "Sem Histórico"
            print("AVISO: Uma das tabelas está vazia. Não foi possível comparar.")
        elif date_silver.date() == date_gold.date():
            status_geral = "Sincronizado"
            print(f"SUCESSO: As tabelas estão sincronizadas. (Data: {date_silver.date()})")
        else:
            status_geral = "Dessincronizado"
            print(f"FALHA: As tabelas estão DESSINCRONIZADAS! (Silver: {date_silver.date()}, Gold: {date_gold.date()})")

    except Exception as e:
        status_geral = 'Erro na Verificação'
        print(f"ERRO INESPERADO durante a checagem: {e}")
    
    finally:
        # --- PREPARAÇÃO DOS LOGS PARA INSERÇÃO ---
        # Gera um log para cada tabela com o status geral da sincronia
        log_silver = {
            'nome_workspace': 'dbDrogamais', 'nome_ativo': silver_table['nome'],
            'tipo_ativo': silver_table['tipo'], 'status_atualizacao': status_geral,
            'data_atualizacao': date_silver, 'tipo_atualizacao': 'Sync Check'
        }
        log_gold = {
            'nome_workspace': 'dbDrogamais', 'nome_ativo': gold_table['nome'],
            'tipo_ativo': gold_table['tipo'], 'status_atualizacao': status_geral,
            'data_atualizacao': date_gold, 'tipo_atualizacao': 'Sync Check'
        }
        all_logs.extend([log_silver, log_gold])

        df_para_inserir = pd.DataFrame(all_logs)

        # Formata a data e trata valores nulos
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})

        print("\n" + "="*50)
        print("--- DADOS A SEREM INSERIDOS NO LOG ---")
        print(df_para_inserir.to_string())
        print("="*50 + "\n")

        insert_dataframe(conn, df_para_inserir, "fat_fiscal")

        if conn:
            conn.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexão com o banco fechada.")
            print("="*50)

if __name__ == "__main__":
    check_closeup_sync()
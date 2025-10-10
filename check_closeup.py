# check_closeup.py (Lógica de Sincronia Direta)

import pandas as pd
import warnings
from datetime import date

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_closeup_sync():
    """
    Verifica se as tabelas Silver e Gold do Close-Up estão sincronizadas
    com a tabela Bronze, fazendo a consulta diretamente no banco.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE SINCRONIA 'CLOSE-UP' (DIRETA) ---")
    print("="*50)

    conn = get_db_connection()
    if conn is None:
        return

    # --- Configuração das tabelas Close-Up ---
    bronze_table = {'nome': 'bronze_close_up', 'coluna': 'data_insercao'}
    silver_table = {'nome': 'silver_close_up', 'tipo': 'TABELA SILVER', 'coluna': 'data_insercao'}
    # Usando o nome corrigido para evitar o erro "Table doesn't exist"
    gold_table = {'nome': 'gold_closeUp_estoque_vendas_analise_de_mercado', 'tipo': 'TABELA GOLD', 'coluna': 'data_insercao'}
    
    all_logs = []
    status_geral = "Não Definido"
    date_bronze, date_silver, date_gold = None, None, None
    dias_silver, dias_gold = None, None

    try:
        # Pega a data de todas as três tabelas diretamente
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            
            query_bronze = f"SELECT MAX(`{bronze_table['coluna']}`) FROM `{bronze_table['nome']}`"
            date_bronze = pd.to_datetime(pd.read_sql(query_bronze, conn).iloc[0, 0])

            query_silver = f"SELECT MAX(`{silver_table['coluna']}`) FROM `{silver_table['nome']}`"
            date_silver = pd.to_datetime(pd.read_sql(query_silver, conn).iloc[0, 0])

            query_gold = f"SELECT MAX(`{gold_table['coluna']}`) FROM `{gold_table['nome']}`"
            date_gold = pd.to_datetime(pd.read_sql(query_gold, conn).iloc[0, 0])
        
        # --- Lógica de Comparação e Cálculo ---
        hoje = date.today()
        if not pd.isna(date_silver): dias_silver = (hoje - date_silver.date()).days
        if not pd.isna(date_gold): dias_gold = (hoje - date_gold.date()).days

        print(f"   Data de Referência (Bronze): {date_bronze.date() if not pd.isna(date_bronze) else 'N/A'}")
        print(f"   Data Atual (Silver):       {date_silver.date() if not pd.isna(date_silver) else 'N/A'} ({dias_silver} dias atrás)")
        print(f"   Data Atual (Gold):         {date_gold.date() if not pd.isna(date_gold) else 'N/A'} ({dias_gold} dias atrás)")
        print("-" * 20)

        if pd.isna(date_bronze) or pd.isna(date_silver) or pd.isna(date_gold):
            status_geral = "Sem Histórico"
        elif date_silver.date() >= date_bronze.date() and date_gold.date() >= date_bronze.date():
            status_geral = "Sincronizado"
        else:
            status_geral = "Dessincronizado"

    except Exception as e:
        status_geral = 'Erro na Verificação'
        print(f"   ERRO INESPERADO: {e}")
    
    finally:
        # Gera logs para Silver e Gold com o status da sincronia
        all_logs.append({
            'nome_workspace': 'dbDrogamais', 'nome_ativo': silver_table['nome'],
            'tipo_ativo': silver_table['tipo'], 'status_atualizacao': status_geral,
            'data_atualizacao': date_silver, 'tipo_atualizacao': 'Sync Check',
            'dias_sem_atualizar': dias_silver
        })
        all_logs.append({
            'nome_workspace': 'dbDrogamais', 'nome_ativo': gold_table['nome'],
            'tipo_ativo': gold_table['tipo'], 'status_atualizacao': status_geral,
            'data_atualizacao': date_gold, 'tipo_atualizacao': 'Sync Check',
            'dias_sem_atualizar': dias_gold
        })

        df_para_inserir = pd.DataFrame(all_logs)
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})
        
        print("\n" + df_para_inserir.to_string())
        insert_dataframe(conn, df_para_inserir, "fat_fiscal")

        if conn: conn.close()
        print("\n--- PROCESSO CLOSE-UP FINALIZADO ---\n")

if __name__ == "__main__":
    check_closeup_sync()
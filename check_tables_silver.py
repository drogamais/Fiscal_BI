# check_tables_silver.py (Lógica de Sincronia Bronze vs Silver para dbSults)

import pandas as pd
import warnings
from datetime import date

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_sync_status(conn_data, bronze_table_name, silver_table_name, date_column):
    """
    Verifica se a tabela Silver está sincronizada (data >= Bronze).
    Retorna o log para a tabela Silver.
    """
    status_geral = "Não Definido"
    date_bronze, date_silver = None, None
    dias_silver = None
    
    print(f"---> Verificando sincronia: {silver_table_name} (Silver) vs {bronze_table_name} (Bronze)...")

    try:
        # Para evitar UserWarning do pandas ao ler a data
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
        
            # Pega a data da tabela Bronze (Referência)
            query_bronze = f"SELECT MAX(`{date_column}`) FROM `{bronze_table_name}`"
            date_bronze = pd.to_datetime(pd.read_sql(query_bronze, conn_data).iloc[0, 0])

            # Pega a data da tabela Silver (Atual)
            query_silver = f"SELECT MAX(`{date_column}`) FROM `{silver_table_name}`"
            date_silver = pd.to_datetime(pd.read_sql(query_silver, conn_data).iloc[0, 0])
        
        # Lógica de Comparação e Cálculo
        hoje = date.today()
        
        if not pd.isna(date_silver): 
            dias_silver = (hoje - date_silver.date()).days

        if pd.isna(date_bronze) or pd.isna(date_silver):
            status_geral = "Sem Histórico"
        elif date_silver.date() >= date_bronze.date():
            status_geral = "Sincronizado"
        else:
            status_geral = "Dessincronizado"

        print(f"   Data de Referência (Bronze): {date_bronze.date() if not pd.isna(date_bronze) else 'N/A'}")
        print(f"   Data Atual (Silver):       {date_silver.date() if not pd.isna(date_silver) else 'N/A'} ({dias_silver} dias atrás)")

    except Exception as e:
        status_geral = 'Erro na Verificação'
        print(f"   ERRO INESPERADO ao checar '{silver_table_name}': {e}")
    
    # Gera log para a tabela Silver
    log_entry = {
        'nome_workspace': 'dbSults',
        'nome_ativo': silver_table_name,
        'tipo_ativo': 'TABELA SILVER',
        'status_atualizacao': status_geral,
        'data_atualizacao': date_silver,
        'tipo_atualizacao': 'Sync Check',
        'dias_sem_atualizar': dias_silver
    }
    return log_entry


def main():
    """
    Função principal que verifica a sincronia entre Bronze e Silver para o dbSults.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE SINCRONIA 'DB SULTS' (BRONZE/SILVER) ---")
    print("="*50)

    # Pares de tabelas a serem checadas (fixo, como nos scripts de sync existentes)
    tabelas_para_checar = [
        {'bronze': 'bronze_movidesk_chamados', 'silver': 'silver_movidesk_chamados', 'coluna': 'data_atualizacao'},
        {'bronze': 'bronze_sults_chamados', 'silver': 'silver_sults_chamados', 'coluna': 'data_atualizacao'},
        {'bronze': 'bronze_sults_implantacao', 'silver': 'silver_sults_implantacao', 'coluna': 'data_atualizacao'},
    ]
    
    all_logs = []
    conn_log = None
    
    # Conexão com o banco de DADOS (dbSults) para executar os SELECTs
    conn_data = get_db_connection(config_key='databaseSults')

    if conn_data is None:
        return

    try:
        for par in tabelas_para_checar:
            log = check_sync_status(conn_data, par['bronze'], par['silver'], par['coluna'])
            all_logs.append(log)
            print("-" * 20)

    except Exception as e:
        print(f"   ERRO GERAL INESPERADO: {e}")
    
    finally:
        if conn_data: conn_data.close()
        
        if not all_logs:
            print("AVISO: Nenhum log foi gerado.")
            print("\n--- PROCESSO DB SULTS (B/S) FINALIZADO ---\n")
            return

        df_para_inserir = pd.DataFrame(all_logs)
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})

        print("\n" + "="*50)
        print("--- DADOS A SEREM INSERIDOS NO LOG ---")
        print(df_para_inserir.to_string())
        print("="*50 + "\n")
        
        # Conexão para o banco de logs (dbDrogamais) para o INSERT
        conn_log = get_db_connection(config_key='database')
        
        if conn_log is None:
             print("ERRO CRÍTICO: Não foi possível conectar ao banco de LOGS (dbDrogamais). Logs não inseridos.")
             return

        insert_dataframe(conn_log, df_para_inserir, "fat_fiscal")

        if conn_log: conn_log.close()
        print("\n--- PROCESSO DB SULTS (B/S) FINALIZADO ---\n")

if __name__ == "__main__":
    main()
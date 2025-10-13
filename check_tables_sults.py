# check_tables_sults.py (com cálculo de dias sem atualizar para dbSults)

import pandas as pd
from datetime import date
import warnings
import json
import sys

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_table_status(conn, table_name, asset_type, date_column):
    """
    Verifica a data da última inserção, calcula os dias sem atualização
    e retorna um dicionário com os dados para o log.
    """
    print(f"---> Verificando a tabela: '{table_name}' (usando coluna '{date_column}')...")
    status = "Não Definido"
    data_ref = None
    dias_sem_atualizar = None

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            dias_sem_atualizar = None
            print(f"AVISO: Não há registros em '{table_name}'. Status: Sem Histórico.")
        else:
            # --- LÓGICA DE CÁLCULO DE DIAS ---
            hoje = date.today()
            dias_sem_atualizar = (hoje - max_date_from_db.date()).days
            
            if dias_sem_atualizar == 0:
                status = 'Atualizada'
                print(f"SUCESSO: Tabela atualizada. (Dias sem atualizar: {dias_sem_atualizar})")
            else:
                status = 'Failed'
                print(f"FALHA: Tabela DESATUALIZADA. (Dias sem atualizar: {dias_sem_atualizar})")

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    # Adiciona a nova métrica ao log
    log_entry = {
        'nome_workspace': 'dbSults', # <-- MUDANÇA AQUI
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled',
        'dias_sem_atualizar': dias_sem_atualizar 
    }
    return log_entry


def main():
    """
    Função principal que orquestra a verificação de todas as tabelas do dbSults
    e insere os logs no banco de dados fat_fiscal.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE ATUALIDADE DAS TABELAS (DB SULTS) ---")
    print("="*50)

    # Função local para carregar a configuração
    def load_table_config(config_file='config_tables.json'):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar config: {e}")
            return None

    config = load_table_config()
    if not config:
        sys.exit(1)

    # Pega as tabelas configuradas na nova seção 'sults_checks'
    tabelas_para_checar = config.get('sults_checks', [])
    
    if not tabelas_para_checar:
        print("ERRO: Nenhuma lista de tabelas válida foi encontrada para 'sults_checks' no arquivo de configuração.")
        sys.exit(1)

    all_logs = []
    conn_log = None # Inicializa a conexão de log para uso no finally
    
    # Conexão com o banco de DADOS (dbSults) para executar os SELECTs
    conn_data = get_db_connection(config_key='databaseSults')

    if conn_data is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco 'databaseSults'. O script será encerrado.")
        return

    try:
        for tabela in tabelas_para_checar:
            if not tabela.get('enabled', True):
                print(f"---> Pulando a tabela desativada: '{tabela['nome']}'")
                continue
            
            # Executa a verificação usando a conexão do dbSults
            log = check_table_status(conn_data, tabela['nome'], tabela['tipo'], tabela['coluna'])
            all_logs.append(log)
            print("-" * 20)
        
        # Fecha a conexão de dados (dbSults) antes de abrir a de log
        conn_data.close()
        
        if not all_logs:
            print("AVISO: Nenhum log foi gerado.")
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

    finally:
        if conn_log:
            conn_log.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexão com o banco de LOGS fechada.")
            print("="*50)

if __name__ == "__main__":
    main()
# check_tables_timestamp.py (com cálculo de dias sem atualizar)

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
    dias_sem_atualizar = None # <-- Nova variável

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            dias_sem_atualizar = None # Ou um valor alto como 999 se preferir
            print(f"AVISO: Não há registros em '{table_name}'. Status: Sem Histórico.")
        else:
            # --- NOVA LÓGICA DE CÁLCULO DE DIAS ---
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
        'nome_workspace': 'dbDrogamais',
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled',
        'dias_sem_atualizar': dias_sem_atualizar # <-- Nova chave no dicionário
    }
    return log_entry


def main():
    """
    Função principal que orquestra a verificação de todas as tabelas
    e insere os logs no banco de dados.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE ATUALIDADE DAS TABELAS ---")
    print("="*50)

    # Carrega a configuração do arquivo JSON
    config = load_table_config() # Supondo que load_table_config exista
    if not config:
        sys.exit(1)

    tabelas_para_checar = []
    for grupo in config.values():
        tabelas_para_checar.extend(grupo)

    if not tabelas_para_checar:
        print("ERRO: Nenhuma tabela encontrada no arquivo de configuração.")
        sys.exit(1)

    all_logs = []
    conn = get_db_connection()

    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    try:
        for tabela in tabelas_para_checar:
            if not tabela.get('enabled', True):
                print(f"---> Pulando a tabela desativada: '{tabela['nome']}'")
                continue
            
            log = check_table_status(conn, tabela['nome'], tabela['tipo'], tabela['coluna'])
            all_logs.append(log)
            print("-" * 20)
        
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

        insert_dataframe(conn, df_para_inserir, "fat_fiscal")

    finally:
        if conn:
            conn.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexão com o banco fechada.")
            print("="*50)

# Supondo que você tenha essa função para carregar o JSON
def load_table_config(config_file='config_tables.json'):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar config: {e}")
        return None

if __name__ == "__main__":
    main()
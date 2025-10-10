# master_checker.py

import pandas as pd
from datetime import date
import warnings
import json
import sys

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_table_status(conn, table_name, asset_type, date_column):
    """
    Verifica a data da última inserção em uma tabela específica, usando uma
    coluna de data customizada, e retorna um dicionário com os dados para o log.
    """
    print(f"---> Verificando a tabela: '{table_name}' (usando coluna '{date_column}')...")
    status = "Não Definido"
    data_ref = None

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            print(f"AVISO: Não há registros em '{table_name}'. Status: Sem Histórico.")
        else:
            if max_date_from_db.date() == date.today():
                status = 'Atualizada'
                print(f"SUCESSO: Tabela atualizada. (Última data: {max_date_from_db.date()})")
            else:
                status = 'Failed'
                print(f"FALHA: Tabela DESATUALIZADA. (Última data: {max_date_from_db.date()}, Hoje: {date.today()})")

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    log_entry = {
        'nome_workspace': 'dbDrogamais',
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled'
    }
    return log_entry


def load_table_config(config_file='config_tables.json'):
    """Carrega a configuração das tabelas a partir de um arquivo JSON."""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO CRÍTICO: Arquivo de configuração de tabelas '{config_file}' não encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"ERRO CRÍTICO: O arquivo '{config_file}' está mal formatado.")
        return None

def main():
    """
    Função principal que orquestra a verificação de todas as tabelas
    e insere os logs no banco de dados.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE ATUALIDADE DAS TABELAS ---")
    print("="*50)

    # Carrega a configuração do arquivo JSON
    config = load_table_config()
    if not config:
        sys.exit(1)

    # Determina qual grupo de tabelas checar a partir dos argumentos da linha de comando
    args = sys.argv[1:]
    tabelas_para_checar = []

    if not args or 'all' in args:
        # Se nenhum argumento for passado ou 'all', checa todos os grupos
        print("INFO: Verificando todos os grupos de tabelas.")
        for grupo in config.values():
            tabelas_para_checar.extend(grupo)
    else:
        # Checa apenas os grupos especificados
        print(f"INFO: Verificando os grupos: {', '.join(args)}")
        for arg in args:
            if arg in config:
                tabelas_para_checar.extend(config[arg])
            else:
                print(f"AVISO: Grupo '{arg}' não encontrado no arquivo de configuração.")

    if not tabelas_para_checar:
        print("ERRO: Nenhum grupo de tabelas válido foi selecionado para verificação.")
        sys.exit(1)

    all_logs = []
    conn = get_db_connection()

    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    try:
        for tabela in tabelas_para_checar:
            log = check_table_status(conn, tabela['nome'], tabela['tipo'], tabela['coluna'])
            all_logs.append(log)
            print("-" * 20)
        
        if not all_logs:
            print("AVISO: Nenhum log foi gerado. Nada para inserir.")
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

if __name__ == "__main__":
    main()
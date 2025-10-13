# database.py

import json
import mariadb
import sys
import pandas as pd

def get_db_connection(config_key='databaseDrogamais', read_timeout=300, write_timeout=300):
    """
    Lê o config.json e estabelece uma conexão com o banco de dados MariaDB
    usando a chave de configuração especificada ('databaseDrogamais' ou 'databaseSults').
    """
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            db_config = config[config_key] # Usa o parâmetro config_key
        
        # Adiciona os timeouts à configuração
        db_config['read_timeout'] = read_timeout
        db_config['write_timeout'] = write_timeout
        
        print(f"INFO: Conectando ao banco de dados MariaDB (Chave: '{config_key}')...")
        conn = mariadb.connect(**db_config)
        print("INFO: Conexão bem-sucedida.")
        return conn

    except FileNotFoundError:
        print("ERRO CRÍTICO: Arquivo 'config.json' não encontrado.")
        return None
    except KeyError:
        print(f"ERRO CRÍTICO: A chave '{config_key}' não foi encontrada no 'config.json'.")
        return None
    except mariadb.Error as ex:
        print(f"ERRO CRÍTICO: Falha ao conectar ao MariaDB. (Erro nº {ex.errno}) {ex.errmsg}")
        return None
    except Exception as e:
        print(f"ERRO CRÍTICO: Ocorreu um erro inesperado ao conectar. Detalhe: {e}")
        return None

def insert_dataframe(conn, df, table_name):
    """
    Insere um DataFrame do Pandas em uma tabela do MariaDB de forma eficiente.
    """
    if df.empty:
        print(f"AVISO: O DataFrame está vazio. Nenhuma inserção foi realizada na tabela '{table_name}'.")
        return True

    print(f"INFO: Iniciando a inserção de {len(df)} linhas na tabela '{table_name}'...")
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        data_tuples = list(df.itertuples(index=False, name=None))
        
        cursor.executemany(query, data_tuples)
        conn.commit()
        
        print(f"INFO: Inserção de {cursor.rowcount} linhas concluída com sucesso na tabela '{table_name}'.")
        return True

    except mariadb.Error as ex:
        print(f"ERRO: Falha ao inserir dados na tabela '{table_name}'. (Erro nº {ex.errno}) {ex.errmsg}")
        print("INFO: Revertendo a transação (rollback)...")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
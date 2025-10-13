# database.py

import json
import mariadb
import sys
import pandas as pd

def get_db_connection(config_key='databaseDrogamais'): 
    """
    Lê o config.json e estabelece uma conexão com o banco de dados MariaDB
    usando a chave de configuração especificada ('databaseDrogamais' ou 'databaseSults').
    Os timeouts (read_timeout e write_timeout) são lidos diretamente do arquivo de configuração,
    com valor padrão de 300s se não definidos.
    """
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            db_config = config[config_key]
        
        # O .get() garante que o código não quebre se as chaves estiverem faltando no config.json
        db_config['read_timeout'] = db_config.get('read_timeout', 300)
        db_config['write_timeout'] = db_config.get('write_timeout', 300)
        
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
    Adicionado log de debug para capturar a query e os dados em caso de falha.
    """
    if df.empty:
        print(f"AVISO: O DataFrame está vazio. Nenhuma inserção foi realizada na tabela '{table_name}'.")
        return True

    print(f"INFO: Iniciando a inserção de {len(df)} linhas na tabela '{table_name}'...")
    
    cursor = None
    query = None # Inicializa query para ser acessível no bloco except
    try:
        cursor = conn.cursor()
        
        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        data_tuples = list(df.itertuples(index=False, name=None))
        
        # --- DEBUG LOGGING PRÉ-EXECUÇÃO ---
        print(f"DEBUG: Query de Inserção: {query}")
        print(f"DEBUG: Número de tuplas a inserir: {len(data_tuples)}")
        
        cursor.executemany(query, data_tuples)
        conn.commit()
        
        print(f"INFO: Inserção de {cursor.rowcount} linhas concluída com sucesso na tabela '{table_name}'.")
        return True

    except mariadb.Error as ex:
        print(f"ERRO: Falha ao inserir dados na tabela '{table_name}'. (Erro nº {ex.errno}) {ex.errmsg}")
        print(f"INFO: Revertendo a transação (rollback)...")
        
        # --- DEBUG LOGGING DE ERRO ---
        print("\n" + "="*50)
        print("--- DEBUG: DADOS E QUERY QUE CAUSARAM O ERRO ---")
        print(f"DEBUG: Query que falhou: {query}")
        
        # Loga as primeiras e últimas linhas do DataFrame para inspeção
        print("DEBUG: Primeiras 5 linhas do DataFrame (df.head()):")
        print(df.head().to_string())
        print("\nDEBUG: Últimas 5 linhas do DataFrame (df.tail()):")
        print(df.tail().to_string())
        print("="*50 + "\n")

        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
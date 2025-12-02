import json
import mariadb
import sys
import pandas as pd
import logging
from pathlib import Path

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(config_key='dbDrogamais'):
    """
    Lê o config.json, processa a herança ($extends) e conecta ao MariaDB.
    """
    # Define o caminho do arquivo config.json
    src_dir = Path(__file__).resolve().parent
    config_path = src_dir.parent / 'config' / 'config.json'

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        
        # Verifica se a chave existe
        if config_key not in full_config:
            logging.error(f"ERRO CRÍTICO: A chave '{config_key}' não foi encontrada no 'config.json'.")
            return None

        db_config = full_config[config_key]

        # --- CORREÇÃO: Processar Herança ($extends) ---
        if "$extends" in db_config:
            parent_key = db_config.pop("$extends") # Remove a chave $extends e pega o nome do pai
            parent_config = full_config.get(parent_key, {})
            
            # Faz o merge: Configurações do pai + Configurações específicas (sobrescrevem o pai)
            final_config = parent_config.copy()
            final_config.update(db_config)
            db_config = final_config
        # ----------------------------------------------

        # Define timeouts com valores padrão se não existirem
        db_config.setdefault('read_timeout', 300)
        db_config.setdefault('write_timeout', 300)

        # Remove chaves que não são aceitas pelo mariadb.connect (caso existam extras)
        # O connect aceita: user, password, host, port, database, etc.
        # chaves como 'read_timeout' funcionam em alguns drivers, mas garantimos limpeza se necessário.
        
        logging.info(f"INFO: Conectando ao banco de dados MariaDB (Chave: '{config_key}')...")
        conn = mariadb.connect(**db_config)
        return conn

    except FileNotFoundError:
        logging.error("ERRO CRÍTICO: Arquivo 'config.json' não encontrado.")
        return None
    except mariadb.Error as ex:
        logging.error(f"ERRO CRÍTICO: Falha ao conectar ao MariaDB. (Erro nº {ex.errno}) {ex.errmsg}")
        return None
    except Exception as e:
        logging.error(f"ERRO CRÍTICO: Ocorreu um erro inesperado ao conectar. Detalhe: {e}")
        return None


def insert_dataframe(conn, df, table_name):
    """
    Insere um DataFrame do Pandas em uma tabela do MariaDB.
    Tenta inserir em lote. Se falhar, tenta linha a linha.
    """
    if df.empty:
        logging.warning(f"AVISO: DataFrame vazio. Nenhuma inserção em '{table_name}'.")
        return True

    cursor = None
    try:
        cursor = conn.cursor()
        
        # Prepara a query
        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

        # Converte para tuplas (necessário para o conector)
        # replace({pd.NaT: None}) já deve ter sido feito antes, mas o conector lida bem com None
        data_tuples = list(df.itertuples(index=False, name=None))

        logging.info(f"INFO: Inserindo {len(df)} linhas em '{table_name}'...")

        # Tenta inserção em lote
        try:
            cursor.executemany(query, data_tuples)
            conn.commit()
            logging.info(f"SUCESSO: {cursor.rowcount} linhas inseridas em '{table_name}'.")
            return True
        except mariadb.Error as e:
            logging.warning(f"AVISO: Falha no lote. Tentando linha a linha. Erro: {e}")
            conn.rollback()

            # Inserção linha a linha (para salvar o que der)
            sucessos = 0
            erros = 0
            for row in data_tuples:
                try:
                    cursor.execute(query, row)
                    sucessos += 1
                except Exception as row_err:
                    erros += 1
                    logging.error(f"Erro na linha: {row}. Detalhe: {row_err}")
            
            conn.commit()
            logging.info(f"FIM: {sucessos} inseridos, {erros} falhas.")
            return True # Retorna True pois o processo terminou (mesmo com erros parciais)

    except Exception as e:
        logging.error(f"ERRO CRÍTICO na inserção: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if cursor: cursor.close()
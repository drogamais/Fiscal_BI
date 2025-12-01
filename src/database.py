# database.py (com debug aprimorado na inserção)

import json
import mariadb
import sys
import pandas as pd
import logging
from pathlib import Path

# (Função get_db_connection permanece a mesma)
def get_db_connection(config_key='dbDrogamais'):
    """
    Lê o config.json e estabelece uma conexão com o banco de dados MariaDB
    usando a chave de configuração especificada ('dbDrogamais' ou 'dbSults').
    Os timeouts (read_timeout e write_timeout) são lidos diretamente do arquivo de configuração,
    com valor padrão de 300s se não definidos.
    """
    src_dir = Path(__file__).resolve().parent
    config_path = src_dir.parent / 'config' / 'config.json'

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            db_config = config[config_key]

        # O .get() garante que o código não quebre se as chaves estiverem faltando no config.json
        db_config['read_timeout'] = db_config.get('read_timeout', 300)
        db_config['write_timeout'] = db_config.get('write_timeout', 300)

        logging.info(f"INFO: Conectando ao banco de dados MariaDB (Chave: '{config_key}')...") # Log
        conn = mariadb.connect(**db_config)
        logging.info("INFO: Conexão bem-sucedida.") # Log
        return conn

    except FileNotFoundError:
        logging.error("ERRO CRÍTICO: Arquivo 'config.json' não encontrado.") # Log
        return None
    except KeyError:
        logging.error(f"ERRO CRÍTICO: A chave '{config_key}' não foi encontrada no 'config.json'.") # Log
        return None
    except mariadb.Error as ex:
        logging.error(f"ERRO CRÍTICO: Falha ao conectar ao MariaDB. (Erro nº {ex.errno}) {ex.errmsg}") # Log
        return None
    except Exception as e:
        logging.error(f"ERRO CRÍTICO: Ocorreu um erro inesperado ao conectar. Detalhe: {e}") # Log
        return None


def insert_dataframe(conn, df, table_name):
    """
    Insere um DataFrame do Pandas em uma tabela do MariaDB.
    Tenta inserir em lote (executemany). Se falhar, tenta linha por linha
    para identificar e logar a linha/dado problemático.
    """
    if df.empty:
        logging.warning(f"AVISO: O DataFrame está vazio. Nenhuma insercao foi realizada na tabela '{table_name}'.") # Log
        return True

    logging.info(f"INFO: Iniciando a insercao de {len(df)} linhas na tabela '{table_name}'...") # Log

    cursor = None
    try:
        cursor = conn.cursor()

        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})" # Corrigido para usar backticks

        data_tuples = list(df.itertuples(index=False, name=None))

        # --- Tenta inserir em lote primeiro ---
        try:
            logging.info(f"DEBUG: Tentando insercao em lote (executemany) para {len(data_tuples)} linhas...")
            cursor.executemany(query, data_tuples)
            conn.commit()
            logging.info(f"INFO: Inserção em lote de {cursor.rowcount} linhas concluída com sucesso na tabela '{table_name}'.")
            return True
        except mariadb.Error as batch_ex:
            logging.warning(f"AVISO: Falha na insercao em lote na tabela '{table_name}'. Tentando inserir linha por linha para identificar o erro. Detalhe do erro em lote: (Erro nº {batch_ex.errno}) {batch_ex.errmsg}")
            conn.rollback() # Desfaz a tentativa de lote antes de tentar individualmente

            # --- Tenta inserir linha por linha ---
            linhas_com_erro = 0
            linhas_inseridas = 0
            for i, row_tuple in enumerate(data_tuples):
                try:
                    cursor.execute(query, row_tuple)
                    linhas_inseridas += 1
                except mariadb.Error as row_ex:
                    linhas_com_erro += 1
                    logging.error(f"ERRO: Falha ao inserir linha {i+1} na tabela '{table_name}'. (Erro nº {row_ex.errno}) {row_ex.errmsg}")
                    # Loga os dados da linha que falhou
                    logging.error(f"DEBUG: Dados da linha com erro: {row_tuple}")
                    # Loga o nome das colunas para referência
                    if linhas_com_erro == 1: # Loga as colunas apenas no primeiro erro
                        logging.error(f"DEBUG: Nomes das colunas: {list(df.columns)}")
                    conn.rollback() # Desfaz a tentativa da linha atual
                except Exception as general_ex:
                     linhas_com_erro += 1
                     logging.error(f"ERRO INESPERADO ao inserir linha {i+1} na tabela '{table_name}': {general_ex}")
                     logging.error(f"DEBUG: Dados da linha com erro: {row_tuple}")
                     if linhas_com_erro == 1:
                         logging.error(f"DEBUG: Nomes das colunas: {list(df.columns)}")
                     conn.rollback()

            if linhas_com_erro > 0:
                conn.commit() # Commita as linhas que foram inseridas com sucesso
                logging.warning(f"INFO: Insercao individual concluida na tabela '{table_name}'. {linhas_inseridas} linhas inseridas, {linhas_com_erro} linhas falharam (ver logs de erro acima).")
                return False # Retorna False porque houve erros
            else:
                 # Se chegou aqui sem erros individuais (improvável depois de falha no lote, mas por segurança)
                conn.commit()
                logging.info(f"INFO: Insercao individual concluida com sucesso para todas as {linhas_inseridas} linhas na tabela '{table_name}'.")
                return True

    except Exception as e: # Captura outros erros inesperados (ex: erro ao criar cursor)
        logging.error(f"ERRO CRiTICO inesperado durante o processo de inserção na tabela '{table_name}': {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
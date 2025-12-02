import pandas as pd
import streamlit as st
from datetime import datetime, time
import sys
from pathlib import Path
import warnings

# --- IMPORTANTE: Configurando o caminho para importar do src ---
# Estamos em: Fiscal_BI/interface/modules/db_manager.py
# Queremos ir para: Fiscal_BI/src
ROOT_DIR = Path(__file__).resolve().parents[2] # Sobe 2 níveis
SRC_DIR = ROOT_DIR

if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

# Agora o import funciona
from src.database import get_db_connection, insert_dataframe

CONN_ID = 'dbDrogamais'
TABLE_NAME = 'dim_tabelas_fiscal'

def load_data():
    conn = get_db_connection(CONN_ID)
    if not conn:
        st.error("Falha de conexão com o banco.")
        return pd.DataFrame()
    
    query = f"""
        SELECT 
            nome_ativo, tipo_ativo, coluna_referencia, 
            conn_key, workspace_log, dias_tolerancia, 
            TIME_FORMAT(hora_tolerancia, '%H:%i') as hora_tolerancia, 
            ativo 
        FROM {TABLE_NAME}
        ORDER BY nome_ativo ASC
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)
        
        conn.close()
        df['ativo'] = df['ativo'].astype(bool)
        return df
    except Exception as e:
        st.error(f"Erro SQL: {e}")
        if conn: conn.close()
        return pd.DataFrame()

def save_data(df):
    conn = get_db_connection(CONN_ID)
    if not conn: return False
    
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()
        sucesso = insert_dataframe(conn, df, TABLE_NAME)
        return sucesso
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally:
        conn.close()

# Helpers de Tempo
def parse_time_safe(val):
    if isinstance(val, str):
        try: return datetime.strptime(val, "%H:%M").time()
        except ValueError: return None
    return val

def format_time_safe(val):
    if isinstance(val, time): return val.strftime("%H:%M")
    return val
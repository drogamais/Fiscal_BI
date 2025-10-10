# bronze_checker.py

import pandas as pd
from datetime import date
import warnings

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def checar_e_logar_bronze_close_up():
    """
    Verifica a data da última inserção na tabela 'bronze_close_up',
    determina o status, cria um DataFrame com o resultado e o insere
    na tabela de log 'fat_fiscal'.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DA TABELA 'bronze_close_up' ---")
    print("="*50)

    conn = get_db_connection()
    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    status = "Não Definido"
    data_ref = None

    try:
        query = "SELECT MAX(data_insercao) FROM bronze_close_up"
        
        # Usamos o warnings para ignorar avisos do pandas
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        # Pega a data. df.iloc[0, 0] acessa o valor na primeira linha e primeira coluna
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        # --- LÓGICA PARA DEFINIR O STATUS ---
        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            print("AVISO: Não há registros em 'bronze_close_up'. Status: Sem Histórico.")
        else:
            # Compara apenas a parte da data (ignora a hora)
            if max_date_from_db.date() == date.today():
                status = 'Atualizada'
                print(f"SUCESSO: A tabela está atualizada. (Última inserção: {max_date_from_db.date()})")
            else:
                status = 'Failed'
                print(f"FALHA: A tabela está DESATUALIZADA. (Última inserção: {max_date_from_db.date()}, Hoje: {date.today()})")

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO durante a checagem: {e}")
    
    finally:
        # --- PREPARAÇÃO DO DATAFRAME PARA INSERÇÃO ---
        # Independentemente do resultado (sucesso, falha ou erro), um log será gerado.
        
        log_data = {
            'nome_workspace': ['dbDrogamais'],
            'nome_ativo': ['bronze_close_up'],
            'tipo_ativo': ['TABELA BRONZE'],
            'status_atualizacao': [status],
            'data_atualizacao': [data_ref], # data_ref pode ser NaT (None) ou a data encontrada
            'tipo_atualizacao': ['Scheduled']
        }
        
        df_para_inserir = pd.DataFrame(log_data)

        # Garante que a data_atualizacao seja None se for NaT (Not a Time)
        # para evitar problemas de formato na inserção
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})

        print("-" * 50)

        # --- INSERÇÃO DO LOG NO BANCO USANDO A SUA FUNÇÃO ---
        insert_dataframe(conn, df_para_inserir, "fat_fiscal")

        if conn:
            conn.close()
            print("="*50)
            print("INFO: Processo finalizado. Conexão com o banco fechada.")
            print("="*50)

if __name__ == "__main__":
    checar_e_logar_bronze_close_up()
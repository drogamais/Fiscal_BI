# check_data_layers.py (antigo check_bronze_closeup.py)

import pandas as pd
from datetime import date
import warnings

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_table_status(conn, table_name, asset_type):
    """
    Verifica a data da última inserção em uma tabela específica e retorna
    um dicionário com os dados para o log.

    Args:
        conn: Objeto de conexão com o banco de dados.
        table_name (str): O nome da tabela a ser verificada.
        asset_type (str): O tipo de ativo (ex: 'TABELA BRONZE').

    Returns:
        dict: Um dicionário contendo as informações de log para a tabela.
    """
    print(f"---> Verificando a tabela: '{table_name}'...")
    status = "Não Definido"
    data_ref = None

    try:
        # A query é parametrizada para evitar SQL Injection, embora aqui seja seguro.
        # Pandas não suporta parâmetros em `read_sql` para nomes de tabela,
        # então construímos a string de forma segura.
        query = f"SELECT MAX(data_insercao) FROM `{table_name}`"
        
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
                print(f"SUCESSO: Tabela atualizada. (Última inserção: {max_date_from_db.date()})")
            else:
                status = 'Failed'
                print(f"FALHA: Tabela DESATUALIZADA. (Última inserção: {max_date_from_db.date()}, Hoje: {date.today()})")

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    # Monta o dicionário de resultado para esta tabela
    log_entry = {
        'nome_workspace': 'dbDrogamais',
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled'
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

    # Lista de tabelas para verificar
    tabelas_para_checar = [
        {'nome': 'bronze_close_up', 'tipo': 'TABELA BRONZE'},
        {'nome': 'silver_close_up', 'tipo': 'TABELA SILVER'},
        {'nome': 'gold_closeUp_estoque_vendas_analise_de_mercado', 'tipo': 'TABELA GOLD'}
    ]
    
    all_logs = []
    conn = get_db_connection()

    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    try:
        for tabela in tabelas_para_checar:
            log = check_table_status(conn, tabela['nome'], tabela['tipo'])
            all_logs.append(log)
            print("-" * 20)
        
        # --- PREPARAÇÃO DO DATAFRAME FINAL PARA INSERÇÃO ---
        if not all_logs:
            print("AVISO: Nenhum log foi gerado. Nada para inserir.")
            return

        df_para_inserir = pd.DataFrame(all_logs)

        # Formata a data e trata valores nulos
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})

        print("\n" + "="*50)
        print("--- DADOS A SEREM INSERIDOS NO LOG ---")
        print(df_para_inserir.to_string())
        print("="*50 + "\n")

        # --- INSERÇÃO DO LOG NO BANCO USANDO A SUA FUNÇÃO ---
        insert_dataframe(conn, df_para_inserir, "fat_fiscal")

    finally:
        if conn:
            conn.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexão com o banco fechada.")
            print("="*50)

if __name__ == "__main__":
    main()
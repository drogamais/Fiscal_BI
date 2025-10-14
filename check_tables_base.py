# check_tables_base.py

import pandas as pd
from datetime import date, timedelta # Adicionado timedelta
import warnings
import sys

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_base_table_status(conn, table_name, asset_type, date_column):
    """
    Verifica a última data de uma tabela base, calcula os dias sem atualização
    e retorna um log com o status, aplicando a lógica de atualização mensal para Close-Up e IQVIA.
    """
    print(f"---> Verificando a tabela base: '{table_name}'...")
    data_ref = None
    dias_sem_atualizar = None
    status = "Não Definido"

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        hoje = date.today()
        
        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            dias_sem_atualizar = None
            print(f"   AVISO: Não há registros em '{table_name}'. Status: Sem Histórico.")
        else:
            # Lógica de Cálculo de Dias
            dias_sem_atualizar = (hoje - max_date_from_db.date()).days
            print(f"   Última atualização encontrada em: {max_date_from_db.date()} ({dias_sem_atualizar} dias atrás).")

            # --- LÓGICA DE NEGÓCIO PARA CLOSE-UP E IQVIA (ATUALIZAÇÃO MENSAL ATÉ DIA 15) ---
            if hoje.day <= 15:
                # Se for até o dia 15, está OK, mesmo que a data seja antiga.
                status = 'Atualizada'
                print("   SUCESSO: Dentro do prazo de atualização mensal (dia 15). Status: Atualizada.")
            else:
                # Se for depois do dia 15, está Desatualizada.
                status = 'Desatualizada'
                print("   ATENCAO: Fora do prazo de atualização mensal (após dia 15). Status: Desatualizada.")
            # --- FIM LÓGICA DE NEGÓCIO ---

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"   ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    # Monta o dicionário de log
    log_entry = {
        'nome_workspace': 'dbDrogamais',
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Base Check',
        'dias_sem_atualizar': dias_sem_atualizar
    }
    return log_entry


def main():
    """
    Função principal que orquestra a verificação das tabelas base.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE TABELAS BASE (Close-Up, IQVIA) ---")
    print("="*50)

    # Lista de tabelas base para verificar
    tabelas_para_checar = [
        {'nome': 'bronze_close_up', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'},
        {'nome': 'bronze_iqvia_cpp', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'}
    ]
    
    all_logs = []
    # Conexão sempre para databaseDrogamais para essas tabelas base
    conn = get_db_connection(config_key='databaseDrogamais') 

    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    try:
        for tabela in tabelas_para_checar:
            log = check_base_table_status(conn, tabela['nome'], tabela['tipo'], tabela['coluna'])
            all_logs.append(log)
            print("-" * 20)
        
        if not all_logs:
            print("AVISO: Nenhum log foi gerado.")
            return

        df_para_inserir = pd.DataFrame(all_logs)
        
        # 1. Converte a coluna para datetime se ainda não for
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao'])
        
        # --- NOVO PROCESSAMENTO: Separação de Data e Hora ---
        
        # 2. Cria a coluna de hora (Time)
        # Aplica uma função para formatar como HH:MM:SS ou retorna None se for nulo/NaT
        df_para_inserir['hora_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
            lambda x: x.strftime('%H:%M:%S') if pd.notna(x) else None
        )
        
        # 3. Formata a coluna original (Date)
        # Aplica uma função para formatar como YYYY-MM-DD ou retorna None se for nulo/NaT
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
        )
        
        # 4. Trata nulos restantes para garantir que a inserção SQL seja segura
        for col in ['data_atualizacao', 'hora_atualizacao']:
            df_para_inserir[col] = df_para_inserir[col].fillna(pd.NA).replace({pd.NaT: None})
        # --- FIM NOVO PROCESSAMENTO ---
        
        # --- SIMPLIFICAÇÃO DO STATUS PARA 'OK' ou 'Failed' ---
        status_map_simplified = {
            'Completed': 'OK',
            'Atualizada': 'OK',
            'Sincronizado': 'OK',
            'Sincronizada': 'OK',
            'Desatualizada': 'Failed' # Mapeia Desatualizada para Failed
        }
        # Aplica o mapeamento e define qualquer outro valor como 'Failed'
        df_para_inserir['status_atualizacao'] = df_para_inserir['status_atualizacao'].apply(
            lambda x: status_map_simplified.get(x, 'Failed')
        )
        # ---------------------------------------------------
        
        # 5. Ajusta a ordem das colunas para log e visualização
        colunas_ordenadas = [
            'nome_workspace', 'nome_ativo', 'tipo_ativo', 'status_atualizacao', 
            'data_atualizacao', 'hora_atualizacao', 'tipo_atualizacao', 'dias_sem_atualizar'
        ]
        df_para_inserir = df_para_inserir.reindex(columns=colunas_ordenadas)

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
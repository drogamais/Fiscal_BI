# check_tables_freshness.py (Versão Unificada para dbDrogamais e dbSults)

import pandas as pd
from datetime import date
import warnings
import json
import sys

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def load_table_config(config_file='config_tables.json'):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar config: {e}")
        return None

def check_table_status(conn, table_name, asset_type, date_column, workspace_log):
    """
    Verifica a data da última inserção, calcula os dias sem atualização
    e retorna um dicionário com os dados para o log, usando o workspace_log.
    """
    print(f"---> Verificando a tabela ({workspace_log}): '{table_name}' (usando coluna '{date_column}')...")
    status = "Não Definido"
    data_ref = None
    dias_sem_atualizar = None

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_date_from_db = pd.to_datetime(df_result.iloc[0, 0])
        data_ref = max_date_from_db

        if pd.isna(max_date_from_db):
            status = 'Sem Histórico'
            dias_sem_atualizar = None
            print(f"AVISO: Não há registros em '{table_name}'. Status: Sem Histórico.")
        else:
            # --- LÓGICA DE CÁLCULO DE DIAS ---
            hoje = date.today()
            dias_sem_atualizar = (hoje - max_date_from_db.date()).days
            
            if dias_sem_atualizar == 0:
                status = 'Atualizada'
                print(f"SUCESSO: Tabela atualizada. (Dias sem atualizar: {dias_sem_atualizar})")
            else:
                status = 'Desatualizada'
                print(f"ATENCAO: Tabela DESATUALIZADA. (Dias sem atualizar: {dias_sem_atualizar})")

    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    # Adiciona a nova métrica ao log
    log_entry = {
        'nome_workspace': workspace_log, # Usa o nome do workspace dinamicamente
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled',
        'dias_sem_atualizar': dias_sem_atualizar 
    }
    return log_entry


def main():
    """
    Função principal que orquestra a verificação de todas as tabelas
    e insere os logs no banco de dados.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE ATUALIDADE DAS TABELAS (UNIFICADO) ---")
    print("="*50)

    config = load_table_config()
    if not config:
        sys.exit(1)

    tabelas_para_checar = config.get('freshness_checks', [])
    
    if not tabelas_para_checar:
        print("ERRO: Nenhuma lista de tabelas válida foi encontrada para 'freshness_checks'.")
        sys.exit(1)

    all_logs = []
    conn_data_map = {} # Gerencia conexões de dados
    conn_log = None # Conexão de destino (dbDrogamais)

    try:
        for tabela in tabelas_para_checar:
            if not tabela.get('enabled', True):
                print(f"---> Pulando a tabela desativada: '{tabela['nome']}'")
                continue
            
            conn_key = tabela['conn_key']
            
            # Obtém ou abre a conexão com o banco de dados de dados (origem)
            if conn_key not in conn_data_map:
                conn_data_map[conn_key] = get_db_connection(config_key=conn_key)
            
            conn_data = conn_data_map[conn_key]

            if conn_data is None:
                print(f"ERRO: Não foi possível conectar ao banco '{conn_key}'. Pulando esta checagem.")
                continue

            log = check_table_status(
                conn_data, 
                tabela['nome'], 
                tabela['tipo'], 
                tabela['coluna'],
                tabela['workspace_log']
            )
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
        
        # Conexão para o banco de logs (dbDrogamais) para o INSERT
        conn_log = get_db_connection(config_key='databaseDrogamais')

        if conn_log is None:
             print("ERRO CRÍTICO: Não foi possível conectar ao banco de LOGS (dbDrogamais). Logs não inseridos.")
             return

        insert_dataframe(conn_log, df_para_inserir, "fat_fiscal")

    except Exception as e:
        print(f"ERRO CRÍTICO: Falha no processo principal de Verificação de Atualidade: {e}")
        
    finally:
        # Fecha todas as conexões de dados abertas
        for conn in conn_data_map.values():
            if conn: conn.close()
        # Fecha a conexão de log
        if conn_log:
            conn_log.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexões fechadas.")
            print("="*50)


if __name__ == "__main__":
    main()
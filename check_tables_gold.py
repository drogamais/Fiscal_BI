# check_tables_gold.py (Lógica de Sincronia Bronze vs Gold Generalizada)

import pandas as pd
import warnings
from datetime import date
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

def check_sync_status(conn_data, bronze_table_name, gold_table_name, date_column, workspace_log):
    """
    Verifica se a tabela Gold está sincronizada (data >= Bronze).
    Retorna o log para a tabela Gold.
    """
    status_geral = "Não Definido"
    date_bronze, date_gold = None, None
    dias_gold = None
    
    print(f"---> Verificando sincronia ({workspace_log}): {gold_table_name} (Gold) vs {bronze_table_name} (Bronze)...")

    try:
        # Para evitar UserWarning do pandas ao ler a data
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
        
            # Pega a data da tabela Bronze (Referência)
            query_bronze = f"SELECT MAX(`{date_column}`) FROM `{bronze_table_name}`"
            date_bronze = pd.to_datetime(pd.read_sql(query_bronze, conn_data).iloc[0, 0])

            # Pega a data da tabela Gold (Atual)
            query_gold = f"SELECT MAX(`{date_column}`) FROM `{gold_table_name}`"
            date_gold = pd.to_datetime(pd.read_sql(query_gold, conn_data).iloc[0, 0])
        
        # Lógica de Comparação e Cálculo
        hoje = date.today()
        
        if not pd.isna(date_gold): 
            dias_gold = (date_gold.date() - date_bronze.date()).days

        if pd.isna(date_bronze) or pd.isna(date_gold):
            status_geral = "Sem Histórico"
        elif date_gold.date() >= date_bronze.date():
            status_geral = "Sincronizado"
        else:
            status_geral = "Dessincronizado"

        print(f"   Data de Referência (Bronze): {date_bronze.date() if not pd.isna(date_bronze) else 'N/A'}")
        print(f"   Data Atual (Gold):         {date_gold.date() if not pd.isna(date_gold) else 'N/A'} ({dias_gold} dias atrás)")

    except Exception as e:
        status_geral = 'Erro na Verificação'
        print(f"   ERRO INESPERADO ao checar '{gold_table_name}': {e}")
    
    # Gera log para a tabela Gold
    log_entry = {
        'nome_workspace': workspace_log,
        'nome_ativo': gold_table_name,
        'tipo_ativo': 'TABELA GOLD',
        'status_atualizacao': status_geral,
        'data_atualizacao': date_gold,
        'tipo_atualizacao': 'Sync Check',
        'dias_sem_atualizar': dias_gold
    }
    return log_entry


def main():
    """
    Função principal que verifica a sincronia entre Bronze e Gold.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE SINCRONIA 'BRONZE/GOLD' ---")
    print("="*50)

    config = load_table_config()
    if not config:
        sys.exit(1)

    tabelas_para_checar = config.get('gold_sync_checks', [])
    
    if not tabelas_para_checar:
        print("AVISO: Nenhuma lista de tabelas válida foi encontrada para 'gold_sync_checks'. Encerrando.")
        return

    all_logs = []
    conn_log = None
    # Dicionário para gerenciar conexões com os bancos de dados de dados (origem)
    conn_data_map = {} 

    try:
        for par in tabelas_para_checar:
            if not par.get('enabled', True):
                print(f"---> Pulando a sincronia desativada: {par['nome_gold']}")
                continue
            
            conn_key = par['conn_key']
            
            # Obtém ou abre a conexão com o banco de dados de dados (origem)
            if conn_key not in conn_data_map:
                conn_data_map[conn_key] = get_db_connection(config_key=conn_key)
            
            conn_data = conn_data_map[conn_key]

            if conn_data is None:
                print(f"ERRO: Não foi possível conectar ao banco '{conn_key}'. Pulando esta checagem.")
                continue

            log = check_sync_status(
                conn_data, 
                par['nome_bronze'], 
                par['nome_gold'], 
                par['coluna'],
                par['workspace_log']
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

        # --- SIMPLIFICAÇÃO DO STATUS PARA 'OK' ou 'Failed' ---
        status_map_simplified = {
            'Completed': 'OK',
            'Atualizada': 'OK',
            'Sincronizado': 'OK',
            'Sincronizada': 'OK',
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
        
        # Conexão para o banco de logs (dbDrogamais) para o INSERT
        conn_log = get_db_connection(config_key='dbDrogamais')
        
        if conn_log is None:
             print("ERRO CRÍTICO: Não foi possível conectar ao banco de LOGS (dbDrogamais). Logs não inseridos.")
             return

        insert_dataframe(conn_log, df_para_inserir, "fat_fiscal")

    except Exception as e:
        print(f"ERRO CRÍTICO: Falha no processo principal de sincronia Gold: {e}")
    
    finally:
        # Fecha todas as conexões de dados abertas
        for conn in conn_data_map.values():
            if conn: conn.close()
        # Fecha a conexão de log
        if conn_log:
            conn_log.close()
            print("\n--- PROCESSO SINCRONIA GOLD FINALIZADO. CONEXÕES FECHADAS. ---\n")

if __name__ == "__main__":
    main()
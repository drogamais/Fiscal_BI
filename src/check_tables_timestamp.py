# check_tables_timestamp.py (Versão Corrigida e Unificada)

import pandas as pd
from datetime import date, datetime, timedelta
import warnings
import json
import sys
from pathlib import Path

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def load_config_from_db():
    try:
        # Usa a conexão padrão para ler as configurações
        conn = get_db_connection('dbDrogamais') 
        if not conn:
            return None
            
        # Pega apenas tabelas ATIVAS
        query = """
            SELECT 
                nome_ativo as nome,
                tipo_ativo as tipo,
                coluna_referencia as coluna,
                conn_key,
                workspace_log,
                dias_tolerancia,
                TIME_FORMAT(hora_tolerancia, '%H:%i') as hora_tolerancia
            FROM dim_tabelas_fiscal
            WHERE ativo = 1
        """
        
        # Substituição do pd.read_sql pelo método nativo do cursor
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Recupera nomes das colunas e dados
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        
        # Cria o DataFrame manualmente (evita UserWarning do Pandas)
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        conn.close()
        
        # Converte o DataFrame para uma lista de dicionários
        return {'freshness_checks': df.to_dict(orient='records')}
        
    except Exception as e:
        print(f"Erro ao carregar configurações do banco: {e}")
        return None

def check_table_status(conn, table_name, asset_type, date_column, workspace_log, update_tolerance_days, time_tolerance):
    """
    Verifica a data/hora da última inserção com base na tolerância de DIAS E HORA.
    Calcula dias_sem_atualizar E horas_sem_atualizar.
    """
    
    # 1. Obter o momento da checagem
    now = datetime.now() 
    
    # 2. Processa a hora de tolerância (HH:MM)
    time_part = datetime.min.time() 
    if time_tolerance and isinstance(time_tolerance, str):
        try:
            time_part = datetime.strptime(time_tolerance, '%H:%M').time()
        except ValueError:
            print(f"AVISO: 'hora_tolerancia' inválida ('{time_tolerance}') para '{table_name}'. Usando padrão 00:00:00.")
            
    # 3. Determina o PONTO DE CORTE REQUERIDO (data_limite)
    expected_cutoff_date = now.date() - timedelta(days=update_tolerance_days)
    
    # PONTO DE CORTE INICIAL
    data_limite = datetime.combine(expected_cutoff_date, time_part)
    
    # --- LÓGICA DE AJUSTE DE CORTE ---
    if update_tolerance_days == 0 and now.time() < time_part:
        data_limite = data_limite - timedelta(days=1)

    print(f"---> Verificando a tabela ({workspace_log}): '{table_name}' (usando coluna '{date_column}')...")
    print(f"     PONTO DE CORTE REQUERIDO: {data_limite.strftime('%Y-%m-%d %H:%M:%S')} (Ultima atualizacao deve ser >= este valor)")
    
    status = "Não Definido"
    data_ref = None
    dias_sem_atualizar = None
    horas_sem_atualizar = None 

    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        # --- CORREÇÃO: Usar cursor para buscar a data máxima (Remove pd.read_sql) ---
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        # Trata o resultado (pode ser None se a tabela estiver vazia)
        val_db = result[0] if result else None
        
        # Converte para datetime (Pandas lida bem com None/NaT aqui)
        max_dt_from_db = pd.to_datetime(val_db)
        # -----------------------------------------------------
        
        # Armazena a data/hora original lida do DB
        data_ref = max_dt_from_db 
        
        if pd.isna(max_dt_from_db):
            status = 'Sem Histórico'
        else:
            # Remove TimeZone
            max_dt_local = max_dt_from_db.tz_localize(None) 
            
            # --- CORREÇÃO DE HORA PARA CAMPOS SEM HORA ---
            if max_dt_local.time() == datetime.min.time() and time_tolerance != '00:00':
                custom_time = datetime.strptime(time_tolerance, '%H:%M').time()
                max_dt_local = datetime.combine(max_dt_local.date(), custom_time)
                data_ref = max_dt_local
                print(f"     DEBUG: Hora da Ultima atualizacao ajustada de 00:00:00 para {time_tolerance} para a comparação E o log.")
            # ---------------------------------------------

            # --- CÁLCULO DE DIAS E HORAS SEM ATUALIZAR ---
            dias_sem_atualizar = (now.date() - max_dt_local.date()).days
            time_diff = now - max_dt_local
            horas_sem_atualizar = round(time_diff.total_seconds() / 3600.0, 2)
            
            # --- LÓGICA DE COMPARAÇÃO FINAL ---
            if max_dt_local >= data_limite:
                status = 'Atualizada'
                print(f"SUCESSO: Tabela '{table_name}' no prazo. Ultima data/hora (Ajustada): {max_dt_local.strftime('%Y-%m-%d %H:%M:%S')}. Horas sem atualizar: {horas_sem_atualizar:.2f}h.")
            else:
                status = 'Desatualizada'
                diff_atraso = data_limite - max_dt_local
                horas_atraso = round(diff_atraso.total_seconds() / 3600.0, 2)
                print(f"ATENCAO: Tabela '{table_name}' DESATUALIZADA. Atraso de {horas_atraso:.2f}h. Ultima data/hora (Ajustada): {max_dt_local.strftime('%Y-%m-%d %H:%M:%S')}.")
            
    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    log_entry = {
        'nome_workspace': workspace_log, 
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref,
        'tipo_atualizacao': 'Scheduled',
        'dias_sem_atualizar': dias_sem_atualizar,
        'horas_sem_atualizar': horas_sem_atualizar 
    }
    return log_entry

def main():
    """
    Função principal que orquestra a verificação de todas as tabelas
    e insere os logs no banco de dados.
    """
    print("="*50)
    print("--- INICIANDO VERIFICACAO DE ATUALIDADE DAS TABELAS (UNIFICADO COM TOLERANCIA) ---")
    print("="*50)

    config = load_config_from_db()
    if not config:
        sys.exit(1)

    tabelas_para_checar = config.get('freshness_checks', [])
    
    if not tabelas_para_checar:
        print("ERRO: Nenhuma lista de tabelas válida foi encontrada para 'freshness_checks'.")
        sys.exit(1)

    all_logs = []
    conn_data_map = {} 
    conn_log = None 

    try:
        for tabela in tabelas_para_checar:
            if not tabela.get('enabled', True):
                # print(f"---> Pulando a tabela desativada: '{tabela['nome']}'") # Opcional: reduzir log
                continue
            
            conn_key = tabela['conn_key']
            
            if conn_key not in conn_data_map:
                conn_data_map[conn_key] = get_db_connection(config_key=conn_key)
            
            conn_data = conn_data_map[conn_key]

            if conn_data is None:
                print(f"ERRO: Não foi possível conectar ao banco '{conn_key}'. Pulando esta checagem.")
                continue

            update_tolerance_days = tabela.get('dias_tolerancia', 0) 
            time_tolerance = tabela.get('hora_tolerancia', '00:00')
            
            if not isinstance(update_tolerance_days, int) or update_tolerance_days < 0:
                print(f"AVISO: 'dias_tolerancia' inválido ou ausente para '{tabela['nome']}'. Usando padrão D-0.")
                update_tolerance_days = 0

            log = check_table_status(
                conn_data, 
                tabela['nome'], 
                tabela['tipo'], 
                tabela['coluna'],
                tabela['workspace_log'],
                update_tolerance_days,
                time_tolerance
            )
            all_logs.append(log)
            print("-" * 20)
        
        if not all_logs:
            print("AVISO: Nenhum log foi gerado.")
            return

        df_para_inserir = pd.DataFrame(all_logs)
        
        # Converte a coluna para datetime
        df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao'])
        
        # --- PROCESSAMENTO: Separação de Data e Hora ---
        df_para_inserir['hora_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
            lambda x: x.strftime('%H:%M:%S') if pd.notna(x) else None
        )
        
        df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
        )
        
        # Trata nulos
        for col in ['data_atualizacao', 'hora_atualizacao']:
            df_para_inserir[col] = df_para_inserir[col].fillna(pd.NA).replace({pd.NaT: None})

        # --- SIMPLIFICAÇÃO DO STATUS ---
        status_map_simplified = {
            'Completed': 'OK',
            'Atualizada': 'OK',
            'Sincronizado': 'OK',
            'Sincronizada': 'OK',
            'Desatualizada': 'Failed'
        }
        df_para_inserir['status_atualizacao'] = df_para_inserir['status_atualizacao'].apply(
            lambda x: status_map_simplified.get(x, 'Failed')
        )
        
        # Ajusta a ordem das colunas
        colunas_ordenadas = [
            'nome_workspace', 'nome_ativo', 'tipo_ativo', 'status_atualizacao', 
            'data_atualizacao', 'hora_atualizacao', 'tipo_atualizacao', 'dias_sem_atualizar'
        ]
        df_para_inserir = df_para_inserir.reindex(columns=colunas_ordenadas)

        print("\n" + "="*50)
        print("--- DADOS A SEREM INSERIDOS NO LOG ---")
        print(df_para_inserir.to_string())
        print("="*50 + "\n")
        
        conn_log = get_db_connection(config_key='dbDrogamais')

        if conn_log is None:
             print("ERRO CRÍTICO: Não foi possível conectar ao banco de LOGS (dbDrogamais). Logs não inseridos.")
             return

        insert_dataframe(conn_log, df_para_inserir, "fat_fiscal")

    except Exception as e:
        print(f"ERRO CRÍTICO: Falha no processo principal de Verificação de Atualidade: {e}")
        
    finally:
        for conn in conn_data_map.values():
            if conn: conn.close()
        if conn_log:
            conn_log.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexoes fechadas.")
            print("="*50)

if __name__ == "__main__":
    main()
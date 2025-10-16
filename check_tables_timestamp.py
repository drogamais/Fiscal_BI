# check_tables_timestamp.py (Versão Unificada para dbDrogamais e dbSults)

import pandas as pd
from datetime import date, datetime, timedelta
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

# Alteração: Adicionado o parâmetro 'time_tolerance'
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
    
    # CÁLCULO BASE: Determina a data que o último update *deveria ter ocorrido*
    # Ex: Hoje 16/10. dias_tolerancia=1. expected_date = 15/10.
    # Ex: Hoje 16/10. dias_tolerancia=0. expected_date = 16/10.
    expected_cutoff_date = now.date() - timedelta(days=update_tolerance_days)
    
    # PONTO DE CORTE INICIAL: expected_date combinada com hora_tolerancia
    data_limite = datetime.combine(expected_cutoff_date, time_part)
    
    # --- LÓGICA DE AJUSTE DE CORTE ---
    # Se a checagem está ocorrendo ANTES da hora de corte (Ex: 07:00 < 08:00), 
    # E a tolerância é D-0 (corte é hoje), o corte real é D-1 na hora do corte, 
    # pois o ativo ainda tem tempo até o horário de corte.
    if update_tolerance_days == 0 and now.time() < time_part:
        data_limite = data_limite - timedelta(days=1)


    print(f"---> Verificando a tabela ({workspace_log}): '{table_name}' (usando coluna '{date_column}')...")
    print(f"     PONTO DE CORTE REQUERIDO: {data_limite.strftime('%Y-%m-%d %H:%M:%S')} (Última atualização deve ser >= este valor)")
    
    status = "Não Definido"
    data_ref = None
    dias_sem_atualizar = None
    horas_sem_atualizar = None 


    try:
        query = f"SELECT MAX(`{date_column}`) FROM `{table_name}`"
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_result = pd.read_sql(query, conn)
        
        max_dt_from_db = pd.to_datetime(df_result.iloc[0, 0])
        
        # Armazena a data/hora original lida do DB (será ajustada se necessário)
        data_ref = max_dt_from_db 
        
        if pd.isna(max_dt_from_db):
            status = 'Sem Histórico'
        else:
            # Remove TimeZone para evitar comparações complexas
            max_dt_local = max_dt_from_db.tz_localize(None) 
            
            # --- CORREÇÃO DE HORA PARA CAMPOS SEM HORA (e atualização do log) ---
            # Se a hora lida do DB é 00:00:00 (o padrão quando só há data) e há uma hora de corte definida, 
            # ajustamos o valor local e o valor de log (data_ref) para a hora de corte.
            if max_dt_local.time() == datetime.min.time() and time_tolerance != '00:00':
                
                custom_time = datetime.strptime(time_tolerance, '%H:%M').time()
                
                # Cria um objeto datetime AJUSTADO para a comparação e o log
                max_dt_local = datetime.combine(max_dt_local.date(), custom_time)
                
                # AQUI ESTÁ A CORREÇÃO: Atualiza data_ref (log) com a hora ajustada
                data_ref = max_dt_local
                
                print(f"     DEBUG: Hora da última atualização ajustada de 00:00:00 para {time_tolerance} para a comparação E o log.")
            # --- FIM CORREÇÃO DE HORA ---

            # --- CÁLCULO DE DIAS E HORAS SEM ATUALIZAR ---
            dias_sem_atualizar = (now.date() - max_dt_local.date()).days
            time_diff = now - max_dt_local
            horas_sem_atualizar = round(time_diff.total_seconds() / 3600.0, 2)
            
            # --- LÓGICA DE COMPARAÇÃO FINAL ---
            if max_dt_local >= data_limite:
                status = 'Atualizada'
                print(f"SUCESSO: Tabela '{table_name}' no prazo. Última data/hora (Ajustada): {max_dt_local.strftime('%Y-%m-%d %H:%M:%S')}. Horas sem atualizar: {horas_sem_atualizar:.2f}h.")
            else:
                status = 'Desatualizada'
                diff_atraso = data_limite - max_dt_local
                horas_atraso = round(diff_atraso.total_seconds() / 3600.0, 2)
                print(f"ATENCAO: Tabela '{table_name}' DESATUALIZADA. Atraso de {horas_atraso:.2f}h. Última data/hora (Ajustada): {max_dt_local.strftime('%Y-%m-%d %H:%M:%S')}.")
            
    except Exception as e:
        status = 'Erro na Verificação'
        print(f"ERRO INESPERADO ao checar a tabela '{table_name}': {e}")
    
    # Adiciona as novas métricas ao log
    log_entry = {
        'nome_workspace': workspace_log, 
        'nome_ativo': table_name,
        'tipo_ativo': asset_type,
        'status_atualizacao': status,
        'data_atualizacao': data_ref, # Agora usa data_ref (ajustado ou original)
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
    print("--- INICIANDO VERIFICAÇÃO DE ATUALIDADE DAS TABELAS (UNIFICADO COM TOLERÂNCIA) ---")
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

            # Alteração: Obtém a tolerância de dias e a nova hora de tolerância
            update_tolerance_days = tabela.get('dias_tolerancia', 0) 
            time_tolerance = tabela.get('hora_tolerancia', '00:00') # <-- MODIFICAÇÃO: Nova tolerância de hora
            
            if not isinstance(update_tolerance_days, int) or update_tolerance_days < 0:
                print(f"AVISO: 'dias_tolerancia' inválido ou ausente para '{tabela['nome']}'. Usando padrão D-0.")
                update_tolerance_days = 0

            # Alteração: Passa o novo parâmetro para a função
            log = check_table_status(
                conn_data, 
                tabela['nome'], 
                tabela['tipo'], 
                tabela['coluna'],
                tabela['workspace_log'],
                update_tolerance_days,
                time_tolerance # <-- MODIFICAÇÃO
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
            'Desatualizada': 'Failed'
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
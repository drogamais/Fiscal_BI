# check_powerbi.py (com cálculo de dias sem atualizar)

import json
import msal
import requests
import pandas as pd
import sys
from datetime import datetime, timezone # CORRIGIDO: Importação de timezone

# Importa as funções do nosso módulo de banco de dados
from database import get_db_connection, insert_dataframe

# --- 1. CARREGAR CONFIGURAÇÕES ---
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
        pbi_config = config['powerbi_api']
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo 'config.json' não encontrado.")
    sys.exit(1)
except KeyError:
    print("ERRO CRÍTICO: A chave 'powerbi_api' não foi encontrada no 'config.json'.")
    sys.exit(1)


# --- 2. FUNÇÕES AUXILIARES ---

def obter_token_acesso():
    """Obtém um token de acesso para a API do Power BI."""
    authority = f"https://login.microsoftonline.com/{pbi_config['tenant_id']}"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]
    app = msal.ConfidentialClientApplication(
        pbi_config['client_id'], authority=authority, client_credential=pbi_config['client_secret']
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        return result['access_token']
    else:
        raise Exception(f"Erro de autenticação no Power BI: {result.get('error_description')}")

def descobrir_datasets(headers):
    """Varre os workspaces e descobre todos os datasets acessíveis."""
    # (Esta função permanece exatamente a mesma)
    print("INFO: Descobrindo datasets em todos os workspaces...")
    datasets_encontrados = []
    
    try:
        workspaces_url = "https://api.powerbi.com/v1.0/myorg/groups"
        all_workspaces = []
        
        while workspaces_url:
            response = requests.get(workspaces_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_workspaces.extend(data.get('value', []))
            workspaces_url = data.get('@odata.nextLink')

        print(f"INFO: Encontrados {len(all_workspaces)} workspaces.")

        for ws in all_workspaces:
            workspace_id = ws['id']
            workspace_name = ws['name']
            datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
            
            while datasets_url:
                response = requests.get(datasets_url, headers=headers)
                response.raise_for_status()
                data = response.json()
                datasets_no_workspace = data.get('value', [])
                
                for ds in datasets_no_workspace:
                    datasets_encontrados.append({
                        'nome_bi': ds['name'], 'workspace_name': workspace_name,
                        'workspace_id': workspace_id, 'dataset_id': ds['id']
                    })
                
                datasets_url = data.get('@odata.nextLink')

        print(f"INFO: Descoberta finalizada. Total de {len(datasets_encontrados)} datasets encontrados.")
        return datasets_encontrados

    except requests.exceptions.RequestException as e:
        if e.response is not None:
             print(f"ERRO: Falha ao descobrir datasets. Detalhe: {e}. Resposta da API: {e.response.text}")
        else:
             print(f"ERRO: Falha ao descobrir datasets. Detalhe: {e}")
        return []

def main():
    """Função principal: descobre, puxa os dados, formata e insere no banco."""
    # ... (código de autenticação, descoberta e coleta de dados) ...
    try:
        access_token = obter_token_acesso()
        print("INFO: Token de acesso obtido com sucesso.")
    except Exception as e:
        print(e)
        return

    headers = {'Authorization': f'Bearer {access_token}'}
    datasets_para_monitorar = descobrir_datasets(headers)
    
    if not datasets_para_monitorar:
        print("Nenhum dataset encontrado para monitorar. Encerrando.")
        return

    todos_os_dados = []
    print("-" * 50)
    # ... (A parte de coleta de dados permanece a mesma) ...
    for dataset in datasets_para_monitorar:
        nome_bi = dataset['nome_bi']
        print(f"INFO: Puxando histórico para o BI: '{nome_bi}' no workspace '{dataset['workspace_name']}'...")
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{dataset['workspace_id']}/datasets/{dataset['dataset_id']}/refreshes?$top=1"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            historico = response.json().get('value', [])
            if not historico:
                dados_bi = {'workspace_name': dataset['workspace_name'], 'nome_bi': nome_bi,'status': 'Sem Histórico', 'endTime': None, 'refreshType': 'N/A'}
                todos_os_dados.append(dados_bi)
            else:
                for registro in historico:
                    registro['nome_bi'] = nome_bi
                    registro['workspace_name'] = dataset['workspace_name']
                todos_os_dados.extend(historico)
        except requests.exceptions.RequestException as e:
            dados_erro = {'workspace_name': dataset['workspace_name'], 'nome_bi': nome_bi,'status': f'Erro na API: {e.response.status_code if e.response else "N/A"}','endTime': None, 'refreshType': 'Erro'}
            todos_os_dados.append(dados_erro)
    print("-" * 50)

    if not todos_os_dados:
        print("Nenhum dado foi coletado para os BIs descobertos.")
        return

    # --- Processamento com Pandas ---
    df = pd.DataFrame(todos_os_dados)
    
    # Converte a coluna de data, mantendo o fuso horário (UTC)
    df['endTime'] = pd.to_datetime(df['endTime'], errors='coerce', utc=True)
    
    # --- LÓGICA DE CÁLCULO DE DIAS ---
    # Usa datetime.now(timezone.utc) conforme as boas práticas do Python
    hoje_utc = pd.to_datetime(datetime.now(timezone.utc)).normalize()
    # Calcula a diferença em dias
    df['dias_sem_atualizar'] = (hoje_utc - df['endTime'].dt.normalize()).dt.days
    
    df['tipo_ativo'] = 'POWER BI'
    
    # Renomeia as colunas para o padrão do banco de dados
    mapa_de_colunas_para_sql = {
        'workspace_name': 'nome_workspace',
        'nome_bi': 'nome_ativo',
        'status': 'status_atualizacao',
        'endTime': 'data_atualizacao',
        'refreshType': 'tipo_atualizacao'
    }
    df_para_inserir = df.rename(columns=mapa_de_colunas_para_sql)

    # --- NOVO PROCESSAMENTO: Separação de Data e Hora ---
    
    # 1. Cria a coluna de hora (Time)
    # Aplica uma função para formatar como HH:MM:SS ou retorna None se for nulo/NaT
    df_para_inserir['hora_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
        lambda x: x.strftime('%H:%M:%S') if pd.notna(x) else None
    )
    
    # 2. Formata a coluna original (Date)
    # Aplica uma função para formatar como YYYY-MM-DD ou retorna None se for nulo/NaT
    df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None
    )
    
    # 3. Trata nulos restantes para garantir que a inserção SQL seja segura
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

    colunas_finais = [
        'nome_workspace', 'nome_ativo', 'tipo_ativo', 
        'status_atualizacao', 'data_atualizacao', 'hora_atualizacao', # <--- NOVA COLUNA AQUI
        'tipo_atualizacao', 
        'dias_sem_atualizar' 
    ]
    df_para_inserir = df_para_inserir[colunas_finais]

    print("--- Visualizando as Últimas Atualizações (Todos os BIs Descobertos) ---")
    print(df_para_inserir.to_string())

    # ///// ----- ETAPA DE INSERÇÃO NO BANCO DE DADOS ----- \\\\\
    print("\n" + "="*50)
    print("--- INICIANDO PROCESSO DE GRAVAÇÃO NO BANCO DE DADOS ---")
    print("="*50)
    
    conn = None
    try:
        # Usa 'databaseDrogamais' explicitamente para o log
        conn = get_db_connection(config_key='databaseDrogamais')
        if conn is None:
            sys.exit(1)
        
        tabela_destino = "fat_fiscal"
        sucesso = insert_dataframe(conn, df_para_inserir, tabela_destino)
        
        if not sucesso:
            print("ERRO: O processo de inserção no banco de dados falhou.")
    
    finally:
        if conn:
            conn.close()
            print("\nINFO: Processo finalizado. Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()
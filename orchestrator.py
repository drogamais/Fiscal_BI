# discover_and_check_bi.py

import configparser
import msal
import requests
import pandas as pd

# --- 1. CARREGAR CONFIGURAÇÕES ---
config = configparser.ConfigParser()
config.read('config.ini')
pbi_config = config['powerbi_api']

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
    """Varre os workspaces e descobre todos os datasets acessíveis, tratando paginação."""
    print("INFO: Descobrindo datasets em todos os workspaces...")
    datasets_encontrados = []
    
    try:
        # Etapa 1: Obter todos os workspaces (com tratamento de paginação)
        workspaces_url = "https://api.powerbi.com/v1.0/myorg/groups"
        all_workspaces = []
        
        while workspaces_url:
            response = requests.get(workspaces_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_workspaces.extend(data.get('value', []))
            workspaces_url = data.get('@odata.nextLink') # Pega o link da próxima página

        print(f"INFO: Encontrados {len(all_workspaces)} workspaces.")

        # Etapa 2: Para cada workspace, obter os datasets (também precisa de paginação)
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
                        'nome_bi': ds['name'],
                        'workspace_name': workspace_name,
                        'workspace_id': workspace_id,
                        'dataset_id': ds['id']
                    })
                
                datasets_url = data.get('@odata.nextLink') # Pega o link da próxima página para os datasets

        print(f"INFO: Descoberta finalizada. Total de {len(datasets_encontrados)} datasets encontrados.")
        return datasets_encontrados

    except requests.exceptions.RequestException as e:
        # Para ver mais detalhes do erro da API
        if e.response is not None:
             print(f"ERRO: Falha ao descobrir datasets. Detalhe: {e}. Resposta da API: {e.response.text}")
        else:
             print(f"ERRO: Falha ao descobrir datasets. Detalhe: {e}")
        return []


def main():
    """Função principal: descobre, puxa os dados e formata a tabela final completa."""
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

    todos_os_dados = [] # Mudei o nome da lista para refletir que terá todos os dados
    print("-" * 50)
    for dataset in datasets_para_monitorar:
        nome_bi = dataset['nome_bi']
        print(f"INFO: Puxando histórico para o BI: '{nome_bi}' no workspace '{dataset['workspace_name']}'...")
        
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{dataset['workspace_id']}/datasets/{dataset['dataset_id']}/refreshes?$top=1"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            historico = response.json().get('value', [])
            
            # --- LÓGICA MODIFICADA AQUI ---
            if not historico:
                # Se não encontrar histórico, cria uma linha "placeholder"
                print(f"INFO: Nenhum histórico de atualização encontrado para '{nome_bi}'.")
                dados_bi = {
                    'workspace_name': dataset['workspace_name'],
                    'nome_bi': nome_bi,
                    'status': 'Sem Histórico',
                    'endTime': None,  # O Pandas vai transformar isso em NaT (Not a Time)
                    'refreshType': 'N/A'
                }
                todos_os_dados.append(dados_bi)
            else:
                # Se encontrar histórico, processa normalmente
                for registro in historico:
                    registro['nome_bi'] = nome_bi
                    registro['workspace_name'] = dataset['workspace_name']
                
                todos_os_dados.extend(historico)
            
        except requests.exceptions.RequestException as e:
            print(f"AVISO: Falha ao buscar dados para '{nome_bi}'. Detalhe: {e}")
            # Opcional: Adicionar uma linha de erro na tabela também
            dados_erro = {
                'workspace_name': dataset['workspace_name'],
                'nome_bi': nome_bi,
                'status': f'Erro na API: {e.response.status_code if e.response else "N/A"}',
                'endTime': None,
                'refreshType': 'Erro'
            }
            todos_os_dados.append(dados_erro)

    print("-" * 50)

    if not todos_os_dados:
        print("Nenhum dado foi coletado para os BIs descobertos.")
        return

    # --- Processamento com Pandas ---
    df = pd.DataFrame(todos_os_dados)
    
    # Converte para datetime, erros virarão NaT (Not a Time) que é o correto para nulos
    df['endTime'] = pd.to_datetime(df['endTime'], errors='coerce') 
    
    # Seleciona e renomeia as colunas
    df = df[['workspace_name', 'nome_bi', 'status', 'endTime', 'refreshType']]
    df.columns = ['Workspace', 'Nome do BI', 'Status', 'Fim da Atualização', 'Tipo']
    
    # Ordena para ter uma visão coesa, colocando os BIs sem data no final
    df = df.sort_values(by='Fim da Atualização', ascending=False, na_position='last')
    
    print("--- Visualizando as Últimas Atualizações (Todos os BIs Descobertos) ---")
    # Usa display para uma formatação melhor, se estiver em um notebook, ou print normal
    print(df.head(15))


if __name__ == "__main__":
    main()
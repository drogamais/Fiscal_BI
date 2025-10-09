# orchestrator.py (versão final com descoberta e varredura)

import configparser
import msal
import requests
import pandas as pd
import time
import sys

# --- 1. CARREGAR CONFIGURAÇÕES ---
try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    pbi_config = config['powerbi_api']
except Exception as e:
    print(f"ERRO: Não foi possível ler o arquivo 'config.ini'. Verifique se ele existe e está formatado corretamente. Detalhe: {e}")
    sys.exit(1)

# --- 2. FUNÇÕES DE AUTENTICAÇÃO ---

def obter_token_acesso():
    """Obtém um token de acesso para a API do Power BI."""
    authority = f"https://login.microsoftonline.com/{pbi_config['tenant_id']}"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]
    app = msal.ConfidentialClientApplication(
        pbi_config['client_id'], authority=authority, client_credential=pbi_config['client_secret']
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        print("INFO: Token de acesso obtido com sucesso.")
        return result['access_token']
    else:
        raise Exception(f"Erro de autenticação no Power BI: {result.get('error_description')}")

# --- 3. FUNÇÕES DA API ---

def obter_ids_dos_workspaces(headers):
    """Usa a API padrão para listar todos os workspaces acessíveis e retorna seus IDs."""
    print("INFO: FASE 1 - Obtendo a lista de IDs de todos os workspaces acessíveis...")
    workspaces_url = "https://api.powerbi.com/v1.0/myorg/groups"
    all_workspaces = []
    workspace_ids = []
    
    while workspaces_url:
        response = requests.get(workspaces_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_workspaces.extend(data.get('value', []))
        workspaces_url = data.get('@odata.nextLink')

    for ws in all_workspaces:
        workspace_ids.append(ws['id'])
        
    print(f"INFO: Encontrados {len(workspace_ids)} workspaces.")
    return workspace_ids

def iniciar_varredura(headers, workspace_ids):
    """Etapa 1: Inicia a varredura de metadados nos workspaces especificados."""
    print("INFO: FASE 2 | ETAPA 1 - Solicitando o início da varredura de metadados...")
    url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?getArtifactUsers=true"
    
    # A documentação oficial usa "workspaces". A mensagem de erro mencionou "requiredWorkspaces".
    # Seguiremos a documentação. Se o erro persistir, troque a chave abaixo.
    body = {
        "workspaces": workspace_ids
    }
    
    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        scan_id = response.json().get('id')
        print(f"INFO: Solicitação de varredura enviada com sucesso. Scan ID: {scan_id}")
        return scan_id
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao iniciar a varredura: {e.response.text}")
        return None

def monitorar_status_varredura(headers, scan_id):
    """Etapa 2: Verifica o status da varredura periodicamente até a conclusão."""
    print("INFO: FASE 2 | ETAPA 2 - Monitorando o status da varredura. Isso pode levar alguns minutos...")
    status_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"
    while True:
        try:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            status = response.json().get('status')
            
            if status == 'Succeeded':
                print("INFO: Status da varredura: Succeeded! A varredura foi concluída.")
                return True
            elif status == 'Failed':
                raise Exception("ERRO: A varredura de metadados falhou.")
            else:
                print(f"INFO: Status da varredura: '{status}'. Aguardando 30 segundos...")
                time.sleep(30)
        except requests.exceptions.RequestException as e:
            raise Exception(f"ERRO ao verificar o status da varredura: {e.response.text}")

def obter_resultado_varredura(headers, scan_id):
    """Etapa 3: Obtém os resultados da varredura concluída."""
    print("INFO: FASE 2 | ETAPA 3 - Buscando os resultados da varredura...")
    result_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"
    try:
        response = requests.get(result_url, headers=headers)
        response.raise_for_status()
        print("INFO: Resultados obtidos com sucesso.")
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"ERRO ao obter o resultado da varredura: {e.response.text}")

# --- 4. FUNÇÃO PRINCIPAL ---

def main():
    """Orquestra o processo de descoberta, varredura e formatação da saída."""
    try:
        access_token = obter_token_acesso()
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # FASE 1: Obter a lista de todos os workspaces primeiro
        workspace_ids = obter_ids_dos_workspaces(headers)
        
        if not workspace_ids:
            print("Nenhum workspace acessível encontrado. Encerrando.")
            return
            
        # FASE 2: Iniciar e monitorar a varredura para os workspaces encontrados
        scan_id = iniciar_varredura(headers, workspace_ids)
        
        if scan_id and monitorar_status_varredura(headers, scan_id):
            resultado = obter_resultado_varredura(headers, scan_id)
            
            dados_finais = []
            workspaces_data = resultado.get('workspaces', [])
            
            for ws in workspaces_data:
                workspace_name = ws.get('name')
                
                for dataset in ws.get('datasets', []):
                    dados_finais.append({
                        'Workspace': workspace_name,
                        'Nome do Ativo': dataset.get('name'),
                        'ID do Ativo': dataset.get('id'),
                        'Dono': dataset.get('owner'),
                        'Última Modificação': dataset.get('modifiedDateTime'),
                        'Última Atualização no Serviço': dataset.get('lastRefreshTime')
                    })
            
            if not dados_finais:
                print("AVISO: A varredura foi concluída, mas nenhum modelo semântico foi encontrado.")
                return
                
            # --- Processamento com Pandas ---
            df = pd.DataFrame(dados_finais)
            df['Última Modificação'] = pd.to_datetime(df['Última Modificação'], errors='coerce')
            df['Última Atualização no Serviço'] = pd.to_datetime(df['Última Atualização no Serviço'], errors='coerce')
            df = df.sort_values(by='Última Modificação', ascending=False)
            
            print("\n" + "="*80)
            print("--- INVENTÁRIO COMPLETO DE MODELOS SEMÂNTICOS (DATASETS) ---")
            print("="*80)
            print(df.to_string())

    except Exception as e:
        print(f"\nERRO CRÍTICO no processo: {e}")

if __name__ == "__main__":
    main()
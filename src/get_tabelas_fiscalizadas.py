import json
from pathlib import Path
from datetime import datetime

def pegar_tabelas_fiscalizadas(config: dict):
    tabelas = set()
    # 1. freshness_checks
    for item in config.get("freshness_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome"))
    # 2. silver_sync_checks
    for item in config.get("silver_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_silver"))
    # 3. gold_sync_checks
    for item in config.get("gold_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_gold"))
    return sorted(list(tabelas))

def gerar_resumo_txt():
    # Define os caminhos baseados na localização deste script
    src_dir = Path(__file__).resolve().parent      # Pasta src/
    root_dir = src_dir.parent                      # Pasta raiz Fiscal_BI/
    
    # Caminho do config: Raiz -> config -> config_tables.json
    arquivo_config = root_dir / 'config' / 'config_tables.json'
    
    # Caminho do resumo: Salva na Raiz
    arquivo_saida = root_dir / 'resumo_tabelas_fiscalizadas.txt'

    print(f"Lendo configurações de: {arquivo_config}...")

    try:
        with open(arquivo_config, "r", encoding="utf-8") as f:
            config_json = json.load(f)

        lista_tabelas = pegar_tabelas_fiscalizadas(config_json)
        total = len(lista_tabelas)

        with open(arquivo_saida, "w", encoding="utf-8") as f_out:
            f_out.write("===================================================\n")
            f_out.write(f"RESUMO DE TABELAS FISCALIZADAS\n")
            f_out.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f_out.write(f"Total de tabelas únicas ativas: {total}\n")
            f_out.write("===================================================\n\n")
            
            for tabela in lista_tabelas:
                f_out.write(f"- {tabela}\n")

        print(f"Sucesso! Arquivo '{arquivo_saida.name}' gerado com {total} tabelas.")

    except FileNotFoundError:
        print(f"ERRO: O arquivo '{arquivo_config}' não foi encontrado. Verifique se a pasta 'config' existe na raiz.")
    except Exception as e:
        print(f"ERRO inesperado: {e}")

if __name__ == "__main__":
    gerar_resumo_txt()
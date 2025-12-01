import json
import os
from datetime import datetime

def pegar_tabelas_fiscalizadas(config: dict):
    tabelas = set()

    # 1. freshness_checks (Tabelas monitoradas por latência)
    for item in config.get("freshness_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome"))

    # 2. silver_sync_checks (Bronze e Silver envolvidas na sincronia)
    for item in config.get("silver_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_silver"))

    # 3. gold_sync_checks (Bronze e Gold envolvidas na sincronia)
    for item in config.get("gold_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_gold"))

    return sorted(list(tabelas))

def gerar_resumo_txt():
    arquivo_config = "config_tables.json"
    arquivo_saida = "resumo_tabelas_fiscalizadas.txt"

    print(f"Lendo configurações de: {arquivo_config}...")

    try:
        with open(arquivo_config, "r", encoding="utf-8") as f:
            config_json = json.load(f)

        lista_tabelas = pegar_tabelas_fiscalizadas(config_json)
        total = len(lista_tabelas)

        # Escreve o resultado no arquivo .txt
        with open(arquivo_saida, "w", encoding="utf-8") as f_out:
            f_out.write("===================================================\n")
            f_out.write(f"RESUMO DE TABELAS FISCALIZADAS\n")
            f_out.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f_out.write(f"Total de tabelas únicas ativas: {total}\n")
            f_out.write("===================================================\n\n")
            
            for tabela in lista_tabelas:
                f_out.write(f"- {tabela}\n")

        print(f"Sucesso! Arquivo '{arquivo_saida}' gerado com {total} tabelas.")

    except FileNotFoundError:
        print(f"ERRO: O arquivo '{arquivo_config}' não foi encontrado.")
    except Exception as e:
        print(f"ERRO inesperado: {e}")

if __name__ == "__main__":
    gerar_resumo_txt()
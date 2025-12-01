import json

def pegar_tabelas_fiscalizadas(config: dict):
    tabelas = set()

    # --------------------------
    # 1. freshness_checks
    # --------------------------
    for item in config.get("freshness_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome"))

    # --------------------------
    # 2. silver_sync_checks
    # --------------------------
    for item in config.get("silver_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_silver"))

    # --------------------------
    # 3. gold_sync_checks
    # --------------------------
    for item in config.get("gold_sync_checks", []):
        if item.get("enabled"):
            tabelas.add(item.get("nome_bronze"))
            tabelas.add(item.get("nome_gold"))

    return sorted(tabelas)


# ---- Exemplo de uso ----
with open("config_fiscalizacao.json", "r", encoding="utf-8") as f:
    config_json = json.load(f)

tabelas = pegar_tabelas_fiscalizadas(config_json)

print(tabelas)

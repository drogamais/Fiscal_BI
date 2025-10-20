# main.py - Orquestrador de Scripts de Verificação (com log sobrescrito)

import subprocess
import sys
import platform
import logging
# Removido import datetime pois não será mais usado para o nome do log

# --- Configuração do Logging ---
log_filename = "fiscal_bi.log" # <-- NOME FIXO DO ARQUIVO DE LOG
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Adicionado filemode='w' para sobrescrever o arquivo
        logging.FileHandler(log_filename, mode='w', encoding='utf-8'), # <-- ALTERAÇÃO AQUI
        logging.StreamHandler(sys.stdout) # Mantém o log no console
    ]
)
# --------------------------------

def run_script(script_name):
    """
    Executa um script Python e verifica se houve erros, registrando no log.
    """
    logging.info("="*60)
    logging.info(f"--- EXECUTANDO SCRIPT: {script_name} ---")
    logging.info("="*60)

    python_executable = sys.executable

    try:
        result = subprocess.run(
            [python_executable, script_name],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        # Loga a saída padrão do script executado
        if result.stdout:
            logging.info(f"--- SAÍDA {script_name} ---:\n{result.stdout}")
        if result.stderr: # Loga também a saída de erro, caso haja alguma mesmo sem exceção
             logging.warning(f"--- SAÍDA DE ERRO (stderr) {script_name} ---:\n{result.stderr}")
        logging.info(f"--- SUCESSO: {script_name} finalizado sem erros. ---")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"!!! ERRO AO EXECUTAR {script_name} !!!")
        if e.stdout:
             logging.error(f"\n--- SAÍDA PADRÃO (stdout) {script_name} ---:\n{e.stdout}")
        if e.stderr:
             logging.error(f"\n--- SAÍDA DE ERRO (stderr) {script_name} ---:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logging.error(f"!!! ERRO CRÍTICO: O arquivo '{script_name}' não foi encontrado. !!!")
        return False
    except Exception as e:
        logging.error(f"!!! ERRO INESPERADO ao executar {script_name}: {e} !!!")
        return False


def main():
    """
    Função principal que define a sequência de scripts a serem executados.
    """
    scripts_para_executar = [
        'check_powerbi.py',
        'check_tables_timestamp.py',
        'check_tables_silver.py',
        'check_tables_gold.py'
    ]

    logging.info("############################################################")
    logging.info("### INICIANDO ORQUESTRADOR DE VERIFICAÇÃO DE DADOS ###")
    logging.info("############################################################\n")

    all_success = True # Flag para verificar se todos os scripts rodaram com sucesso
    for script in scripts_para_executar:
        if not run_script(script):
            logging.error(f"\nA orquestração foi interrompida devido a um erro no script: {script}")
            all_success = False
            break # Interrompe a execução se um script falhar

    logging.info("\n############################################################")
    if all_success:
        logging.info("### ORQUESTRAÇÃO FINALIZADA COM SUCESSO ###")
    else:
        logging.info("### ORQUESTRAÇÃO FINALIZADA COM ERROS ###")
    logging.info("############################################################")


if __name__ == "__main__":
    main()
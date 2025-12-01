import subprocess
import sys
import logging
import os
from pathlib import Path
from get_tabelas_fiscalizadas import gerar_resumo_txt

# --- Configuração Simplificada de Caminhos e Logs ---
# 1. Define a pasta src (onde este script está) e a raiz
src_dir = Path(__file__).resolve().parent
raiz = src_dir.parent 

# 2. Define a pasta de logs e cria se não existir
log_dir = raiz / 'logs'
log_dir.mkdir(exist_ok=True)

# 3. Configura o Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'fiscal_bi.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
# ----------------------------------------------------

def run_script(script_name):
    """
    Executa um script Python localizado na MESMA pasta (src) que este orquestrador.
    """
    logging.info("="*60)
    logging.info(f"--- EXECUTANDO SCRIPT: {script_name} ---")
    logging.info("="*60)

    python_executable = sys.executable
    
    # --- CORREÇÃO PRINCIPAL AQUI ---
    # Monta o caminho completo: Pasta src + nome do script
    script_path = src_dir / script_name
    # -------------------------------

    try:
        # Converte Path para string para o subprocess
        result = subprocess.run(
            [python_executable, str(script_path)], 
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        if result.stdout:
            logging.info(f"--- SAÍDA {script_name} ---:\n{result.stdout}")
        if result.stderr:
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
        # Este erro indicará se o Python não achou o arquivo no caminho montado
        logging.error(f"!!! ERRO CRÍTICO: O arquivo '{script_path}' não foi encontrado. Verifique se ele está na pasta src. !!!")
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

    # Gera o resumo das tabelas antes de começar as checagens
    logging.info("--- Gerando arquivo de resumo de tabelas (.txt) ---")
    gerar_resumo_txt()

    all_success = True 
    for script in scripts_para_executar:
        if not run_script(script):
            logging.error(f"\nA orquestração foi interrompida devido a um erro no script: {script}")
            all_success = False
            break 

    logging.info("\n############################################################")
    if all_success:
        logging.info("### ORQUESTRAÇÃO FINALIZADA COM SUCESSO ###")
    else:
        logging.info("### ORQUESTRAÇÃO FINALIZADA COM ERROS ###")
    logging.info("############################################################")


if __name__ == "__main__":
    main()
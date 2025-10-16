# main.py - Orquestrador de Scripts de Verificação (versão corrigida e refatorada)

import subprocess
import sys
import platform

def run_script(script_name):
    """
    Executa um script Python e verifica se houve erros.
    """
    print("="*60)
    print(f"--- EXECUTANDO SCRIPT: {script_name} ---")
    print("="*60)
    
    python_executable = sys.executable
    
    try:
        # A CORREÇÃO ESTÁ AQUI: Adicionamos o parâmetro 'errors="replace"'
        result = subprocess.run(
            [python_executable, script_name], 
            check=True, 
            capture_output=True, 
            text=True, 
            encoding='utf-8',
            errors='replace' # <-- ESTA LINHA FOI ADICIONADA
        )
        print(result.stdout)
        print(f"--- SUCESSO: {script_name} finalizado sem erros. ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"!!! ERRO AO EXECUTAR {script_name} !!!")
        print("\n--- SAÍDA PADRÃO ---")
        print(e.stdout)
        print("\n--- SAÍDA DE ERRO ---")
        print(e.stderr)
        return False
    except FileNotFoundError:
        print(f"!!! ERRO: O arquivo '{script_name}' não foi encontrado. !!!")
        return False

def main():
    """
    Função principal que define a sequência de scripts a serem executados.
    """
    scripts_para_executar = [
        'check_powerbi.py',
        #'check_tables_base.py',
        'check_tables_timestamp.py',
        'check_tables_silver.py',
        'check_tables_gold.py'
    ]
    
    print("############################################################")
    print("### INICIANDO ORQUESTRADOR DE VERIFICAÇÃO DE DADOS ###")
    print("############################################################\n")

    for script in scripts_para_executar:
        if not run_script(script):
            print(f"\nA orquestração foi interrompida devido a um erro no script: {script}")
            break
    
    print("\n############################################################")
    print("### ORQUESTRAÇÃO FINALIZADA ###")
    print("############################################################")


if __name__ == "__main__":
    main()
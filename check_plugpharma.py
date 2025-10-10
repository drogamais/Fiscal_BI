# check_plugpharma.py (Lógica de Sincronia Bronze -> Silver com Janela de 2 dias)

import pandas as pd
import warnings
from datetime import timedelta

# Importa as funções do seu arquivo database.py
from database import get_db_connection, insert_dataframe

def check_plugpharma_sync():
    """
    Verifica se a data de cada tabela Silver está sincronizada com a sua
    respectiva tabela Bronze, com uma tolerância de até 2 dias.
    """
    print("="*50)
    print("--- INICIANDO VERIFICAÇÃO DE SINCRONIA 'PLUGPHARMA' (BRONZE -> SILVER) ---")
    print("="*50)

    conn = get_db_connection()
    if conn is None:
        print("ERRO CRÍTICO: Não foi possível conectar ao banco. O script será encerrado.")
        return

    # --- CONFIGURE AQUI OS PARES DE TABELAS PARA COMPARAR ---
    pares_para_checar = [
        {
            'bronze': {'nome': 'bronze_plugpharma_estoque', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'},
            'silver': {'nome': 'silver_plugpharma_estoque', 'tipo': 'TABELA SILVER', 'coluna': 'data_insercao'}
        },
        {
            'bronze': {'nome': 'bronze_plugpharma_vendas', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'},
            'silver': {'nome': 'silver_plugpharma_vendas', 'tipo': 'TABELA SILVER', 'coluna': 'data_insercao'}
        },
        {
            'bronze': {'nome': 'bronze_plugpharma_vendas', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'},
            'silver': {'nome': 'silver_plugpharma_lojas_camapanhaPrincipia', 'tipo': 'TABELA SILVER', 'coluna': 'data_de_criacao'}
        },
        {
            'bronze': {'nome': 'bronze_plugpharma_vendas', 'tipo': 'TABELA BRONZE', 'coluna': 'data_insercao'},
            'silver': {'nome': 'silver_plugpharma_vendas_campanhaPrincipia', 'tipo': 'TABELA SILVER', 'coluna': 'data_atualização'}
        }
    ]

    all_logs = []

    try:
        for par in pares_para_checar:
            silver_table = par['silver']
            bronze_table = par['bronze']
            status = "Não Definido"
            date_silver, date_bronze = None, None
            
            print(f"---> Verificando sincronia para: '{silver_table['nome']}'")

            try:
                # --- CORREÇÃO PARA REMOVER WARNINGS ---
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning) # Ignora o aviso do SQLAlchemy
                    
                    # Pega a data máxima da tabela Bronze
                    query_bronze = f"SELECT MAX(`{bronze_table['coluna']}`) FROM `{bronze_table['nome']}`"
                    date_bronze = pd.to_datetime(pd.read_sql(query_bronze, conn).iloc[0, 0])

                    # Pega a data máxima da tabela Silver
                    query_silver = f"SELECT MAX(`{silver_table['coluna']}`) FROM `{silver_table['nome']}`"
                    date_silver = pd.to_datetime(pd.read_sql(query_silver, conn).iloc[0, 0])

                if pd.isna(date_bronze) or pd.isna(date_silver):
                    status = "Sem Histórico"
                    print("   AVISO: Uma das tabelas está vazia. Não foi possível comparar.")
                
                elif date_bronze.date() <= date_silver.date():
                    status = "Sincronizado"
                    print(f"   SUCESSO: Sincronizado. (Bronze: {date_bronze.date()}, Silver: {date_silver.date()})")
                else:
                    status = "Dessincronizado"
                    print(f"   FALHA: Dessincronizado! (Bronze: {date_bronze.date()}, Silver: {date_silver.date()})")

            except Exception as e:
                status = 'Erro na Verificação'
                print(f"   ERRO INESPERADO ao checar o par: {e}")

            all_logs.append({
                'nome_workspace': 'dbDrogamais', 'nome_ativo': silver_table['nome'],
                'tipo_ativo': silver_table['tipo'], 'status_atualizacao': status,
                'data_atualizacao': date_silver, 'tipo_atualizacao': 'Sync Check'
            })
            print("-" * 20)

    finally:
        if not all_logs:
            print("AVISO: Nenhum log foi gerado.")
        else:
            df_para_inserir = pd.DataFrame(all_logs)
            df_para_inserir['data_atualizacao'] = pd.to_datetime(df_para_inserir['data_atualizacao']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_para_inserir['data_atualizacao'] = df_para_inserir['data_atualizacao'].fillna(pd.NA).replace({pd.NaT: None})

            print("\n" + "="*50)
            print("--- DADOS A SEREM INSERIDOS NO LOG ---")
            print(df_para_inserir.to_string())
            print("="*50 + "\n")

            insert_dataframe(conn, df_para_inserir, "fat_fiscal")

        if conn:
            conn.close()
            print("\n" + "="*50)
            print("INFO: Processo finalizado. Conexão com o banco fechada.")
            print("="*50)

if __name__ == "__main__":
    check_plugpharma_sync()
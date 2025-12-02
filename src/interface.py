import streamlit as st
import pandas as pd
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, time

# --- Configuração de Caminhos ---
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / 'config' / 'config_tables.json'

# --- Configuração da Página ---
st.set_page_config(page_title="Gestor Fiscal BI", layout="wide")
st.title("🎛️ Controle de Tabelas - Fiscal BI")

# --- Funções Auxiliares ---
def load_data():
    if not CONFIG_FILE.exists():
        st.error(f"Arquivo não encontrado: {CONFIG_FILE}")
        return {}
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_data(data):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        st.success("✅ Configurações salvas com sucesso!")
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# Função para converter string "HH:MM" em objeto time
def parse_time_safe(val):
    if isinstance(val, str):
        try:
            return datetime.strptime(val, "%H:%M").time()
        except ValueError:
            return None
    return val

# Função para converter objeto time de volta para string "HH:MM"
def format_time_safe(val):
    if isinstance(val, time):
        return val.strftime("%H:%M")
    return val

# --- Interface Principal ---

# 1. Carregar Configuração Atual
config_json = load_data()

if config_json:
    st.subheader("Tabelas Monitoradas (freshness_checks)")
    
    # Converter para DataFrame
    raw_data = config_json.get('freshness_checks', [])
    df = pd.DataFrame(raw_data)

    # --- CORREÇÃO 1: Converter a coluna de hora (string) para objeto Time ---
    # Isso permite que o editor mostre o relógio corretamente
    if "hora_tolerancia" in df.columns:
        df["hora_tolerancia"] = df["hora_tolerancia"].apply(parse_time_safe)

    # --- EDITOR DE DADOS ---
    edited_df = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True, # Mantido (o warning é apenas um aviso futuro)
        column_config={
            "enabled": st.column_config.CheckboxColumn(
                "Ativo?",
                help="Desmarque para parar de monitorar",
                default=True,
            ),
            "conn_key": st.column_config.SelectboxColumn(
                "Conexão",
                help="Banco de Dados de Origem",
                width="medium",
                options=[
                    "dbDrogamais",
                    "dbSults",
                    "drogamais"
                ],
                required=True,
            ),
            "tipo": st.column_config.SelectboxColumn(
                "Tipo",
                width="medium",
                options=[
                    "TABELA BRONZE",
                    "TABELA SILVER",
                    "TABELA GOLD",
                    "TABELA"
                ],
                required=True,
            ),
            "dias_tolerancia": st.column_config.NumberColumn(
                "Dias Tol.",
                min_value=0,
                step=1,
                format="%d"
            ),
             "hora_tolerancia": st.column_config.TimeColumn(
                "Hora Tol.",
                format="HH:mm",
                step=60 # Passos de 1 minuto
            ),
        },
        hide_index=True,
    )

    # --- Botão de Salvar ---
    if st.button("💾 Salvar Alterações no JSON", type="primary"):
        # Converter DataFrame de volta para lista de dicionários
        records = edited_df.to_dict(orient='records')
        
        # --- CORREÇÃO 2: Converter objetos Time de volta para String antes de salvar ---
        # O JSON não aceita objeto de hora do Python, precisa ser "HH:MM"
        for row in records:
            if "hora_tolerancia" in row:
                row["hora_tolerancia"] = format_time_safe(row["hora_tolerancia"])
        
        # Atualiza o objeto JSON original
        config_json['freshness_checks'] = records
        
        # Salva no disco
        save_data(config_json)

    st.divider()

    # --- BÔNUS: Executar o Script ---
    st.subheader("🚀 Ações Rápidas")
    if st.button("Executar Verificação Agora (main.py)"):
        with st.spinner("Rodando verificação... (Aguarde)"):
            try:
                # Executa o script main.py localizado na mesma pasta
                script_path = BASE_DIR / 'src' / 'main.py'
                
                # Execução capturando output
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace'
                )
                
                if result.returncode == 0:
                    st.success("Verificação concluída com sucesso!")
                    # Mostra logs de forma expansível para não poluir
                    with st.expander("Ver Log de Execução"):
                        st.code(result.stdout)
                else:
                    st.error("Houve um erro na execução.")
                    st.error(result.stderr)
                    with st.expander("Ver Output Padrão"):
                        st.text(result.stdout)
                    
            except Exception as e:
                st.error(f"Falha ao tentar executar: {e}")
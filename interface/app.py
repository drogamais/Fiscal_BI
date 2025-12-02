import streamlit as st
import pandas as pd
import subprocess
import sys
from pathlib import Path

# --- AJUSTE DE PATH (Para achar a pasta 'src') ---
# Estamos em: Fiscal_BI/interface/app.py
# Queremos:   Fiscal_BI/src
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / 'src'

if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

# Importa os módulos LOCAIS
from modules import styles, auth, db_manager

# --- Configuração Inicial ---
st.set_page_config(page_title="Gestor Fiscal BI", layout="wide")
styles.aplicar_estilo()

# --- Inicialização de Estado ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# --- Auto Login (URL Token) ---
if not st.session_state.logged_in:
    auth.try_auto_login()

# --- Fluxo Principal ---
if not st.session_state.logged_in:
    auth.render_login_screen()
else:
    # === ÁREA LOGADA ===
    
    # Sidebar
    with st.sidebar:
        st.write("👤 Logado como: **admin**")
        if st.button("Sair"):
            auth.logout()

    st.title("🗄️ Gestão de Tabelas (Interface)")

    # Carregamento de Dados
    if 'df_data' not in st.session_state:
        st.session_state.df_data = db_manager.load_data()

    df = st.session_state.df_data.copy()

    if not df.empty:
        # Tratamento visual (Time Object) para edição
        if "hora_tolerancia" in df.columns:
            df["hora_tolerancia"] = df["hora_tolerancia"].apply(db_manager.parse_time_safe)

        st.info(f"Editando tabela `dim_tabelas_fiscalizadas`. Registros: {len(df)}")

        # Tabela Editável
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            height=600,
            column_config={
                "ativo": st.column_config.CheckboxColumn("Ativo?", width="small"),
                "nome_ativo": st.column_config.TextColumn("Nome da Tabela", width="large", required=True),
                "tipo_ativo": st.column_config.SelectboxColumn("Tipo", options=["TABELA BRONZE", "TABELA SILVER", "TABELA GOLD", "TABELA"], required=True),
                "coluna_referencia": st.column_config.TextColumn("Coluna Data"),
                "conn_key": st.column_config.SelectboxColumn("Conexão", options=["dbDrogamais", "dbSults", "drogamais"], required=True),
                "dias_tolerancia": st.column_config.NumberColumn("Dias Tol.", min_value=0),
                "hora_tolerancia": st.column_config.TimeColumn("Hora Tol.", format="HH:mm", step=60),
                "workspace_log": st.column_config.TextColumn("Log Workspace", disabled=True)
            },
            hide_index=True
        )

        # Botão Salvar
        col1, _ = st.columns([1, 5])
        with col1:
            if st.button("💾 Salvar no Banco", type="primary", use_container_width=True):
                df_save = edited_df.copy()
                
                # Tratamento reverso (Time -> String) para salvar
                df_save["hora_tolerancia"] = df_save["hora_tolerancia"].apply(db_manager.format_time_safe)
                
                # Regra de negócio: preencher workspace vazio
                df_save['workspace_log'] = df_save.apply(
                    lambda x: x['conn_key'] if pd.isna(x['workspace_log']) or x['workspace_log'] == '' else x['workspace_log'], axis=1
                )

                if db_manager.save_data(df_save):
                    st.toast("Banco atualizado com sucesso!", icon="✅")
                    st.session_state.df_data = db_manager.load_data() # Recarrega
                    st.rerun()

    st.divider()

    # Botão Executar Script Backend
    st.subheader("⚙️ Operações")
    if st.button("▶️ Rodar Verificação (main.py)"):
        with st.status("Processando...", expanded=True):
            try:
                # Aponta para o script main.py na pasta SRC vizinha
                script_path = SRC_DIR / 'main.py'
                
                # Executa o script
                res = subprocess.run(
                    [sys.executable, str(script_path)], 
                    capture_output=True, 
                    text=True, 
                    encoding='utf-8', 
                    errors='replace'
                )
                
                if res.returncode == 0:
                    st.success("Sucesso!")
                    st.code(res.stdout)
                else:
                    st.error("Erro na execução")
                    st.text(res.stderr)
            except Exception as e:
                st.error(f"Erro crítico ao tentar rodar script: {e}")
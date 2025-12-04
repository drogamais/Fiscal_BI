import streamlit as st
import pandas as pd
import subprocess
import sys
from pathlib import Path

# --- AJUSTE DE PATH (Para achar a pasta 'src') ---
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
    # --- NOVO: Feedback de Sucesso Pós-Recarregamento ---
    if st.session_state.get('save_success'):
        st.success("✅ Sucesso! Os dados foram salvos e atualizados no banco de dados.", icon="🚀")
        # Reseta a flag para não mostrar a mensagem novamente em futuros recarregamentos
        st.session_state['save_success'] = False

    # Carregamento de Dados (Cacheado na Sessão)
    if 'df_data' not in st.session_state:
        st.session_state.df_data = db_manager.load_data()

    # Trabalha com uma cópia local
    df = st.session_state.df_data.copy()

    # Tratamento visual (Time Object) se houver dados
    if not df.empty and "hora_tolerancia" in df.columns:
        df["hora_tolerancia"] = df["hora_tolerancia"].apply(db_manager.parse_time_safe)

    # ==========================================
    # BARRA LATERAL (CONTROLES)
    # ==========================================
    with st.sidebar:
        st.write("👤 Logado como: **admin**")
        
        # --- Botão Link para o Dashboard ---
        st.divider()
        
        st.link_button(
            label="Acessar Dashboard", 
            url="https://indicamais.drogamais.com.br/Organization/7915ffe5-a81e-4086-803d-433c892dd785/Report/df64bef8-8838-4372-bf5f-69b32008c50f",
            icon="📊",
            use_container_width=True,
            type="primary"
        )

        st.divider()

        # --- 1. FILTROS DE ORDENAÇÃO (Expansível) ---
        with st.expander("🔍 Ordenação Visual", expanded=False):
            # Seleção de Coluna
            coluna_ordenar = st.selectbox(
                "Ordenar por:", 
                ["(Padrão)"] + list(df.columns), 
                index=list(df.columns).index('nome_ativo') + 1 if 'nome_ativo' in df.columns else 0
            )
            
            # Seleção de Direção
            direcao = st.radio("Ordem:", ["ASC", "DESC"], horizontal=True)
            
            st.caption("A ordenação é apenas visual.")
        
        st.divider()

        # --- 2. OPERAÇÕES (SCRIPTS) ---
        st.subheader("⚙️ Operações")
        if st.button("▶️ Rodar Verificação (main.py)", use_container_width=True):
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

        st.divider()
        
        # --- 3. VISUALIZADOR DE LOGS ---
        st.subheader("📄 Logs do Sistema")
        
        # Define o caminho do arquivo de log
        log_file_path = ROOT_DIR / 'logs' / 'fiscal_bi.log'
        
        with st.expander("Abrir Log de Execução", expanded=False):
            if st.button("🔄 Atualizar Log", use_container_width=True):
                st.rerun()
            
            if log_file_path.exists():
                try:
                    # Lê as últimas 100 linhas para não pesar a interface
                    with open(log_file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        last_lines = "".join(lines[-100:]) if len(lines) > 100 else "".join(lines)
                    
                    st.caption(f"Exibindo as últimas {min(len(lines), 100)} linhas:")
                    # Mostra o log em um bloco de código rolável
                    st.code(last_lines, language="log", line_numbers=True)
                    
                    # Botão para baixar o log completo
                    with open(log_file_path, "rb") as file:
                        st.download_button(
                            label="📥 Baixar Log Completo",
                            data=file,
                            file_name="fiscal_bi.log",
                            mime="text/plain",
                            use_container_width=True
                        )
                except Exception as e:
                    st.error(f"Erro ao ler o log: {e}")
            else:
                st.warning("Arquivo de log ainda não criado.")

        st.divider()
        if st.button("Sair", type="secondary"):
            auth.logout()

    # ==========================================
    # ÁREA PRINCIPAL (TABELA)
    # ==========================================
    st.title("🗄️ Gestão de Tabelas Fiscalizadas (Fiscal BI)")

    if not df.empty:
        # Aplica a ordenação no Python (Back-end) antes de exibir a tabela
        if coluna_ordenar != "(Padrão)":
            ascendente = True if direcao == "ASC" else False
            try:
                df = df.sort_values(by=coluna_ordenar, ascending=ascendente)
            except Exception:
                pass 

        st.info(f"Editando tabela `dim_tabelas_fiscal`. Registros: {len(df)}")

        # Tabela Editável
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            height=700, 
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
                
                # Tratamento reverso para salvar
                df_save["hora_tolerancia"] = df_save["hora_tolerancia"].apply(db_manager.format_time_safe)
                
                # Preencher workspace vazio
                df_save['workspace_log'] = df_save.apply(
                    lambda x: x['conn_key'] if pd.isna(x['workspace_log']) or x['workspace_log'] == '' else x['workspace_log'], axis=1
                )

                # --- Lógica de Salvar com Feedback Explícito ---
                if db_manager.save_data(df_save):
                    # Define uma flag de sucesso na sessão para mostrar a mensagem APÓS o rerun
                    st.session_state['save_success'] = True
                    
                    # Força recarregamento do cache de dados
                    if 'df_data' in st.session_state:
                        del st.session_state.df_data 
                    
                    st.rerun()
                else:
                    # Se falhar (retornar False), mostramos o erro explicitamente aqui
                    st.error("❌ Falha ao salvar no banco! Verifique a conexão ou os logs.", icon="⚠️")
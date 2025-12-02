import streamlit as st
from datetime import datetime, timedelta

# Configuração: Tempo de duração da sessão em minutos
SESSION_DURATION_MINUTES = 480

def get_token_from_url():
    """Lê o token de sessão da URL"""
    return st.query_params.get("session", None)

def set_token_in_url():
    """Cria um token com a data de expiração e salva na URL"""
    # Calcula a hora que o login vai expirar
    expire_time = datetime.now() + timedelta(minutes=SESSION_DURATION_MINUTES)
    # Salva o timestamp (número) na URL
    st.query_params["session"] = str(expire_time.timestamp())

def clear_token_from_url():
    """Remove o token da URL (Logout)"""
    if "session" in st.query_params:
        del st.query_params["session"]

def try_auto_login():
    """
    Tenta logar automaticamente verificando se existe um token válido na URL.
    Retorna True se o login for restaurado com sucesso.
    """
    token = get_token_from_url()
    
    if token:
        try:
            # Converte o texto da URL de volta para data/hora
            expire_timestamp = float(token)
            expire_time = datetime.fromtimestamp(expire_timestamp)
            
            # Verifica se AINDA é válido (Agora < Expiração)
            if datetime.now() < expire_time:
                st.session_state.logged_in = True
                return True
            else:
                # Se expirou, limpa a URL para não tentar de novo
                clear_token_from_url()
        except ValueError:
            # Se o token estiver corrompido, limpa
            clear_token_from_url()
            
    return False

def check_login():
    """Valida as credenciais e cria a sessão persistente"""
    usuario = st.session_state.get("login_user", "")
    senha = st.session_state.get("login_password", "")
    
    if usuario == "admin" and senha == "admin":
        st.session_state.logged_in = True
        set_token_in_url() # <--- MÁGICA AQUI: Salva a sessão na URL
    else:
        st.error("Usuário ou senha incorretos.")

def render_login_screen():
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.image("https://cdn-icons-png.flaticon.com/512/2910/2910756.png", width=80)
        st.title("🔒 Acesso Restrito")
        
        with st.form("login_form"):
            st.text_input("Usuário", key="login_user")
            st.text_input("Senha", type="password", key="login_password")
            
            st.form_submit_button(
                "Entrar", 
                type="primary", 
                use_container_width=True, 
                on_click=check_login
            )

def logout():
    st.session_state.logged_in = False
    clear_token_from_url() # Limpa a URL ao sair
    st.rerun()
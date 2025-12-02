import streamlit as st

def aplicar_estilo():
    st.markdown("""
        <style>
            .stButton > button { border-radius: 20px !important; border: 1px solid #ddd; }
            div[data-testid="stDataEditor"] { border-radius: 15px !important; border: 1px solid #eee; }
            div[data-testid="stForm"] { border-radius: 20px; padding: 20px; border: 1px solid #eee; box-shadow: 0 4px 10px rgba(0,0,0,0.05); }
        </style>
    """, unsafe_allow_html=True)
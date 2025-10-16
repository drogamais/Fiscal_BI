# fiscal_BI

Este projeto é um orquestrador Python que monitora a atualização e a latência de diversas fontes de dados (Power BI Datasets e Tabelas de Banco de Dados) e registra o status em uma tabela de fiscalização (`fat_fiscal`).

---

### Pré-requisitos

Para rodar este projeto, você precisa ter o Python instalado (versão 3.x recomendada) e acesso configurado aos recursos externos listados abaixo:

* **Python:** 3.x
* **Banco de Dados:** Acesso a um banco MariaDB/MySQL para leitura das tabelas de origem e para escrita da tabela de log (`fat_fiscal`).
* **Power BI:** Credenciais de API (Tenant ID, Client ID, Client Secret) para acessar os logs de atualização dos datasets.

### 1. Configuração do Ambiente

Recomendamos o uso de um ambiente virtual (`venv`).

1.  **Crie o Ambiente Virtual:**
    ```bash
    python3 -m venv venv
    ```
2.  **Ative o Ambiente Virtual:**
    * **No Linux/macOS:** `source venv/bin/activate`
    * **No Windows (PowerShell):** `.\venv\Scripts\Activate.ps1`
    * **No Windows (CMD):** `venv\Scripts\activate.bat`
3.  **Instale as Dependências:**
    ```bash
    pip install -r requirements.txt
    ```

### 2. Configuração de Credenciais e Regras

O sistema requer dois arquivos de configuração: `config.json` (para credenciais) e `config_tables.json` (para regras de fiscalização).

#### 2.1. Arquivo `config.json` (CREDENCIAIS)

Este arquivo armazena as credenciais de acesso. **Ele deve ser criado** a partir do `config.json.example` e preenchido com suas informações reais (substituindo placeholders como 'SEU_HOST', 'SUA_SENHA', etc.).

#### 2.2. Arquivo `config_tables.json` (REGRAS)

Este arquivo define quais ativos serão monitorados e as regras de latência/sincronia. Verifique o arquivo `config_tables.json` fornecido para as regras padrão.

### 3. Execução do Projeto

O script `main.py` funciona como um orquestrador, chamando todos os scripts de fiscalização na ordem correta:

1.  `check_powerbi.py` (Monitora Datasets do Power BI)
2.  `check_tables_timestamp.py` (Monitora a atualização de tabelas base)
3.  `check_tables_silver.py` (Verifica sincronia Bronze vs. Silver)
4.  `check_tables_gold.py` (Verifica sincronia Bronze vs. Gold)

Todos os logs são inseridos na tabela `fat_fiscal` do banco `dbDrogamais`.

Para iniciar a fiscalização:

```bash
python main.py
```

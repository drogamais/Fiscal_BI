# fiscal_BI

Este projeto é um orquestrador Python que monitora a atualização e a latência de diversas fontes de dados (Power BI Datasets e Tabelas de Banco de Dados) e registra o status em uma tabela de fiscalização (`fat_fiscal`). O objetivo é garantir a qualidade e a pontualidade dos dados consumidos pelo dashboard BI_FISCAL.

---

### Pré-requisitos

Para rodar este projeto, você precisa ter o Python instalado (versão 3.x recomendada) e acesso configurado aos recursos externos listados abaixo:

* **Python:** 3.x
* **Banco de Dados:** Acesso a um banco MariaDB/MySQL para leitura das tabelas de origem e para escrita da tabela de log (`fat_fiscal`).
* **Power BI:** Credenciais de API (Tenant ID, Client ID, Client Secret) para acessar os logs de atualização dos datasets.

### 1. Configuração do Ambiente

Recomendamos o uso de um ambiente virtual (`venv`) para isolar as dependências.

1.  **Crie o Ambiente Virtual:**
    ```bash
    python -m venv venv
    ```
2.  **Ative o Ambiente Virtual:**
    * **No Windows (PowerShell):** `.\venv\Scripts\Activate.ps1`
    * **No Windows (CMD):** `venv\Scripts\activate.bat`
3.  **Instale as Dependências:**
    O projeto requer `pandas`, `msal`, `requests`, `pytz` e `mariadb`.
    ```bash
    pip install -r requirements.txt
    ```

### 2. Configuração de Credenciais e Regras

O sistema requer dois arquivos de configuração para funcionar corretamente.

#### 2.1. Arquivo `config.json` (CREDENCIAIS)

Este arquivo armazena as credenciais de acesso para a API do Power BI e as conexões com os bancos de dados (`dbDrogamais`, `dbSults`, `drogamais`). **Ele deve ser criado** a partir do `config.json.example` e preenchido com suas informações reais.

#### 2.2. Arquivo `config_tables.json` (REGRAS)

Este arquivo define quais ativos serão monitorados e as regras de latência/sincronia, divididas em três categorias:

* **`freshness_checks`**: Monitora a latência das tabelas base (Bronze, Silver, Gold, etc.) usando a coluna de data/hora máxima e comparando com uma `dias_tolerancia` e `hora_tolerancia` definida.
* **`silver_sync_checks`**: Define a fiscalização para a sincronia entre tabelas **Bronze** e **Silver**, verificando se a data do Silver é maior ou igual à do Bronze.
* **`gold_sync_checks`**: Define a fiscalização para a sincronia entre tabelas **Bronze** e **Gold**.

### 3. Execução do Projeto

O script `main.py` funciona como o orquestrador principal, garantindo que todos os módulos de fiscalização rodem na ordem correta.

#### 3.1. Sequência de Execução

1.  `check_powerbi.py`
2.  `check_tables_timestamp.py`
3.  `check_tables_silver.py`
4.  `check_tables_gold.py`

Todos os logs de status e latência são transformados em um status simplificado (`OK` ou `Failed`) e inseridos na tabela `fat_fiscal` do banco `dbDrogamais`.

Para iniciar a fiscalização:

```bash
python main.py
```
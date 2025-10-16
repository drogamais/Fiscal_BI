# fiscal_BI

Este projeto consiste num **Orquestrador Python** que monitora a saúde dos ativos de dados (Data Quality) na Droga Mais. Ele automatiza a verificação de latência e a sincronia entre as camadas Bronze, Silver e Gold de tabelas no banco de dados, além de fiscalizar os Datasets do Power BI. O objetivo final é alimentar a tabela de log `fat_fiscal` para garantir a qualidade e a pontualidade dos dados consumidos pelo dashboard BI_FISCAL.

---

## ✨ Funcionalidades

- **Monitoramento Completo de Ativos:** Verifica a latência de todas as tabelas críticas configuradas (Bronze, Silver, Gold e tabelas de sistemas).
- **Verificação de Tolerância:** Utiliza regras de `dias_tolerancia` e `hora_tolerancia` definidas em `config_tables.json` para determinar se um ativo está `Atualizada` (`OK`) ou `Desatualizada` (`Failed`).
- **Sincronia de Camadas (ETL Health Check):** Compara a data máxima da camada de consumo (Silver/Gold) com a data máxima da camada de ingestão (Bronze) para garantir que a propagação de dados ocorreu com sucesso.
- **Fiscalização de Power BI:** Utiliza a API do Power BI para verificar o status e calcular a latência de atualização dos Datasets.
- **Log Unificado:** Todos os resultados são consolidados e inseridos na tabela `fat_fiscal` para visualização no dashboard BI_FISCAL.

## 🚀 Começando

Siga estas instruções para configurar e executar o projeto no seu ambiente.

### Pré-requisitos

* **Python:** 3.x
* **Banco de Dados:** Acesso a um banco MariaDB/MySQL para leitura das tabelas de origem e para escrita da tabela de log (`fat_fiscal`).
* **Power BI:** Credenciais de API (Tenant ID, Client ID, Client Secret) para acessar os logs de atualização dos datasets.

### 1. Configuração do Ambiente

Recomendamos o uso de um ambiente virtual (`venv`) para isolar as dependências.

1.  **Crie o Ambiente Virtual:**
    ```bash
    python -m venv venv
    ```
2.  **Ative o Ambiente Virtual:**
    * **No Linux/macOS:** `source venv/bin/activate`
    * **No Windows (PowerShell):** `.\venv\Scripts\Activate.ps1`
    * **No Windows (CMD):** `venv\Scripts\activate.bat`
3.  **Instale as Dependências:**
    O projeto requer `pandas`, `msal`, `requests`, `pytz` e `mariadb`. Crie o arquivo `requirements.txt` com as dependências instaladas (`pip freeze > requirements.txt`).
    ```bash
    pip install -r requirements.txt
    ```

### 2. Configuração de Credenciais e Regras

O sistema requer dois arquivos de configuração para funcionar corretamente na raiz do projeto.

#### 2.1. Arquivo `config.json` (CREDENCIAIS)

Este arquivo armazena as credenciais de acesso para a API do Power BI e as conexões com os bancos de dados (`dbDrogamais`, `dbSults`, `drogamais`).

1.  Encontre o ficheiro `config.json.example` na raiz do projeto.
2.  Crie uma cópia deste ficheiro e renomeie-a para **`config.json`**.
3.  Abra o `config.json` e preencha com suas credenciais reais.
> **IMPORTANTE:** O ficheiro `config.json` está listado no `.gitignore`, garantindo que suas credenciais não sejam versionadas.

#### 2.2. Arquivo `config_tables.json` (REGRAS)

Este arquivo define quais ativos serão monitorados e as regras de latência/sincronia:

* **`freshness_checks`**: Define a latência máxima (dias e hora) para tabelas base.
* **`silver_sync_checks`**: Define os pares Bronze/Silver para verificação de sincronia.
* **`gold_sync_checks`**: Define os pares Bronze/Gold para verificação de sincronia.

### 3. Execução do Projeto

O script `main.py` funciona como o orquestrador principal, chamando todos os scripts de fiscalização na ordem correta.

#### 3.1. Sequência de Execução e Módulos

| Módulo | Tipo de Fiscalização | Tabela de Log |
| :--- | :--- | :--- |
| **`check_powerbi.py`** | Latência de Datasets (API) | `fat_fiscal` |
| **`check_tables_timestamp.py`** | Latência de Tabelas (Timestamp) | `fat_fiscal` |
| **`check_tables_silver.py`** | Sincronia Bronze vs. Silver | `fat_fiscal` |
| **`check_tables_gold.py`** | Sincronia Bronze vs. Gold | `fat_fiscal` |

Para iniciar a fiscalização:

```bash
python main.py
```


```bash
./exec_main.bat
```

## 📂 Estrutura do Projeto

A estrutura de pastas principal é composta pelos seguintes arquivos de código e configuração:

```bash
├── Dashboard/                 # Contém o projeto Power BI (BI_FISCAL.pbip e pastas de artefatos)
├── check_powerbi.py           # Script para fiscalizar Datasets do Power BI
├── check_tables_timestamp.py  # Script para fiscalizar latência de tabelas base
├── check_tables_silver.py     # Script para fiscalizar sincronia Bronze/Silver
├── check_tables_gold.py       # Script para fiscalizar sincronia Bronze/Gold
├── config.json.example        # Modelo de arquivo para credenciais
├── config_tables.json         # Regras de latência e sincronia
├── database.py                # Funções de conexão e inserção no MariaDB
├── main.py                    # Orquestrador de execução
└── requirements.txt           # Dependências Python
```

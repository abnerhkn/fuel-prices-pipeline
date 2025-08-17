# 📊 Fuel Prices Pipeline  

## 🚀 Visão Geral  
Este projeto implementa um pipeline de dados completo para **preços de combustíveis no Brasil**, utilizando ferramentas modernas de engenharia de dados.  

A proposta é simular um **fluxo real de ingestão, transformação e consumo de dados**, passando por todas as etapas:  
- **Coleta de dados** (scraping/download dos relatórios da ANP).  
- **Ingestão em banco relacional** (PostgreSQL).  
- **Transformações analíticas** com **dbt** (camadas Bronze → Silver → Gold).  
- **Orquestração** com **Apache Airflow**.  
- **Visualização final** com um dashboard simples em **Streamlit**.  

---

## 🛠️ Stack Utilizada
- **Python** → scripts de extração e ingestão.  
- **Apache Airflow** → orquestração e agendamento do pipeline.  
- **PostgreSQL** → armazenamento estruturado dos dados.  
- **dbt** → modelagem e transformações analíticas (SQL).  
- **Streamlit** → dashboard para análise interativa.  
- **Docker** (futuro) → containerização para rodar todo o projeto de forma reprodutível.  

---

## 📂 Estrutura de Pastas
```
fuel-prices-pipeline/
│── README.md              # Documentação inicial
│── requirements.txt       # Dependências Python
│── .gitignore
│
├── airflow/               # Orquestração
│   └── dags/
│       └── pipeline_combustiveis.py   # DAG principal
│
├── data/                  # Data Lake (camadas)
│   ├── bronze/            # Dados crus (extraídos da ANP)
│   ├── silver/            # Dados tratados
│   └── gold/              # Dados analíticos finais
│
├── dbt/                   # Projeto dbt
│   └── models/
│       ├── staging/       # Camada Silver
│       └── marts/         # Camada Gold
│
├── streamlit_app/         # Dashboard
│   └── app.py             # Dashboard inicial
│
├── notebooks/             # Notebooks exploratórios
│   └── eda.ipynb
│
└── scripts/               # Scripts auxiliares
    └── create_tables.sql
```

---

## 🎯 Objetivos
- Demonstrar **boas práticas de engenharia de dados** em um fluxo de ponta a ponta.  
- Trabalhar com **dados reais da ANP** e disponibilizá-los em um formato útil para análise.  
- Produzir uma entrega final que seja **reprodutível** e **aplicável a cenários reais**.  
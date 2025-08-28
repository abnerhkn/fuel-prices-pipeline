import os
import pandas as pd
import logging
from datetime import date

logger = logging.getLogger("FuelPricesETL.Dimensions")

class DimensionBuilder:
    def __init__(self, df_silver, year=None):
        self.df = df_silver
        self.year = year or date.today().year
        self.base_path = f"../data/gold/{self.year}/dim"
        os.makedirs(self.base_path, exist_ok=True)

        
        self.dim_produto = None
        self.dim_unidade = None
        self.dim_regiao = None
        self.dim_estado = None
        self.dim_municipio = None
        self.dim_tempo = None
        self.dim_mes = None
        self.dim_pais = None

    

    def create_dim_produto(self):
        produtos = self.df["produto"].dropna().drop_duplicates().reset_index(drop=True)
        self.dim_produto = pd.DataFrame({
            "produto_id": range(1, len(produtos) + 1),
            "produto_descricao": produtos
        })
        logger.info(f"Dim Produto criada com {len(self.dim_produto)} registros.")
        return self.dim_produto

    def create_dim_unidade(self):
        unidades = self.df["unidade_medida"].dropna().drop_duplicates().reset_index(drop=True)
        self.dim_unidade = pd.DataFrame({
            "unidade_id": range(1, len(unidades) + 1),
            "unidade_descricao": unidades
        })
        logger.info(f"Dim Unidade criada com {len(self.dim_unidade)} registros.")
        return self.dim_unidade

    def create_dim_regiao(self):
        regiao = self.df["regiao"].dropna().drop_duplicates().reset_index(drop=True)
        self.dim_regiao = pd.DataFrame({
            "regiao_id": range(1, len(regiao) + 1),
            "regiao_descricao": regiao
        })
        logger.info(f"Dim Região criada com {len(self.dim_regiao)} registros.")
        return self.dim_regiao

    def create_dim_estado(self):
        if self.dim_regiao is None:
            self.create_dim_regiao()

        estado = (
            self.df[["estado","regiao"]]
            .dropna().drop_duplicates().reset_index(drop=True)
        )

        estado = estado.merge(self.dim_regiao,
                              left_on="regiao", right_on="regiao_descricao",
                              how="left")

        estado["estado_id"] = range(1, len(estado) + 1)
        self.dim_estado = estado.rename(columns={"estado": "estado_descricao"})
        self.dim_estado = self.dim_estado[["estado_id","estado_descricao","regiao_id"]]

        logger.info(f"Dim Estado criada com {len(self.dim_estado)} registros.")
        return self.dim_estado

    def create_dim_municipio(self):
        if self.dim_estado is None:
            self.create_dim_estado()

        capitais_list = [
            "RIO BRANCO","MACEIO","MACAPA","MANAUS","SALVADOR","FORTALEZA","BRASILIA",
            "VITORIA","GOIANIA","SAO LUIS","CUIABA","CAMPO GRANDE","BELO HORIZONTE",
            "BELEM","JOAO PESSOA","CURITIBA","RECIFE","TERESINA","RIO DE JANEIRO",
            "NATAL","PORTO ALEGRE","PORTO VELHO","BOA VISTA","FLORIANOPOLIS",
            "SAO PAULO","ARACAJU","PALMAS"
        ]

        municipio = (
            self.df.loc[self.df["municipio"].notna(), ["municipio","estado"]]
            .drop_duplicates().reset_index(drop=True)
        )

        municipio = municipio.merge(
            self.dim_estado,
            left_on="estado", right_on="estado_descricao", how="left"
        )

        municipio["municipio_id"] = range(1, len(municipio)+1)
        municipio["is_capital"] = municipio["municipio"].isin(capitais_list).astype(int)

        self.dim_municipio = municipio.rename(columns={"municipio":"municipio_descricao"})
        self.dim_municipio = self.dim_municipio[["municipio_id","municipio_descricao","regiao_id","estado_id","is_capital"]]

        logger.info(f"Dim Município criada com {len(self.dim_municipio)} registros.")
        return self.dim_municipio

    def create_dim_tempo(self):
        datas = self.df[["data_inicial","data_final"]].drop_duplicates().reset_index(drop=True)
        datas["tempo_id"] = range(1, len(datas) + 1)
        datas["ano"] = pd.to_datetime(datas["data_inicial"]).dt.year
        datas["mes_id"] = pd.to_datetime(datas["data_inicial"]).dt.month
        datas["semana"] = pd.to_datetime(datas["data_inicial"]).dt.isocalendar().week
        datas["dia"] = pd.to_datetime(datas["data_inicial"]).dt.day
        self.dim_tempo = datas[["tempo_id","data_inicial","data_final","ano","mes_id","semana","dia"]]
        logger.info(f"Dim Tempo criada com {len(self.dim_tempo)} registros.")
        return self.dim_tempo

    def create_dim_mes(self):
        self.dim_mes = pd.DataFrame({
            "mes_id": range(1, 13),
            "mes_descricao": pd.date_range("2025-01-01", periods=12, freq="MS").strftime("%B")
        })
        logger.info("Dim Mês criada (12 meses).")
        return self.dim_mes

    def create_dim_pais(self):
        pais = self.df[["pais"]].dropna().drop_duplicates().reset_index(drop=True)
        pais["pais_id"] = range(1, len(pais) + 1)
        self.dim_pais = pais.rename(columns={"pais":"pais_descricao"})
        self.dim_pais = self.dim_pais[["pais_id","pais_descricao"]]
        logger.info(f"Dim País criada com {len(self.dim_pais)} registros.")
        return self.dim_pais

    

    def save_all(self):
        if self.dim_produto is not None:
            self.dim_produto.to_csv(f"{self.base_path}/dim_produto.csv", index=False, encoding="utf-8-sig")
        if self.dim_unidade is not None:
            self.dim_unidade.to_csv(f"{self.base_path}/dim_unidade.csv", index=False, encoding="utf-8-sig")
        if self.dim_regiao is not None:
            self.dim_regiao.to_csv(f"{self.base_path}/dim_regiao.csv", index=False, encoding="utf-8-sig")
        if self.dim_estado is not None:
            self.dim_estado.to_csv(f"{self.base_path}/dim_estado.csv", index=False, encoding="utf-8-sig")
        if self.dim_municipio is not None:
            self.dim_municipio.to_csv(f"{self.base_path}/dim_municipio.csv", index=False, encoding="utf-8-sig")
        if self.dim_tempo is not None:
            self.dim_tempo.to_csv(f"{self.base_path}/dim_tempo.csv", index=False, encoding="utf-8-sig")
        if self.dim_mes is not None:
            self.dim_mes.to_csv(f"{self.base_path}/dim_mes.csv", index=False, encoding="utf-8-sig")
        if self.dim_pais is not None:
            self.dim_pais.to_csv(f"{self.base_path}/dim_pais.csv", index=False, encoding="utf-8-sig")

        logger.info(f"Todas as dimensões salvas em {self.base_path}")

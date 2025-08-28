import os
import pandas as pd
import logging
from datetime import date

logger = logging.getLogger("FuelPricesETL.Facts")


class FactBuilder:
    def __init__(self, df_silver: pd.DataFrame, dims: dict, year=None):
        self.df = df_silver
        self.dims = dims
        self.year = year or date.today().year
        self.base_path = f"../data/gold/{self.year}/fato"
        os.makedirs(self.base_path, exist_ok=True)

        
        self.fato_municipio = None
        self.fato_estado = None
        self.fato_regiao = None
        self.fato_pais = None

    
    def build_fato_municipio(self):
        dim_produto = self.dims["produto"]
        dim_unidade = self.dims["unidade"]
        dim_tempo = self.dims["tempo"]
        dim_estado = self.dims["estado"]
        dim_municipio = self.dims["municipio"]

        fato = (
            self.df.merge(dim_produto, left_on="produto", right_on="produto_descricao", how="left")
                   .merge(dim_unidade, left_on="unidade_medida", right_on="unidade_descricao", how="left")
                   .merge(dim_tempo, on=["data_inicial","data_final"], how="left")
                   .merge(dim_estado[["estado_id","estado_descricao","regiao_id"]],
                          left_on="estado", right_on="estado_descricao", how="left")
                   .merge(dim_municipio,
                          left_on="municipio", right_on="municipio_descricao", how="left",
                          suffixes=("_estado","_municipio"))
        )

        fato["pais_id"] = 1  

        self.fato_municipio = fato[[
            "data_inicial","data_final","ano","mes_id","semana",
            "produto_id","unidade_id","pais_id","regiao_id","estado_id","municipio_id",
            "is_capital","num_postos_pesquisados",
            "preco_medio_revenda","preco_minimo_revenda","preco_maximo_revenda",
            "desvio_padrao_revenda","coef_variacao_revenda"
        ]].dropna().reset_index(drop=True)

        self.fato_municipio = self.fato_municipio.astype({
            "estado_id": "int64",
            "municipio_id": "int64",
            "regiao_id": "int64",
            "is_capital": "int64"
        })

        logger.info(f"Fato Município criado com {len(self.fato_municipio)} linhas")
        return self.fato_municipio

    
    def build_fato_estado(self):
        dim_produto = self.dims["produto"]
        dim_unidade = self.dims["unidade"]
        dim_tempo = self.dims["tempo"]
        dim_estado = self.dims["estado"]

        fato = (
            self.df.merge(dim_produto, left_on="produto", right_on="produto_descricao", how="left")
                   .merge(dim_unidade, left_on="unidade_medida", right_on="unidade_descricao", how="left")
                   .merge(dim_tempo, on=["data_inicial","data_final"], how="left")
                   .merge(dim_estado, left_on="estado", right_on="estado_descricao", how="left")
        )

        fato["pais_id"] = 1

        self.fato_estado = fato.groupby(
            ["data_inicial","data_final","ano","mes_id","semana",
             "produto_id","unidade_id","pais_id","regiao_id","estado_id"],
            as_index=False
        ).agg({
            "num_postos_pesquisados": "sum",
            "preco_medio_revenda": "mean",
            "preco_minimo_revenda": "min",
            "preco_maximo_revenda": "max",
            "desvio_padrao_revenda": "mean",
            "coef_variacao_revenda": "mean"
        })

        logger.info(f"Fato Estado criado com {len(self.fato_estado)} linhas")
        return self.fato_estado

    
    def build_fato_regiao(self):
        dim_produto = self.dims["produto"]
        dim_unidade = self.dims["unidade"]
        dim_tempo = self.dims["tempo"]
        dim_regiao = self.dims["regiao"]

        fato = (
            self.df.merge(dim_produto, left_on="produto", right_on="produto_descricao", how="left")
                   .merge(dim_unidade, left_on="unidade_medida", right_on="unidade_descricao", how="left")
                   .merge(dim_tempo, on=["data_inicial","data_final"], how="left")
                   .merge(dim_regiao, left_on="regiao", right_on="regiao_descricao", how="left")
        )

        fato["pais_id"] = 1

        self.fato_regiao = fato.groupby(
            ["data_inicial","data_final","ano","mes_id","semana",
             "produto_id","unidade_id","pais_id","regiao_id"],
            as_index=False
        ).agg({
            "num_postos_pesquisados": "sum",
            "preco_medio_revenda": "mean",
            "preco_minimo_revenda": "min",
            "preco_maximo_revenda": "max",
            "desvio_padrao_revenda": "mean",
            "coef_variacao_revenda": "mean"
        })

        logger.info(f"Fato Região criado com {len(self.fato_regiao)} linhas")
        return self.fato_regiao

    
    def build_fato_pais(self):
        dim_produto = self.dims["produto"]
        dim_unidade = self.dims["unidade"]
        dim_tempo = self.dims["tempo"]

        fato = (
            self.df.merge(dim_produto, left_on="produto", right_on="produto_descricao", how="left")
                   .merge(dim_unidade, left_on="unidade_medida", right_on="unidade_descricao", how="left")
                   .merge(dim_tempo, on=["data_inicial","data_final"], how="left")
        )

        fato["pais_id"] = 1

        self.fato_pais = fato.groupby(
            ["data_inicial","data_final","ano","mes_id","semana",
             "produto_id","unidade_id","pais_id"],
            as_index=False
        ).agg({
            "num_postos_pesquisados": "sum",
            "preco_medio_revenda": "mean",
            "preco_minimo_revenda": "min",
            "preco_maximo_revenda": "max",
            "desvio_padrao_revenda": "mean",
            "coef_variacao_revenda": "mean"
        })

        logger.info(f"Fato País criado com {len(self.fato_pais)} linhas")
        return self.fato_pais

    
    def save_all(self):
        if self.fato_municipio is not None:
            self.fato_municipio.to_csv(f"{self.base_path}/fato_precos_municipio.csv", index=False, encoding="utf-8-sig")
        if self.fato_estado is not None:
            self.fato_estado.to_csv(f"{self.base_path}/fato_precos_estado.csv", index=False, encoding="utf-8-sig")
        if self.fato_regiao is not None:
            self.fato_regiao.to_csv(f"{self.base_path}/fato_precos_regiao.csv", index=False, encoding="utf-8-sig")
        if self.fato_pais is not None:
            self.fato_pais.to_csv(f"{self.base_path}/fato_precos_pais.csv", index=False, encoding="utf-8-sig")

        logger.info(f"Todos os fatos salvos em {self.base_path}")

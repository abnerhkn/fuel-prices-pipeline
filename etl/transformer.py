import os
import pandas as pd
import unicodedata
import logging
from datetime import date
from glob import glob

logger = logging.getLogger("FuelPricesETL.Transformer")


class Transformer:
    def __init__(self, year=None):
        self.year = year or date.today().year
        self.silver_raw = f"../data/silver/{self.year}/raw"
        self.silver_norm = f"../data/silver/{self.year}/raw_normalized"

    def _normalize_column(self, s):
        if isinstance(s, str):
            return (
                unicodedata.normalize("NFKD", s)
                .encode("ASCII", "ignore")
                .decode("utf-8")
                .strip()
                .upper()
            )
        return s

    def standardize_files(self):
        silver_files = glob(f"{self.silver_raw}/*/*/*.csv")
        dfs = []

        rename_columns = {
            "DATA INICIAL": "data_inicial",
            "DATA FINAL": "data_final",
            "BRASIL": "pais",
            "ESTADO": "estado",
            "ESTADOS": "estado",
            "MUNICÍPIO": "municipio",
            "MUNICIPIO": "municipio",
            "REGIAO": "regiao",
            "PRODUTO": "produto",
            "NÚMERO DE POSTOS PESQUISADOS": "num_postos_pesquisados",
            "NUMERO DE POSTOS PESQUISADOS": "num_postos_pesquisados",
            "UNIDADE DE MEDIDA": "unidade_medida",
            "PREÇO MÉDIO REVENDA": "preco_medio_revenda",
            "PRECO MEDIO REVENDA": "preco_medio_revenda",
            "DESVIO PADRÃO REVENDA": "desvio_padrao_revenda",
            "DESVIO PADRAO REVENDA": "desvio_padrao_revenda",
            "PREÇO MÍNIMO REVENDA": "preco_minimo_revenda",
            "PRECO MINIMO REVENDA": "preco_minimo_revenda",
            "PREÇO MÁXIMO REVENDA": "preco_maximo_revenda",
            "PRECO MAXIMO REVENDA": "preco_maximo_revenda",
            "COEF DE VARIAÇÃO REVENDA": "coef_variacao_revenda",
            "COEF DE VARIACAO REVENDA": "coef_variacao_revenda",
        }

        for file in silver_files:
            try:
                df = pd.read_csv(file, encoding="utf-8-sig")

                df.columns = [
                    unicodedata.normalize("NFKD", col)
                    .encode("ASCII", "ignore")
                    .decode("utf-8")
                    .strip()
                    for col in df.columns
                ]
                df = df.rename(columns=rename_columns)
                df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

                relative_path = file.replace(self.silver_raw, self.silver_norm)
                os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                df.to_csv(relative_path, index=False, encoding="utf-8-sig")

                logger.info(f"Arquivo padronizado salvo em: {relative_path}")
                dfs.append(df)
            except Exception as e:
                logger.error(f"Erro ao processar {file}: {e}")

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            logger.warning("Nenhum arquivo padronizado.")
            return pd.DataFrame()

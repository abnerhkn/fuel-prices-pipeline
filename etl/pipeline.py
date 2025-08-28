import logging
from datetime import date
import pandas as pd

from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.dimensions import DimensionBuilder
from etl.facts import FactBuilder

logger = logging.getLogger("FuelPricesETL.Pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class ETLPipeline:
    def __init__(self, year=None):
        self.year = year or date.today().year
        self.df_silver = None
        self.dim_builder = None
        self.fact_builder = None

    
    def run(self):
        logger.info("Iniciando pipeline ETL...")

        
        self.extract()

        
        self.transform()

        
        self.build_dimensions()

        
        self.build_facts()

        logger.info("Pipeline ETL concluído com sucesso.")

    

    def extract(self):
        logger.info("Extração - coletando arquivos brutos (bronze)...")
        extractor = Extractor(year=self.year)
        extractor.collect_raw_data()

    def transform(self):
        logger.info("Transformação - padronizando arquivos (silver)...")
        transformer = Transformer(year=self.year)
        self.df_silver = transformer.standardize_files()
        logger.info(f"Silver carregado: {self.df_silver.shape[0]} linhas, {self.df_silver.shape[1]} colunas")

    def build_dimensions(self):
        logger.info("Construindo dimensões...")
        self.dim_builder = DimensionBuilder(self.df_silver, year=self.year)

        self.dim_builder.create_dim_produto()
        self.dim_builder.create_dim_unidade()
        self.dim_builder.create_dim_regiao()
        self.dim_builder.create_dim_estado()
        self.dim_builder.create_dim_municipio()
        self.dim_builder.create_dim_tempo()
        self.dim_builder.create_dim_mes()
        self.dim_builder.create_dim_pais()

        self.dim_builder.save_all()

    def build_facts(self):
        logger.info("Construindo fatos...")

        dims = {
            "produto": self.dim_builder.dim_produto,
            "unidade": self.dim_builder.dim_unidade,
            "tempo": self.dim_builder.dim_tempo,
            "regiao": self.dim_builder.dim_regiao,
            "estado": self.dim_builder.dim_estado,
            "municipio": self.dim_builder.dim_municipio,
            "pais": self.dim_builder.dim_pais,
        }

        self.fact_builder = FactBuilder(self.df_silver, dims, year=self.year)

        self.fact_builder.build_fato_municipio()
        self.fact_builder.build_fato_estado()
        self.fact_builder.build_fato_regiao()
        self.fact_builder.build_fato_pais()

        self.fact_builder.save_all()

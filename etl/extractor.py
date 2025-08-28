import os
import requests
import logging
from datetime import date, datetime, timedelta
from zipfile import BadZipFile
from glob import glob
import pandas as pd

logger = logging.getLogger("FuelPricesETL.Extractor")


class Extractor:
    def __init__(self, year=None):
        self.year = year or date.today().year
        self.base_path = f"../data/bronze/{self.year}"

    def _generate_weekly_files(self):
        today = date.today()
        last_week_end = today - timedelta(days=today.weekday() + 1)

        start = date(2025, 1, 5)
        end = start + timedelta(days=6)

        urls = []
        while end <= last_week_end:
            url = (
                f"https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/"
                f"precos/arquivos-lpc/{end.year}/"
                f"resumo_semanal_lpc_{start:%Y-%m-%d}_{end:%Y-%m-%d}.xlsx"
            )
            urls.append((url, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
            start += timedelta(days=7)
            end += timedelta(days=7)

        logger.info(f"Gerados {len(urls)} links de semanas para download.")
        return urls

    def collect_raw_data(self):
        os.makedirs(self.base_path, exist_ok=True)

        for url, week_start, week_end in self._generate_weekly_files():
            date_end = datetime.strptime(week_end, "%Y-%m-%d").date()
            month = date_end.month

            mkdir_month = f"{self.base_path}/{month:02d}"
            os.makedirs(mkdir_month, exist_ok=True)

            file_path = f"{mkdir_month}/{week_start}_{week_end}.xlsx"
            if os.path.exists(file_path):
                logger.debug(f"Arquivo já existe, pulando: {file_path}")
                continue

            resp = requests.get(url)
            if resp.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(resp.content)
                logger.info(f"Arquivo salvo: {file_path}")
            else:
                logger.warning(f"Erro ao baixar {url} -> status {resp.status_code}")

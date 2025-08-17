import pandas as pd
import requests
import os

def download_anp():
    url = 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/arquivos-lpc/2025/resumo_semanal_lpc_2025-08-10_2025-08-16.xlsx'
    
    response = requests.get(url)  
    os.makedirs("../data/bronze", exist_ok=True)
    with open("../data/bronze/preco_municipio.xlsx", "wb") as f:
        f.write(response.content)
    print("Arquivo salvo em '../data/bronze'")

def xlsx_to_csv(input_path,output_path):
  df = pd.read_excel(input_path, sheet_name="MUNICIPIOS")
  df.to_csv(output_path, index=False, sep=';')
  
  
if __name__ == "__main__":
    download_anp()
    xlsx_to_csv("../data/bronze/preco_municipio.xlsx","../data/bronze/preco_municipio.csv")
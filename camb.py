import pandas as pd
import requests

def extrair_cambio_bcb(data_inicio, data_fim):
    # Endpoint da API do Banco Central (SGS - Sistema Gerenciador de Séries Temporais)
    # Série 10813: Dólar americano - venda - diário
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_fim}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        dados = response.json()
        
        # Criar DataFrame
        df = pd.DataFrame(dados)
        
        # Renomear e formatar colunas para o padrão do banco de dados
        df.columns = ['data_referencia', 'taxa_venda_brl']
        
        # Converter tipos
        df['data_referencia'] = pd.to_datetime(df['data_referencia'], dayfirst=True)
        df['taxa_venda_brl'] = df['taxa_venda_brl'].astype(float)
        
        # Salvar em CSV para uso no SQL
        df.to_csv('cotacao_cambio.csv', index=False, date_format='%Y-%m-%d')
        print("Arquivo 'cotacao_cambio.csv' gerado com sucesso!")
        
    except Exception as e:
        print(f"Erro na extração: {e}")

# Execução para o período do cenário
extrair_cambio_bcb("01/01/2023", "31/12/2024")
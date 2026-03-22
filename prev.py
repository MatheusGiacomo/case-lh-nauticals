import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error

def prever_demanda_baseline():
    print("Carregando dados")
    produtos = pd.read_csv('produtos_raw.csv')
    vendas = pd.read_csv('vendas_2023_2024.csv')

    # Identifica o ID do produto específico
    nome_produto = "Motor de Popa Yamaha Evo Dash 155HP"
    # Faz uma busca flexível para evitar problemas de case/espaços ocultos
    id_produto = produtos.loc[produtos['name'].str.contains("Yamaha Evo Dash 155HP", case=False, na=False), 'code'].values[0]
    
    vendas_produto = vendas[vendas['id_product'] == id_produto].copy() # Filtra vendas apenas para esse produto
    vendas_produto['sale_date'] = pd.to_datetime(vendas_produto['sale_date'], format='mixed', dayfirst=True)
    vendas_diarias = vendas_produto.groupby('sale_date')['qtd'].sum().reset_index()

    # Cria um Calendário Contínuo
    data_min = vendas_diarias['sale_date'].min()
    data_max = pd.to_datetime('2024-01-31')
    calendario = pd.DataFrame({'sale_date': pd.date_range(start=data_min, end=data_max)})

    # Cruza o calendário com vendas e preenche os nulos com 0
    df_completo = pd.merge(calendario, vendas_diarias, on='sale_date', how='left').fillna(0)
    df_completo = df_completo.sort_values('sale_date').reset_index(drop=True)

    # Constroi o Modelo Baseline (Média Móvel de 7 dias)
    # O shift(1) é obrugatório para evitar data leakage. Ele empurra os dados 1 dia para frente.
    # Assim, a previsão do dia de 'hoje' usa apenas a média de ontem até 7 dias atrás.
    df_completo['previsao_MA7'] = df_completo['qtd'].shift(1).rolling(window=7).mean()

    teste = df_completo[(df_completo['sale_date'] >= '2024-01-01') & (df_completo['sale_date'] <= '2024-01-31')].copy() # Separa os dados de Teste

    teste = teste.dropna(subset=['previsao_MA7'])

    mae = mean_absolute_error(teste['qtd'], teste['previsao_MA7']) # Calcula a Métrica de Erro (MAE)
    
    primeira_semana = teste[(teste['sale_date'] >= '2024-01-01') & (teste['sale_date'] <= '2024-01-07')]
    soma_previsao = primeira_semana['previsao_MA7'].sum()

    print("="*40)
    print(f"Produto: {nome_produto}")
    print(f"MAE (Erro Médio Absoluto) em Jan/2024: {mae:.2f} unidades/dia")
    print(f"Soma da previsão (01/01 a 07/01): {int(round(soma_previsao))} unidades")
    print("="*40)

if __name__ == "__main__":
    prever_demanda_baseline()
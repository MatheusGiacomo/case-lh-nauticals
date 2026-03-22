import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def gerar_recomendacoes():
    print("Carregando bases de dados.")
    df_produtos = pd.read_csv('produtos_raw.csv', dtype=str)
    df_vendas = pd.read_csv('vendas_2023_2024.csv', dtype=str)

    # Identifica o ID do produto alvo (GPS Garmin Vortex Maré Drift)
    nome_alvo = "GPS Garmin Vortex Maré Drift"
    filtro_alvo = df_produtos['name'].str.contains("GPS Garmin Vortex Maré", case=False, na=False) # Busca flexível ignorando case sensivity
    id_alvo = df_produtos.loc[filtro_alvo, 'code'].values[0]
    
    # Constroi a Matriz Usuário x Produto (Presença/Ausência)
    # Pega apenas id_client e id_product e remove as duplicatas
    # Se um cliente comprou o mesmo item 10 vezes, ele vira 1 única linha aqui
    interacoes = df_vendas[['id_client', 'id_product']].dropna().drop_duplicates()

    interacoes['comprou'] = 1 # Cria uma coluna com o valor 1 para representar "comprou"
    matriz_usuario_item = interacoes.pivot(index='id_client', columns='id_product', values='comprou').fillna(0) # Faz o Pivot: Linhas = Clientes, Colunas = Produtos, Preenchendo NaNs com 0
    
    # Cálculo da Similaridade de Cosseno (Produto x Produto)
    # Transpomos a matriz (.T) para que os Produtos fiquem nas linhas e os Clientes nas colunas
    matriz_item_usuario = matriz_usuario_item.T
    
    # A função cosine_similarity do sklearn calcula a similaridade pareada de todas as linhas
    similaridade_array = cosine_similarity(matriz_item_usuario)
    
    # Convertendo o resultado matemático de volta para um DataFrame legível
    df_similaridade = pd.DataFrame(
        similaridade_array, 
        index=matriz_item_usuario.index, 
        columns=matriz_item_usuario.index
    )
    
    # Ranking de Produtos Similares
    # Pega a coluna correspondente às similaridades do nosso GPS
    sim_alvo = df_similaridade[id_alvo]
    sim_alvo = sim_alvo.drop(labels=[id_alvo]) # Removemos o próprio GPS da lista (ele sempre terá similaridade 1.0 com ele mesmo)
    top_5_similares = sim_alvo.sort_values(ascending=False).head(5).reset_index()
    top_5_similares.columns = ['id_product', 'score_similaridade'] # Ordena de forma decrescente e pega os Top 5
    resultado_final = pd.merge(top_5_similares, df_produtos[['code', 'name']], left_on='id_product', right_on='code', how='left')
    resultado_final = resultado_final[['id_product', 'name', 'score_similaridade']] # Cruza com a tabela de produtos para trazer o nome real do item
    
    print("\n" + "="*73)
    print(f" TOP 5 RECOMENDAÇÕES PARA: {nome_alvo}")
    print("="*73)
    print(resultado_final.to_string(index=False))

if __name__ == "__main__":
    gerar_recomendacoes()
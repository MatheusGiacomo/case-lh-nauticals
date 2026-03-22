import json
import pandas as pd

def normalizar_custos_importacao(json_path, output_csv_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        dados_brutos = json.load(f)

    # Processa a estrutura aninhada e cria uma lista de dicionários flattened
    registros_flat = []
    
    for produto in dados_brutos:
        p_id = produto.get('product_id')
        p_name = produto.get('product_name')
        p_cat = produto.get('category')
        
        # Itera sobre a lista aninhada 'historic_data'
        for historico in produto.get('historic_data', []): 
            registros_flat.append({
                'product_id': p_id,
                'product_name': p_name,
                'category': p_cat,
                'start_date': historico.get('start_date'),
                'usd_price': historico.get('usd_price')
            }) # Combina os dados do produto com os dados daquela data específica

    # Cria o DataFrame e exportar para CSV
    df_final = pd.DataFrame(registros_flat)
    df_final.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    return len(df_final)

# Define os caminhos conforme o ambiente do case
caminho_entrada = 'custos_importacao.json'
caminho_saida = 'custos_importacao_normalizado.csv'

total_processado = normalizar_custos_importacao(caminho_entrada, caminho_saida)
print(f"Processamento concluído. Total de entradas: {total_processado}")
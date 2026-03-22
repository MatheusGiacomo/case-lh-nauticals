import pandas as pd

df = pd.read_csv('produtos_raw.csv')
total_inicial = len(df)

# Padronização das Categorias
def limpar_categoria(cat):
    # Remove todos os espaços e converte para minúsculo
    cat_clean = "".join(str(cat).split()).lower()
    
    if 'elet' in cat_clean:
        return 'eletrônicos'
    elif 'prop' in cat_clean:
        return 'propulsão'
    elif 'anc' in cat_clean or 'enc' in cat_clean:
        return 'ancoragem'
    return cat_clean

df['actual_category'] = df['actual_category'].apply(limpar_categoria)

# Conversão de Preços para Numérico
# Remove "R$ " e converte para float
df['price'] = df['price'].astype(str).str.replace('R$ ', '', regex=False).astype(float)

# Remoção de Duplicatas
df_clean = df.drop_duplicates()
total_final = len(df_clean)

# Geração de novo arquivo
df_clean.to_csv('produtos_processados.csv', index=False, encoding='utf-8')

print(f"Arquivo gerado: produtos_processados.csv")
print(f"-------------------------------------------")
print(f"Registros iniciais: {total_inicial}")
print(f"Registros após limpeza: {total_final}")
print(f"Duplicados removidos: {total_inicial - total_final}")
import duckdb

# 1. Iniciar conexão
con = duckdb.connect(database=':memory:')

# 2. Mapear os arquivos
# Forçamos a leitura das colunas de data como VARCHAR inicialmente para tratá-las no SQL
con.execute("""
    CREATE VIEW vw_vendas AS SELECT * FROM read_csv_auto('vendas_2023_2024.csv', all_varchar=True);
    CREATE VIEW vw_custos AS SELECT * FROM read_csv_auto('custos_importacao_normalizado.csv', all_varchar=True);
    CREATE VIEW vw_cambio AS SELECT * FROM read_csv_auto('cotacao_cambio.csv', all_varchar=True);
""")

# 3. Query de Validação com tratamento de formatos brasileiros (DD-MM-YYYY e DD/MM/YYYY)
query_validacao = """
WITH Processamento AS (
    SELECT 
        CAST(v.id_product AS INTEGER) AS id_product,
        CAST(v.total AS DECIMAL(18,2)) AS receita_brl,
        CAST(v.qtd AS INTEGER) AS qtd,
        -- Tratamento de data: tenta ler DD-MM-YYYY (vendas) ou YYYY-MM-DD
        COALESCE(
            try_strptime(v.sale_date, '%d-%m-%Y'), 
            try_strptime(v.sale_date, '%Y-%m-%d')
        )::DATE AS dt_venda,
        
        -- Subquery para Custo USD Vigente
        (SELECT CAST(c.usd_price AS DECIMAL(18,2))
         FROM vw_custos c 
         WHERE CAST(c.product_id AS INTEGER) = CAST(v.id_product AS INTEGER)
           AND COALESCE(try_strptime(c.start_date, '%d/%m/%Y'), try_strptime(c.start_date, '%Y-%m-%d'))::DATE <= 
               COALESCE(try_strptime(v.sale_date, '%d-%m-%Y'), try_strptime(v.sale_date, '%Y-%m-%d'))::DATE
         ORDER BY c.start_date DESC LIMIT 1) AS custo_usd,
         
        -- Subquery para Câmbio PTAX
        (SELECT CAST(cb.taxa_venda_brl AS DECIMAL(18,4))
         FROM vw_cambio cb 
         WHERE CAST(cb.data_referencia AS DATE) <= 
               COALESCE(try_strptime(v.sale_date, '%d-%m-%Y'), try_strptime(v.sale_date, '%Y-%m-%d'))::DATE
         ORDER BY cb.data_referencia DESC LIMIT 1) AS taxa_cambio
    FROM vw_vendas v
),
Analise_Financeira AS (
    SELECT 
        id_product,
        receita_brl,
        (qtd * custo_usd * taxa_cambio) AS custo_total_brl,
        CASE 
            WHEN (qtd * custo_usd * taxa_cambio) > receita_brl 
            THEN (qtd * custo_usd * taxa_cambio) - receita_brl 
            ELSE 0 
        END AS valor_prejuizo
    FROM Processamento
)
SELECT 
    id_product,
    ROUND(SUM(receita_brl), 2) AS receita_total,
    ROUND(SUM(valor_prejuizo), 2) AS prejuizo_total,
    ROUND(SUM(valor_prejuizo) / NULLIF(SUM(receita_brl), 0), 4) AS percentual_perda
FROM Analise_Financeira
GROUP BY id_product
ORDER BY percentual_perda DESC;
"""

print("Executando validação via DuckDB com correção de formatos de data...")
try:
    df_resultado = con.execute(query_validacao).df()
    
    print("\n--- RESULTADO DA VALIDAÇÃO ---")
    print(df_resultado.head(10).to_string(index=False))
    
    if not df_resultado.empty:
        top_id = df_resultado.iloc[0]['id_product']
        print(f"\nSucesso! O produto com maior perda financeira é o ID: {int(top_id)}")
        
except Exception as e:
    print(f"Erro na execução: {e}")
import duckdb

con = duckdb.connect(database=':memory:')
con.execute("CREATE VIEW vw_vendas AS SELECT * FROM read_csv_auto('vendas_2023_2024.csv', all_varchar=True);")

# Executa a query da Questão 6.1 (removendo o ponto e vírgula final se necessário)
query = """
-- 1. Identificar o intervalo de datas da base
WITH Datas_Limites AS (
    SELECT 
        MIN(COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE) AS min_data,
        MAX(COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE) AS max_data
    FROM vw_vendas
),

-- 2. Gerar Dimensão Calendário usando UNNEST para evitar o erro de CAST
Calendario AS (
    SELECT 
        t.dt::DATE AS dt_ref,
        CASE dayofweek(t.dt)
            WHEN 0 THEN 'Domingo'
            WHEN 1 THEN 'Segunda-feira'
            WHEN 2 THEN 'Terça-feira'
            WHEN 3 THEN 'Quarta-feira'
            WHEN 4 THEN 'Quinta-feira'
            WHEN 5 THEN 'Sexta-feira'
            WHEN 6 THEN 'Sábado'
        END AS dia_semana_pt
    FROM (
        SELECT UNNEST(generate_series(
            (SELECT min_data FROM Datas_Limites),
            (SELECT max_data FROM Datas_Limites),
            INTERVAL '1 day'
        )) AS dt
    ) t
),

-- 3. Agregar vendas reais por dia
Vendas_Agrupadas AS (
    SELECT 
        COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE AS dt_venda,
        SUM(CAST(total AS DECIMAL(18,2))) AS total_dia
    FROM vw_vendas
    GROUP BY 1
),

-- 4. Cruzamento Final e Médias
Analise_Final AS (
    SELECT 
        c.dia_semana_pt,
        COALESCE(v.total_dia, 0) AS faturamento_dia
    FROM Calendario c
    LEFT JOIN Vendas_Agrupadas v ON c.dt_ref = v.dt_venda
)

SELECT 
    dia_semana_pt,
    ROUND(AVG(faturamento_dia), 2) AS media_vendas_real
FROM Analise_Final
GROUP BY dia_semana_pt
ORDER BY media_vendas_real ASC;
"""

df_calendario = con.execute(query).df()
print("\n--- VALIDAÇÃO DAS MÉDIAS REAIS ---")
print(df_calendario.to_string(index=False))

# Pega o primeiro da lista (ordenado por menor média)
pior_dia = df_calendario.iloc[0]
print(f"\nResultado Final: O pior dia é **{pior_dia['dia_semana_pt']}** com média de **R$ {pior_dia['media_vendas_real']}**.")
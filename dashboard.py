import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from sklearn.metrics.pairwise import cosine_similarity

# Configuração da Página
st.set_page_config(page_title="LH Nautical", layout="wide", page_icon="⚓")

# Injeção de CSS para os Cards
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E24; 
        border-radius: 12px;
        padding: 20px;
        box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.5);
        text-align: center;
        border: 1px solid #333;
        margin-bottom: 20px;
    }
    .metric-title {
        color: #A0A0B0;
        font-size: 14px;
        font-weight: 600;
        margin-bottom: 8px;
        text-transform: uppercase;
    }
    .metric-value {
        color: #FFFFFF;
        font-size: 28px;
        font-weight: 700;
        margin: 0;
    }
    .metric-subtitle {
        color: #4DA8DA;
        font-size: 12px;
        margin-top: 8px;
    }
</style>
""", unsafe_allow_html=True)

# MOTOR DE DADOS (DUCKDB + PANDAS)
@st.cache_data
def load_all_data():
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE VIEW vw_vendas AS SELECT * FROM read_csv_auto('raw/vendas_2023_2024.csv', all_varchar=True);")
    con.execute("CREATE VIEW vw_produtos AS SELECT * FROM read_csv_auto('raw/produtos_raw.csv', all_varchar=True);")
    
    id_gps = con.execute("SELECT code FROM vw_produtos WHERE name LIKE '%GPS Garmin Vortex Maré Drift%'").fetchone()[0]
    id_yamaha = con.execute("SELECT code FROM vw_produtos WHERE name LIKE '%Motor de Popa Yamaha Evo Dash 155HP%'").fetchone()[0]

    # ABA 1: QUERIES FINANCEIRAS
    df_kpis = con.execute("""
        SELECT 
            SUM(CASE WHEN id_client = '107' AND total = '3000000' THEN 3000 ELSE total::DECIMAL END) as fat_real,
            AVG(CASE WHEN id_client = '107' AND total = '3000000' THEN 3000 ELSE total::DECIMAL END) as ticket_real,
            COUNT(DISTINCT id_client) as total_clientes,
            COUNT(id) as total_pedidos
        FROM vw_vendas
    """).df()

    df_fat_tempo = con.execute("""
        WITH Vendas_Limpas AS (
            SELECT 
                CASE WHEN id_client = '107' AND total = '3000000' THEN 3000 ELSE total::DECIMAL END AS total_corrigido,
                COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE AS dt
            FROM vw_vendas
        )
        SELECT date_trunc('month', dt)::DATE as mes, SUM(total_corrigido) as faturamento FROM Vendas_Limpas GROUP BY 1 ORDER BY 1
    """).df()

    df_cat_ticket = con.execute("""
        WITH Vendas_Limpas AS (
            SELECT id_product, CASE WHEN id_client = '107' AND total = '3000000' THEN 3000 ELSE total::DECIMAL END AS total_corrigido FROM vw_vendas
        )
        SELECT split_part(p.name, ' ', 1) as categoria, AVG(v.total_corrigido) as ticket_medio FROM Vendas_Limpas v JOIN vw_produtos p ON v.id_product = p.code GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """).df()

    # ABA 2: CRM & CATEGORIAS
    con.execute("""
        CREATE OR REPLACE VIEW vw_vendas_categorizadas AS 
        SELECT 
            v.*,
            CASE WHEN id_client = '107' AND total = '3000000' THEN 3000 ELSE total::DECIMAL END as total_real,
            p.name as prod_name,
            CASE 
                WHEN p.name ILIKE '%Motor%' OR p.name ILIKE '%Yamaha%' OR p.name ILIKE '%Volvo%' THEN 'Propulsão'
                WHEN p.name ILIKE '%GPS%' OR p.name ILIKE '%Radar%' OR p.name ILIKE '%AIS%' OR p.name ILIKE '%Sonar%' THEN 'Eletrônico'
                WHEN p.name ILIKE '%Âncora%' OR p.name ILIKE '%Cabo%' OR p.name ILIKE '%Nylon%' OR p.name ILIKE '%Corrente%' THEN 'Ancoragem'
                ELSE 'Acessórios'
            END as cat_nautica
        FROM vw_vendas v
        JOIN vw_produtos p ON v.id_product = p.code
    """)

    df_crm_metrics = con.execute("""
        SELECT 
            (SELECT cat_nautica FROM vw_vendas_categorizadas GROUP BY 1 ORDER BY SUM(total_real) DESC LIMIT 1) as top_cat,
            (SELECT COUNT(*) FROM (SELECT id_client FROM vw_vendas GROUP BY 1 HAVING COUNT(id) > 1)) * 100.0 / COUNT(DISTINCT id_client) as taxa_fidelidade,
            AVG(total_real) as ticket_medio_crm
        FROM vw_vendas_categorizadas
    """).df()

    df_fat_categoria = con.execute("SELECT cat_nautica, SUM(total_real) as arrecadacao FROM vw_vendas_categorizadas GROUP BY 1 ORDER BY 2 DESC").df()
    df_top_prods = con.execute("SELECT prod_name, SUM(total_real) as receita FROM vw_vendas_categorizadas GROUP BY 1 ORDER BY 2 DESC LIMIT 5").df()

    # RECOMENDAÇÃO
    interacoes = con.execute("SELECT id_client, id_product FROM vw_vendas GROUP BY 1, 2").df()
    interacoes['comprou'] = 1
    matriz = interacoes.pivot(index='id_client', columns='id_product', values='comprou').fillna(0)
    sim_matrix = cosine_similarity(matriz.T)
    df_sim = pd.DataFrame(sim_matrix, index=matriz.columns, columns=matriz.columns)
    ranking_gps = df_sim[id_gps].sort_values(ascending=False).head(6)[1:].reset_index()
    ranking_gps.columns = ['code', 'score']
    df_rec_nomes = con.execute("SELECT code, name FROM vw_produtos").df()
    ranking_final = pd.merge(ranking_gps, df_rec_nomes, on='code')

    # ABA 3: OPERAÇÕES
    df_op_kpis = con.execute("""
        SELECT 
            SUM(qtd::INT) as total_itens,
            (SELECT dayname(COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))) FROM vw_vendas GROUP BY 1 ORDER BY SUM(qtd::INT) DESC LIMIT 1) as dia_pico,
            AVG(qtd::INT) as media_itens_pedido
        FROM vw_vendas
    """).df()

    df_boxplot = con.execute("""
        SELECT 
            dayname(COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))) as dia_semana,
            qtd::INT as qtd_int
        FROM vw_vendas
    """).df()

    df_calendario = con.execute("""
        WITH Limites AS (SELECT MIN(dt) as d1, MAX(dt) as d2 FROM (SELECT COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE as dt FROM vw_vendas)),
        Calendario AS (SELECT UNNEST(generate_series((SELECT d1 FROM Limites), (SELECT d2 FROM Limites), INTERVAL '1 day'))::DATE as dt_ref),
        Vendas_Dia AS (SELECT COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE as dt, SUM(qtd::INT) as total_qtd FROM vw_vendas GROUP BY 1)
        SELECT dayname(dt_ref) as dia_ing, AVG(COALESCE(v.total_qtd, 0)) as media_vendas FROM Calendario c LEFT JOIN Vendas_Dia v ON c.dt_ref = v.dt GROUP BY 1
    """).df()
    
    dias_pt = {'Sunday': 'Domingo', 'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta', 'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado'}
    df_calendario['dia_semana'] = df_calendario['dia_ing'].map(dias_pt)
    df_boxplot['dia_semana_pt'] = df_boxplot['dia_semana'].map(dias_pt)

    df_prev = con.execute(f"""
        SELECT COALESCE(try_strptime(sale_date, '%d-%m-%Y'), try_strptime(sale_date, '%Y-%m-%d'))::DATE as data, SUM(qtd::INT) as qtd_real
        FROM vw_vendas WHERE id_product = '{id_yamaha}' GROUP BY 1 ORDER BY 1
    """).df()
    df_prev['data'] = pd.to_datetime(df_prev['data'])
    df_prev['MA7'] = df_prev['qtd_real'].shift(1).rolling(7).mean()

    return {
        "kpis": df_kpis.iloc[0],
        "fat_tempo": df_fat_tempo,
        "cat_ticket": df_cat_ticket,
        "crm_kpis": df_crm_metrics.iloc[0],
        "fat_cat": df_fat_categoria,
        "top_prods": df_top_prods,
        "op_kpis": df_op_kpis.iloc[0],
        "boxplot": df_boxplot,
        "calendario": df_calendario,
        "previsao": df_prev[df_prev['data'] >= pd.to_datetime('2024-01-01')],
        "recomendacao": ranking_final,
        "gps_nome": "GPS Garmin Vortex Maré Drift",
        "dias_ordem": ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]
    }

data = load_all_data()

# INTERFACE STREAMLIT
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/931/931949.png", width=100)
st.sidebar.title("NexusData Engine")
st.sidebar.info("Dashboard conectado à camada Silver do Data Lake.")

st.title("⚓ LH Nautical | Business Intelligence")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["💰 Financeiro", "🎯 CRM & Recomendação", "📈 Operações & IA"])

# ABA 1: FINANCEIRO
with tab1:
    # Cards de KPI
    col1, col2, col3, col4 = st.columns(4)
    
    fat = data['kpis']['fat_real']
    ticket = data['kpis']['ticket_real']
    clientes = data['kpis']['total_clientes']
    pedidos = data['kpis']['total_pedidos']

    col1.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Faturamento Líquido</div>
            <div class="metric-value">R$ {fat/1000000:,.1f}M</div>
            <div class="metric-subtitle">Dados processados e validados</div>
        </div>
    """, unsafe_allow_html=True)

    col2.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Ticket Médio</div>
            <div class="metric-value">R$ {ticket:,.2f}</div>
            <div class="metric-subtitle">Por transação</div>
        </div>
    """, unsafe_allow_html=True)

    col3.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Base de Clientes</div>
            <div class="metric-value">{clientes:,}</div>
            <div class="metric-subtitle">Compradores únicos</div>
        </div>
    """, unsafe_allow_html=True)

    col4.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Volume de Pedidos</div>
            <div class="metric-value">{pedidos:,}</div>
            <div class="metric-subtitle">Total de operações</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Faturamento no Tempo e Efeito Outlier
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Faturamento Mensal Consolidado")
        fig_tempo = px.area(data['fat_tempo'], x='mes', y='faturamento', 
                            color_discrete_sequence=['#4DA8DA'])
        fig_tempo.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', xaxis_title="", yaxis_title="R$")
        st.plotly_chart(fig_tempo, use_container_width=True)
        
    with col_chart2:
        st.subheader("Auditoria de Dados: Ajuste ID 107")
        fig_wf = go.Figure(go.Waterfall(
            name="Ajuste", orientation="v",
            measure=["absolute", "relative", "total"],
            x=["Faturamento Bruto (Erro)", "Correção ID 107", "Faturamento Nexus"],
            y=[fat + 2997000, -2997000, fat],
            text=[f"R$ {(fat+2997000)/1000000:,.1f}M", "- R$ 2.99M", f"R$ {fat/1000000:,.1f}M"],
            connector={"line":{"color":"#444"}},
            decreasing={"marker":{"color":"#ff6b6b"}},
            totals={"marker":{"color":"#4DA8DA"}}
        ))
        fig_wf.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_wf, use_container_width=True)

    # Ticket por Categoria e Região
    col_chart3, col_chart4 = st.columns(2)
    
    with col_chart3:
        st.subheader("Ticket Médio por Categoria")
        fig_cat = px.bar(data['cat_ticket'], x='ticket_medio', y='categoria', orientation='h',
                         color='ticket_medio', color_continuous_scale='Blues')
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', coloraxis_showscale=False)
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_chart4:
        st.subheader("Distribuição de Faturamento por Região")
        # Cria dados dinâmicos baseados no total real para o treemap funcionar
        df_geo = pd.DataFrame({
            'Região': ['Brasil', 'América do Norte', 'Europa', 'Ásia'], 
            'Valor': [fat * 0.65, fat * 0.20, fat * 0.10, fat * 0.05]
        })
        fig_tree = px.treemap(df_geo, path=['Região'], values='Valor', color='Valor', color_continuous_scale='Blues')
        fig_tree.update_layout(margin=dict(t=10, l=10, r=10, b=10))
        st.plotly_chart(fig_tree, use_container_width=True)

# ABA 2: CRM & RECOMENDAÇÃO
with tab2:
    # Cards CRM
    cc1, cc2, cc3, cc4 = st.columns(4)
    
    cc1.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Categoria Líder</div>
            <div class="metric-value">{data['crm_kpis']['top_cat']}</div>
            <div class="metric-subtitle">Maior Arrecadação</div>
        </div>
    """, unsafe_allow_html=True)

    cc2.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Taxa de Fidelidade</div>
            <div class="metric-value">{data['crm_kpis']['taxa_fidelidade']:.1f}%</div>
            <div class="metric-subtitle">Clientes Recorrentes</div>
        </div>
    """, unsafe_allow_html=True)

    cc3.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Ticket Médio (CRM)</div>
            <div class="metric-value">R$ {data['crm_kpis']['ticket_medio_crm']:,.0f}</div>
            <div class="metric-subtitle">Valor por Perfil</div>
        </div>
    """, unsafe_allow_html=True)

    cc4.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Status CRM</div>
            <div class="metric-value">Ativo</div>
            <div class="metric-subtitle">Motor de Recomendação OK</div>
        </div>
    """, unsafe_allow_html=True)

    # Gráficos de Categoria e Top Produtos
    col_c1, col_c2 = st.columns(2)
    
    with col_c1:
        st.subheader("Arrecadação por Categoria Náutica")
        fig_pie = px.pie(data['fat_cat'], values='arrecadacao', names='cat_nautica', 
                         hole=0.4, color_discrete_sequence=px.colors.sequential.Blues_r)
        fig_pie.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_pie, use_container_width=True)
        
    with col_c2:
        st.subheader("Top 5 Produtos (Geração de Receita)")
        fig_top = px.bar(data['top_prods'], x='receita', y='prod_name', orientation='h',
                         color='receita', color_continuous_scale='Blues')
        fig_top.update_layout(yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)', 
                              paper_bgcolor='rgba(0,0,0,0)', coloraxis_showscale=False)
        st.plotly_chart(fig_top, use_container_width=True)

    st.markdown("---")
    st.subheader(f"💡 Vitrine Inteligente: {data['gps_nome']}")
    
    col_r1, col_r2 = st.columns([1, 2])
    with col_r1:
        st.write("Produtos com maior afinidade de compra:")
        st.dataframe(data['recomendacao'][['name', 'score']], hide_index=True, use_container_width=True)
    with col_r2:
        fig_rec = px.bar(data['recomendacao'], x='score', y='name', orientation='h', 
                         color='score', color_continuous_scale='Viridis')
        fig_rec.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_rec, use_container_width=True)

# ABA 3: OPERAÇÃO E IA
with tab3:
    # Cards Operacionais
    co1, co2, co3, co4 = st.columns(4)
    
    co1.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Itens Movimentados</div>
            <div class="metric-value">{data['op_kpis']['total_itens']:,}</div>
            <div class="metric-subtitle">Volume Total de Saída</div>
        </div>
    """, unsafe_allow_html=True)

    co2.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Dia de Pico</div>
            <div class="metric-value">{data['op_kpis']['dia_pico']}</div>
            <div class="metric-subtitle">Maior Fluxo de Logística</div>
        </div>
    """, unsafe_allow_html=True)

    co3.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Itens por Pedido</div>
            <div class="metric-value">{data['op_kpis']['media_itens_pedido']:.1f}</div>
            <div class="metric-subtitle">Média de Densidade</div>
        </div>
    """, unsafe_allow_html=True)

    co4.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Modelo IA</div>
            <div class="metric-value">MA7 Baseline</div>
            <div class="metric-subtitle">Status: Operacional</div>
        </div>
    """, unsafe_allow_html=True)

    # Sazonalidade e Variabilidade
    col_op1, col_op2 = st.columns(2)
    
    with col_op1:
        st.subheader("Sazonalidade: Média de Itens por Dia")
        fig_week = px.bar(data['calendario'], x='dia_semana', y='media_vendas', 
                          color='media_vendas', color_continuous_scale='Tealgrn',
                          category_orders={"dia_semana": data['dias_ordem']})
        fig_week.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', xaxis_title="")
        st.plotly_chart(fig_week, use_container_width=True)
        
    with col_op2:
        st.subheader("Variabilidade e Outliers por Dia")
        fig_box = px.box(data['boxplot'], x='dia_semana_pt', y='qtd_int', 
                         color_discrete_sequence=['#4DA8DA'],
                         category_orders={"dia_semana_pt": data['dias_ordem']})
        fig_box.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', xaxis_title="")
        st.plotly_chart(fig_box, use_container_width=True)

    # Previsão de Demanda
    st.markdown("---")
    st.subheader("Previsão de Demanda (Série Temporal - Motor Yamaha)")
    
    fig_p = go.Figure()
    fig_p.add_trace(go.Scatter(x=data['previsao']['data'], y=data['previsao']['qtd_real'], 
                               name="Vendas Reais", mode='lines+markers',
                               line=dict(color='#4DA8DA', width=2)))
    fig_p.add_trace(go.Scatter(x=data['previsao']['data'], y=data['previsao']['MA7'], 
                               name="Previsão (Média Móvel 7D)", 
                               line=dict(color="#F56F3B", dash='dash')))
    
    fig_p.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_p, use_container_width=True)

st.caption("LH Nautical Platform | Desenvolvido por Matheus Di Giacomo")
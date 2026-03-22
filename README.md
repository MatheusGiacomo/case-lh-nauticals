# ⚓ LH Nautical Business Intelligence

O **LH Nautical Business Intelligence** é uma solução de inteligência de negócios ponta a ponta desenvolvida para a **LH Nautical**. O projeto transforma dados brutos de vendas, clientes e produtos em insights acionáveis, utilizando uma arquitetura de dados moderna para garantir a qualidade (Data Quality) e aplicar modelos analíticos avançados de recomendação e previsão.

## 🏗️ Arquitetura do Projeto

O projeto segue a lógica de **Arquitetura de Medalhão** para organização e processamento dos dados:

* **📁 raw/**: Armazenamento dos dados brutos em formatos JSON e CSV (clientes, cotações, custos e vendas).
* **📁 silver/**: Camada de processamento onde os dados são limpos, normalizados e enriquecidos. Contém scripts Python especializados para cada domínio (câmbio, produtos, recomendações, etc.).

## 🖥️ O Dashboard: Interface & Experiência do Usuário

Desenvolvido inteiramente em **Streamlit**, o dashboard foi projetado para oferecer uma navegação intuitiva dividida em três pilares estratégicos, utilizando CSS customizado para criar cartões de métricas modernos e de alta legibilidade.

### 1. Aba Financeira: Auditoria e Performance
* **Visualização de KPIs:** Exibição clara do faturamento líquido, ticket médio e volume de transações.
* **Gráfico de Waterfall (Cascata):** Uma ferramenta visual de auditoria que demonstra o impacto da limpeza de dados. Ele destaca como a identificação de outliers (como o erro no ID 107) é fundamental para a precisão dos relatórios executivos.

### 2. Aba CRM: Inteligência de Vendas
* **Vitrine Inteligente:** Interface que apresenta recomendações de produtos baseadas em comportamento de compra.
* **Gráficos de Similaridade:** Utiliza barras horizontais coloridas (escala Viridis) para mostrar o score de similaridade (Cosseno) entre produtos, facilitando a identificação de oportunidades de cross-selling.

### 3. Aba Operações: Sazonalidade e IA
* **Análise de Fluxo:** Gráficos de barras que mapeiam a média de vendas por dia da semana, auxiliando no planejamento de escala da equipe operacional.
* **Previsão de Demanda:** Gráfico de linha interativo que sobrepõe os dados reais de venda de motores Yamaha à curva de previsão (Média Móvel de 7 dias), permitindo a detecção imediata de desvios na demanda.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.13
* **Interface/Dashboard:** Streamlit
* **Processamento de Dados:** Pandas / DuckDB
* **Machine Learning & Analytics:** * Similaridade de Cosseno (Sistemas de Recomendação)
    * Médias Móveis (Baseline MA7 para Previsão de Demanda)

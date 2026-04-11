# 💰 FinDash — Dashboard Financeiro Pessoal

Dashboard financeiro pessoal completo, com design premium, otimizado para acompanhamento diário.

## 🚀 Como Usar

### 1. Abrir o Dashboard
Basta abrir o arquivo `index.html` no seu navegador (recomendado: Chrome, Edge ou Firefox).

### 2. Registrar Transações
1. Clique em **"+ Nova Transação"**
2. Preencha: **Data**, **Descrição**, **Categoria**, **Tipo** (Receita/Despesa), **Valor**
3. Clique em **"Salvar"**
4. A transação aparece na tabela e os KPIs/gráficos atualizam automaticamente

### 3. Navegar entre Meses
Use os botões **◀ ▶** no topo para alternar entre meses.

### 4. Definir Metas de Gastos
1. No card "Metas de Gastos", clique em **"⚙️ Definir"**
2. Defina um limite mensal para cada categoria
3. Barras de progresso mostram quanto você já gastou:
   - 🟢 **Verde**: dentro do limite (< 80%)
   - 🟡 **Amarelo**: perto do limite (80-100%)
   - 🔴 **Vermelho**: limite ultrapassado (> 100%)

### 5. Abas
- **📊 Dashboard**: KPIs, gráficos e visão geral
- **📝 Transações**: Lista completa com busca e filtros

### 6. Exportar/Importar Dados
- **📤 Exportar**: Baixa um arquivo JSON com todos os seus dados
- **📥 Importar**: Carrega dados de um backup JSON

---

## 📊 KPIs Disponíveis

| Indicador | Descrição |
|-----------|-----------|
| **Saldo Mensal** | Receitas - Despesas do mês |
| **Receitas** | Total de entradas no mês |
| **Despesas** | Total de saídas no mês |
| **Economia** | % da receita que sobrou |
| **Investimentos** | Total em "Investimentos" |
| **Saúde Financeira** | Excelente (≥30%), Bom (≥15%), Atenção (≥0%), Crítico (<0%) |

---

## 📈 Gráficos

1. **Gastos por Categoria** — Gráfico de rosca
2. **Evolução do Saldo** — Linha ao longo do mês
3. **Receitas vs Despesas** — Barras comparativas dos últimos 6 meses

---

## 🏷️ Categorias

| Categoria | Tipo Sugerido |
|-----------|---------------|
| 🍽️ Alimentação | Despesa |
| 🏠 Aluguel | Despesa |
| 💼 Salário | Receita |
| 🎮 Lazer | Despesa |
| 🚗 Transporte | Despesa |
| 📄 Contas | Despesa |
| 🏥 Saúde | Despesa |
| 📚 Educação | Despesa |
| 📈 Investimentos | Despesa |
| 💻 Freelance | Receita |
| 📦 Outros | Ambos |

---

## 💡 Dicas de Melhoria Contínua

1. **Registre diariamente** — Um hábito de 2 minutos que transforma suas finanças
2. **Revise semanalmente** — Verifique os gráficos toda semana
3. **Ajuste metas mensalmente** — Metas realistas motivam mais
4. **Use a regra 50/30/20** — 50% necessidades, 30% desejos, 20% investimentos
5. **Exporte backups** — Faça backup dos dados regularmente

---

## 🛠️ Tecnologias

- HTML5 + CSS3 (design system customizado, tema escuro, glassmorphism)
- JavaScript ES6+ (lógica de negócio)
- Chart.js 4 (gráficos interativos)
- localStorage (persistência de dados)
- Google Fonts (Inter)

---

## 📂 Estrutura

```
dashboard_fin/
├── index.html      # Página principal
├── styles.css      # Design system + estilos
├── app.js          # Lógica da aplicação
└── README.md       # Este arquivo
```

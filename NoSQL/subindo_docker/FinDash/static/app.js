/* ================================================================
   FINDASH v2 — Dashboard Financeiro Pessoal
   Application Logic (API-driven + Recurring + Annual)
   ================================================================ */

// ========================
// CONSTANTS
// ========================
const CATEGORIES = [
  { name: 'Alimentação', icon: '🍽️', color: '#e17055' },
  { name: 'Aluguel', icon: '🏠', color: '#6c5ce7' },
  { name: 'Salário', icon: '💼', color: '#00b894' },
  { name: 'Lazer', icon: '🎮', color: '#fdcb6e' },
  { name: 'Transporte', icon: '🚗', color: '#0984e3' },
  { name: 'Contas', icon: '📄', color: '#e84393' },
  { name: 'Saúde', icon: '🏥', color: '#00cec9' },
  { name: 'Educação', icon: '📚', color: '#a29bfe' },
  { name: 'Investimentos', icon: '📈', color: '#55efc4' },
  { name: 'Freelance', icon: '💻', color: '#74b9ff' },
  { name: 'Outros', icon: '📦', color: '#636e72' },
];
const EXPENSE_CATEGORIES = CATEGORIES.filter(c => !['Salário', 'Freelance'].includes(c.name));
const MONTHS_PT = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'];

// ========================
// API CLIENT
// ========================
const API = {
  async req(url, opts = {}) {
    try {
      const res = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...opts });
      if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `Erro ${res.status}`); }
      return res.json();
    } catch (err) { showToast(err.message || 'Erro de conexão', 'error'); throw err; }
  },
  getMonthTx(y, m) { return this.req(`/api/transactions?year=${y}&month=${m + 1}`); },
  getAllTx() { return this.req('/api/transactions'); },
  createTx(d) { return this.req('/api/transactions', { method: 'POST', body: JSON.stringify(d) }); },
  updateTx(id, d) { return this.req(`/api/transactions/${id}`, { method: 'PUT', body: JSON.stringify(d) }); },
  deleteTx(id) { return this.req(`/api/transactions/${id}`, { method: 'DELETE' }); },
  getRecurring() { return this.req('/api/recurring'); },
  createRecurring(d) { return this.req('/api/recurring', { method: 'POST', body: JSON.stringify(d) }); },
  updateRecurring(id, d) { return this.req(`/api/recurring/${id}`, { method: 'PUT', body: JSON.stringify(d) }); },
  deleteRecurring(id) { return this.req(`/api/recurring/${id}`, { method: 'DELETE' }); },
  generateRecurring(y, m) { return this.req('/api/recurring/generate', { method: 'POST', body: JSON.stringify({ year: y, month: m + 1 }) }); },
  getGoals() { return this.req('/api/goals'); },
  saveGoals(g) { return this.req('/api/goals', { method: 'POST', body: JSON.stringify(g) }); },
  getAnnual(y) { return this.req(`/api/summary/annual?year=${y}`); },
  exportData() { return this.req('/api/export'); },
  importData(d) { return this.req('/api/import', { method: 'POST', body: JSON.stringify(d) }); },
};

// ========================
// STATE
// ========================
let currentMonth = new Date().getMonth();
let currentYear = new Date().getFullYear();
let currentAnnualYear = new Date().getFullYear();
let editingTransactionId = null;
let editingRecurringId = null;
let currentTab = 'dashboard';

// Chart instances
let categoryChart = null, balanceChart = null, comparisonChart = null;
let annualBarChart = null, annualBalanceChart = null, annualTopChart = null, annualEconomyChart = null;

// ========================
// UTILITIES
// ========================
function formatCurrency(v) { return v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }); }
function formatDate(ds) { const [y, m, d] = ds.split('-'); return `${d}/${m}/${y}`; }
function getTodayStr() { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
function getCategoryInfo(n) { return CATEGORIES.find(c => c.name === n) || CATEGORIES[CATEGORIES.length - 1]; }
function getDaysInMonth(y, m) { return new Date(y, m + 1, 0).getDate(); }

// ========================
// TOAST
// ========================
function showToast(msg, type = 'info') {
  const c = document.getElementById('toast-container');
  const icons = { success: '✅', info: 'ℹ️', warning: '⚠️', error: '❌' };
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.innerHTML = `<span>${icons[type] || ''}</span> ${msg}`;
  c.appendChild(t);
  setTimeout(() => t.remove(), 3200);
}

// ========================
// CHART DEFAULTS
// ========================
const chartOpts = {
  responsive: true, maintainAspectRatio: false,
  plugins: {
    legend: { labels: { color: '#a0a0b8', font: { family: 'Inter', size: 11 }, padding: 16, usePointStyle: true, pointStyleWidth: 8 } },
    tooltip: { backgroundColor: 'rgba(14,14,36,0.95)', titleColor: '#f0f0f8', bodyColor: '#a0a0b8', titleFont: { family: 'Inter', weight: '600' }, bodyFont: { family: 'Inter' }, padding: 12, cornerRadius: 8, borderColor: 'rgba(255,255,255,0.1)', borderWidth: 1 }
  }
};

// ========================
// RENDER KPIs
// ========================
function renderKPIs(monthTx) {
  const rec = monthTx.filter(t => t.type === 'receita').reduce((s, t) => s + t.value, 0);
  const desp = monthTx.filter(t => t.type === 'despesa').reduce((s, t) => s + t.value, 0);
  const saldo = rec - desp;
  const inv = monthTx.filter(t => t.category === 'Investimentos' && t.type === 'despesa').reduce((s, t) => s + t.value, 0);
  const eco = rec > 0 ? ((rec - desp) / rec) * 100 : 0;

  document.getElementById('kpi-saldo').textContent = formatCurrency(saldo);
  document.getElementById('kpi-saldo').style.color = saldo < 0 ? '#e17055' : '';
  document.getElementById('kpi-receitas').textContent = formatCurrency(rec);
  document.getElementById('kpi-despesas').textContent = formatCurrency(desp);
  document.getElementById('kpi-investimentos').textContent = formatCurrency(inv);
  document.getElementById('kpi-economia').textContent = eco.toFixed(1) + '%';

  const badge = document.getElementById('health-badge');
  const saudeEl = document.getElementById('kpi-saude');
  if (monthTx.length === 0) { saudeEl.textContent = '—'; badge.textContent = 'Sem dados'; badge.className = 'health-badge'; }
  else if (eco >= 30) { saudeEl.textContent = eco.toFixed(1)+'%'; badge.textContent = '🟢 Excelente'; badge.className = 'health-badge excelente'; }
  else if (eco >= 15) { saudeEl.textContent = eco.toFixed(1)+'%'; badge.textContent = '🔵 Bom'; badge.className = 'health-badge bom'; }
  else if (eco >= 0) { saudeEl.textContent = eco.toFixed(1)+'%'; badge.textContent = '🟡 Atenção'; badge.className = 'health-badge atencao'; }
  else { saudeEl.textContent = eco.toFixed(1)+'%'; badge.textContent = '🔴 Crítico'; badge.className = 'health-badge critico'; }
}

// ========================
// RENDER TABLES
// ========================
function renderRecentTransactions(monthTx) {
  const recent = monthTx.slice(-5).reverse();
  const tbody = document.getElementById('recent-transactions-body');
  if (!recent.length) {
    tbody.innerHTML = '<tr><td colspan="5"><div class="empty-state"><div class="empty-state-icon">📭</div><div class="empty-state-text">Nenhuma transação neste mês.</div></div></td></tr>';
    return;
  }
  tbody.innerHTML = recent.map(tx => {
    const c = getCategoryInfo(tx.category);
    return `<tr><td>${formatDate(tx.date)}</td><td>${tx.description}${tx.recurring_id ? ' 🔄' : ''}</td><td><span class="category-tag"><span class="category-dot" style="background:${c.color}"></span>${c.icon} ${tx.category}</span></td><td><span class="type-badge ${tx.type}">${tx.type === 'receita' ? '↑ Receita' : '↓ Despesa'}</span></td><td><span class="value-cell ${tx.type}">${formatCurrency(tx.value)}</span></td></tr>`;
  }).join('');
}

async function renderAllTransactions() {
  const searchTerm = document.getElementById('tx-search').value.toLowerCase();
  const filterCat = document.getElementById('tx-filter-category').value;
  const filterType = document.getElementById('tx-filter-type').value;
  let all;
  try { all = await API.getAllTx(); } catch { return; }

  let filtered = all;
  if (searchTerm) filtered = filtered.filter(t => t.description.toLowerCase().includes(searchTerm) || t.category.toLowerCase().includes(searchTerm));
  if (filterCat) filtered = filtered.filter(t => t.category === filterCat);
  if (filterType) filtered = filtered.filter(t => t.type === filterType);

  const tbody = document.getElementById('all-transactions-body');
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="6"><div class="empty-state"><div class="empty-state-icon">📭</div><div class="empty-state-text">Nenhuma transação encontrada.</div></div></td></tr>';
    return;
  }
  tbody.innerHTML = filtered.map(tx => {
    const c = getCategoryInfo(tx.category);
    return `<tr><td>${formatDate(tx.date)}</td><td>${tx.description}${tx.recurring_id ? ' 🔄' : ''}</td><td><span class="category-tag"><span class="category-dot" style="background:${c.color}"></span>${c.icon} ${tx.category}</span></td><td><span class="type-badge ${tx.type}">${tx.type === 'receita' ? '↑ Receita' : '↓ Despesa'}</span></td><td><span class="value-cell ${tx.type}">${formatCurrency(tx.value)}</span></td><td><div class="action-btns"><button class="action-btn" onclick="editTransaction('${tx.id}')" title="Editar">✏️</button><button class="action-btn delete" onclick="confirmDelete('${tx.id}')" title="Excluir">🗑️</button></div></td></tr>`;
  }).join('');
}

// ========================
// RENDER GOALS
// ========================
function renderGoals(monthTx, goalsData) {
  const despesas = monthTx.filter(t => t.type === 'despesa');
  const container = document.getElementById('goals-list');
  const active = goalsData.filter(g => g.limit_value > 0);
  if (!active.length) {
    container.innerHTML = '<div class="goals-empty"><div class="goals-empty-icon">🎯</div><div>Defina metas para acompanhar seus gastos</div></div>';
    return;
  }
  container.innerHTML = active.map(g => {
    const spent = despesas.filter(t => t.category === g.category).reduce((s, t) => s + t.value, 0);
    const pct = g.limit_value > 0 ? (spent / g.limit_value) * 100 : 0;
    const c = getCategoryInfo(g.category);
    let barColor, cls;
    if (pct >= 100) { barColor = '#e17055'; cls = 'danger'; }
    else if (pct >= 80) { barColor = '#fdcb6e'; cls = 'warning'; }
    else { barColor = '#00b894'; cls = 'ok'; }
    return `<div class="goal-item"><div class="goal-info"><span class="goal-category"><span class="category-dot" style="background:${c.color}"></span>${c.icon} ${g.category}</span><span class="goal-values">${formatCurrency(spent)} / ${formatCurrency(g.limit_value)}</span></div><div class="goal-bar-track"><div class="goal-bar-fill" style="width:${Math.min(pct,100)}%;background:${barColor}"></div></div><span class="goal-percent ${cls}">${pct.toFixed(0)}%${pct>=100?' ⚠️ Limite ultrapassado!':pct>=80?' ⚠️ Quase no limite':''}</span></div>`;
  }).join('');
}

// ========================
// MONTHLY CHARTS
// ========================
function destroyChart(c) { if (c) c.destroy(); return null; }

function updateCategoryChart(monthTx) {
  const despesas = monthTx.filter(t => t.type === 'despesa');
  const container = document.getElementById('category-chart-container');
  const grouped = {};
  despesas.forEach(t => { grouped[t.category] = (grouped[t.category] || 0) + t.value; });
  const labels = Object.keys(grouped), data = Object.values(grouped);
  if (!labels.length) { categoryChart = destroyChart(categoryChart); container.innerHTML = '<div class="chart-empty"><div class="chart-empty-icon">🍩</div><div>Sem despesas neste mês</div></div>'; return; }
  container.innerHTML = '<canvas id="category-chart"></canvas>';
  const ctx = document.getElementById('category-chart').getContext('2d');
  categoryChart = destroyChart(categoryChart);
  categoryChart = new Chart(ctx, { type: 'doughnut', data: { labels, datasets: [{ data, backgroundColor: labels.map(l => getCategoryInfo(l).color), borderColor: 'rgba(8,8,22,0.8)', borderWidth: 3, hoverOffset: 6 }] }, options: { ...chartOpts, cutout: '65%', plugins: { ...chartOpts.plugins, legend: { ...chartOpts.plugins.legend, position: 'bottom' }, tooltip: { ...chartOpts.plugins.tooltip, callbacks: { label: ctx => { const t = ctx.dataset.data.reduce((a,b)=>a+b,0); return ` ${ctx.label}: ${formatCurrency(ctx.parsed)} (${((ctx.parsed/t)*100).toFixed(1)}%)`; } } } } } });
}

function updateBalanceChart(monthTx) {
  const container = document.getElementById('balance-chart-container');
  const days = getDaysInMonth(currentYear, currentMonth);
  if (!monthTx.length) { balanceChart = destroyChart(balanceChart); container.innerHTML = '<div class="chart-empty"><div class="chart-empty-icon">📉</div><div>Sem dados neste mês</div></div>'; return; }
  container.innerHTML = '<canvas id="balance-chart"></canvas>';
  const ctx = document.getElementById('balance-chart').getContext('2d');
  balanceChart = destroyChart(balanceChart);
  const labels = [], balData = []; let cum = 0;
  for (let d = 1; d <= days; d++) {
    const ds = `${currentYear}-${String(currentMonth+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    monthTx.filter(t => t.date === ds).forEach(t => { cum += t.type === 'receita' ? t.value : -t.value; });
    labels.push(d); balData.push(cum);
  }
  const grad = ctx.createLinearGradient(0,0,0,280); grad.addColorStop(0,'rgba(108,92,231,0.3)'); grad.addColorStop(1,'rgba(108,92,231,0)');
  balanceChart = new Chart(ctx, { type: 'line', data: { labels, datasets: [{ label: 'Saldo', data: balData, borderColor: '#6c5ce7', backgroundColor: grad, fill: true, tension: 0.4, pointRadius: 0, pointHitRadius: 10, pointHoverRadius: 5, pointHoverBackgroundColor: '#6c5ce7', borderWidth: 2.5 }] }, options: { ...chartOpts, scales: { x: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', font: { family: 'Inter', size: 10 }, maxTicksLimit: 10 } }, y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', font: { family: 'Inter', size: 10 }, callback: v => formatCurrency(v) } } }, plugins: { ...chartOpts.plugins, legend: { display: false }, tooltip: { ...chartOpts.plugins.tooltip, callbacks: { title: i => `Dia ${i[0].label}`, label: ctx => ` Saldo: ${formatCurrency(ctx.parsed.y)}` } } } } });
}

async function updateComparisonChart() {
  const container = document.getElementById('comparison-chart-container');
  const fetches = [];
  for (let i = 5; i >= 0; i--) {
    let m = currentMonth - i, y = currentYear;
    if (m < 0) { m += 12; y--; }
    const mm = m, yy = y;
    fetches.push(API.getMonthTx(yy, mm).then(tx => ({
      label: MONTHS_PT[mm].slice(0, 3),
      receitas: tx.filter(t => t.type === 'receita').reduce((s, t) => s + t.value, 0),
      despesas: tx.filter(t => t.type === 'despesa').reduce((s, t) => s + t.value, 0),
    })).catch(() => ({ label: MONTHS_PT[mm].slice(0,3), receitas: 0, despesas: 0 })));
  }
  const data = await Promise.all(fetches);
  if (!data.some(d => d.receitas > 0 || d.despesas > 0)) { comparisonChart = destroyChart(comparisonChart); container.innerHTML = '<div class="chart-empty"><div class="chart-empty-icon">📊</div><div>Sem dados para comparação</div></div>'; return; }
  container.innerHTML = '<canvas id="comparison-chart"></canvas>';
  const ctx = document.getElementById('comparison-chart').getContext('2d');
  comparisonChart = destroyChart(comparisonChart);
  comparisonChart = new Chart(ctx, { type: 'bar', data: { labels: data.map(d => d.label), datasets: [{ label: 'Receitas', data: data.map(d => d.receitas), backgroundColor: 'rgba(0,184,148,0.7)', hoverBackgroundColor: 'rgba(0,184,148,0.9)', borderRadius: 6, maxBarThickness: 28 }, { label: 'Despesas', data: data.map(d => d.despesas), backgroundColor: 'rgba(225,112,85,0.7)', hoverBackgroundColor: 'rgba(225,112,85,0.9)', borderRadius: 6, maxBarThickness: 28 }] }, options: { ...chartOpts, scales: { x: { grid: { display: false }, ticks: { color: '#55557a', font: { family: 'Inter', size: 11 } } }, y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', font: { family: 'Inter', size: 10 }, callback: v => formatCurrency(v) } } }, plugins: { ...chartOpts.plugins, legend: { ...chartOpts.plugins.legend, position: 'top' }, tooltip: { ...chartOpts.plugins.tooltip, callbacks: { label: ctx => ` ${ctx.dataset.label}: ${formatCurrency(ctx.parsed.y)}` } } } } });
}

// ========================
// DASHBOARD RENDER
// ========================
async function renderDashboard() {
  try {
    const [monthTx, goalsData] = await Promise.all([API.getMonthTx(currentYear, currentMonth), API.getGoals()]);
    renderKPIs(monthTx);
    renderRecentTransactions(monthTx);
    renderGoals(monthTx, goalsData);
    updateCategoryChart(monthTx);
    updateBalanceChart(monthTx);
    await updateComparisonChart();
  } catch (e) { console.error('Dashboard render error:', e); }
}

// ========================
// MONTH / YEAR NAVIGATION
// ========================
function updateMonthLabel() { document.getElementById('month-label').textContent = `${MONTHS_PT[currentMonth]} ${currentYear}`; }
function updateYearLabel() { document.getElementById('year-label').textContent = currentAnnualYear; }

function changeMonth(delta) {
  currentMonth += delta;
  if (currentMonth > 11) { currentMonth = 0; currentYear++; }
  if (currentMonth < 0) { currentMonth = 11; currentYear--; }
  updateMonthLabel();
  renderDashboard();
}

function changeYear(delta) {
  currentAnnualYear += delta;
  updateYearLabel();
  renderAnnualSummary();
}

// ========================
// TAB SWITCHING
// ========================
function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
  document.getElementById(`tab-${tab}`).classList.add('active');

  // Show month nav or year nav
  document.getElementById('month-nav-container').style.display = tab === 'annual' ? 'none' : '';
  document.getElementById('year-nav-container').style.display = tab === 'annual' ? '' : 'none';

  if (tab === 'transactions') renderAllTransactions();
  if (tab === 'annual') renderAnnualSummary();
}

// ========================
// TRANSACTION MODAL
// ========================
function openTransactionModal(txId = null) {
  editingTransactionId = txId;
  const form = document.getElementById('transaction-form');
  form.reset();
  document.getElementById('tx-date').value = getTodayStr();
  if (txId) {
    API.getAllTx().then(all => {
      const tx = all.find(t => t.id === txId);
      if (tx) {
        document.getElementById('modal-tx-title').textContent = '✏️ Editar Transação';
        document.getElementById('tx-date').value = tx.date;
        document.getElementById('tx-description').value = tx.description;
        document.getElementById('tx-category').value = tx.category;
        document.getElementById('tx-type').value = tx.type;
        document.getElementById('tx-value').value = tx.value;
      }
    });
  } else { document.getElementById('modal-tx-title').textContent = '➕ Nova Transação'; }
  document.getElementById('transaction-modal').classList.add('active');
}
function closeTransactionModal() { document.getElementById('transaction-modal').classList.remove('active'); editingTransactionId = null; }

async function handleTransactionSubmit(e) {
  e.preventDefault();
  const data = { date: document.getElementById('tx-date').value, description: document.getElementById('tx-description').value.trim(), category: document.getElementById('tx-category').value, type: document.getElementById('tx-type').value, value: parseFloat(document.getElementById('tx-value').value) };
  if (!data.description || !data.category || !data.value || data.value <= 0) { showToast('Preencha todos os campos.', 'warning'); return; }
  try {
    if (editingTransactionId) { await API.updateTx(editingTransactionId, data); showToast('Transação atualizada!', 'success'); }
    else { await API.createTx(data); showToast('Transação adicionada!', 'success'); }
    closeTransactionModal();
    renderDashboard();
    if (currentTab === 'transactions') renderAllTransactions();
  } catch { /* error shown by API */ }
}

function editTransaction(id) { openTransactionModal(id); }
async function confirmDelete(id) {
  if (!confirm('Excluir esta transação?')) return;
  try { await API.deleteTx(id); showToast('Transação excluída.', 'info'); renderDashboard(); if (currentTab === 'transactions') renderAllTransactions(); } catch {}
}

// ========================
// GOALS MODAL
// ========================
async function openGoalsModal() {
  let goalsData = [];
  try { goalsData = await API.getGoals(); } catch {}
  const body = document.getElementById('goals-form-body');
  body.innerHTML = EXPENSE_CATEGORIES.map(cat => {
    const existing = goalsData.find(g => g.category === cat.name);
    return `<div class="goal-form-item"><div class="goal-form-category"><span class="category-dot" style="background:${cat.color}"></span>${cat.icon} ${cat.name}</div><div class="goal-form-input"><input type="number" step="0.01" min="0" placeholder="Sem limite" data-category="${cat.name}" value="${existing ? existing.limit_value : ''}"></div></div>`;
  }).join('');
  document.getElementById('goals-modal').classList.add('active');
}
function closeGoalsModal() { document.getElementById('goals-modal').classList.remove('active'); }
async function saveGoalsFromModal() {
  const inputs = document.querySelectorAll('#goals-form-body input[data-category]');
  const goals = [];
  inputs.forEach(i => { const v = parseFloat(i.value); if (v > 0) goals.push({ category: i.dataset.category, limit_value: v }); });
  try { await API.saveGoals(goals); closeGoalsModal(); renderDashboard(); showToast('Metas salvas!', 'success'); } catch {}
}

// ========================
// RECURRING TRANSACTIONS
// ========================
async function openRecurringModal() {
  let list = [];
  try { list = await API.getRecurring(); } catch {}
  const container = document.getElementById('recurring-list');
  if (!list.length) { container.innerHTML = '<div class="recurring-empty">🔄 Nenhuma transação recorrente configurada.</div>'; }
  else {
    container.innerHTML = '<div class="recurring-list-items">' + list.map(r => {
      const c = getCategoryInfo(r.category);
      return `<div class="recurring-item ${r.active ? '' : 'inactive'}"><div class="recurring-info"><div class="recurring-main"><span class="category-tag"><span class="category-dot" style="background:${c.color}"></span>${c.icon} ${r.category}</span> ${r.description}</div><div class="recurring-details"><span class="type-badge ${r.type}">${r.type === 'receita' ? '↑ Receita' : '↓ Despesa'}</span><span class="recurring-day">Dia ${r.day_of_month}</span><span class="recurring-value value-cell ${r.type}">${formatCurrency(r.value)}</span></div></div><div class="recurring-actions"><label class="toggle"><input type="checkbox" ${r.active?'checked':''} onchange="toggleRecurring('${r.id}', this.checked)"><span class="toggle-slider"></span></label><button class="action-btn" onclick="editRecurring('${r.id}')" title="Editar">✏️</button><button class="action-btn delete" onclick="confirmDeleteRecurring('${r.id}')" title="Excluir">🗑️</button></div></div>`;
    }).join('') + '</div>';
  }
  document.getElementById('recurring-modal').classList.add('active');
}
function closeRecurringModal() { document.getElementById('recurring-modal').classList.remove('active'); }

function openRecurringForm(recId = null) {
  editingRecurringId = recId;
  const form = document.getElementById('recurring-form');
  form.reset();
  if (recId) {
    API.getRecurring().then(list => {
      const r = list.find(x => x.id === recId);
      if (r) {
        document.getElementById('modal-rec-form-title').textContent = '✏️ Editar Recorrente';
        document.getElementById('rec-description').value = r.description;
        document.getElementById('rec-category').value = r.category;
        document.getElementById('rec-type').value = r.type;
        document.getElementById('rec-value').value = r.value;
        document.getElementById('rec-day').value = r.day_of_month;
      }
    });
  } else { document.getElementById('modal-rec-form-title').textContent = '➕ Nova Recorrente'; }
  document.getElementById('recurring-form-modal').classList.add('active');
}
function closeRecurringForm() { document.getElementById('recurring-form-modal').classList.remove('active'); editingRecurringId = null; }

async function handleRecurringSubmit(e) {
  e.preventDefault();
  const data = { description: document.getElementById('rec-description').value.trim(), category: document.getElementById('rec-category').value, type: document.getElementById('rec-type').value, value: parseFloat(document.getElementById('rec-value').value), day_of_month: parseInt(document.getElementById('rec-day').value) };
  if (!data.description || !data.category || !data.value) { showToast('Preencha todos os campos.', 'warning'); return; }
  try {
    if (editingRecurringId) { await API.updateRecurring(editingRecurringId, data); showToast('Recorrente atualizada!', 'success'); }
    else { await API.createRecurring(data); showToast('Recorrente criada!', 'success'); }
    closeRecurringForm();
    openRecurringModal(); // refresh list
  } catch {}
}

async function toggleRecurring(id, active) {
  try { await API.updateRecurring(id, { active: active ? 1 : 0 }); } catch {}
}
function editRecurring(id) { openRecurringForm(id); }
async function confirmDeleteRecurring(id) {
  if (!confirm('Excluir esta recorrente?')) return;
  try { await API.deleteRecurring(id); showToast('Recorrente excluída.', 'info'); openRecurringModal(); } catch {}
}
async function generateRecurringForMonth() {
  try {
    const res = await API.generateRecurring(currentYear, currentMonth);
    if (res.count > 0) { showToast(`${res.count} transação(ões) gerada(s)!`, 'success'); renderDashboard(); openRecurringModal(); }
    else { showToast('Todas as recorrentes já foram geradas para este mês.', 'info'); }
  } catch {}
}

// ========================
// ANNUAL SUMMARY
// ========================
async function renderAnnualSummary() {
  let data;
  try { data = await API.getAnnual(currentAnnualYear); } catch { return; }

  document.getElementById('annual-receitas').textContent = formatCurrency(data.total_receitas);
  document.getElementById('annual-despesas').textContent = formatCurrency(data.total_despesas);
  document.getElementById('annual-saldo').textContent = formatCurrency(data.saldo_anual);
  document.getElementById('annual-saldo').style.color = data.saldo_anual < 0 ? '#e17055' : '';
  document.getElementById('annual-economia').textContent = data.media_economia + '%';

  // Summary table
  const tbody = document.getElementById('annual-summary-body');
  tbody.innerHTML = data.months.map(m => {
    const cls = m.saldo >= 0 ? 'annual-row-positive' : 'annual-row-negative';
    return `<tr class="${cls}"><td>${m.name}</td><td><span class="value-cell receita">${formatCurrency(m.receitas)}</span></td><td><span class="value-cell despesa">${formatCurrency(m.despesas)}</span></td><td><span class="value-cell">${formatCurrency(m.saldo)}</span></td><td>${m.economia}%</td></tr>`;
  }).join('');

  renderAnnualCharts(data);
}

function renderAnnualCharts(data) {
  const hasData = data.months.some(m => m.receitas > 0 || m.despesas > 0);

  // Bar chart: monthly income vs expenses
  const barC = document.getElementById('annual-bar-container');
  barC.innerHTML = '<canvas id="annual-bar-chart"></canvas>';
  annualBarChart = destroyChart(annualBarChart);
  if (hasData) {
    annualBarChart = new Chart(document.getElementById('annual-bar-chart').getContext('2d'), { type: 'bar', data: { labels: data.months.map(m => m.name.slice(0,3)), datasets: [{ label: 'Receitas', data: data.months.map(m => m.receitas), backgroundColor: 'rgba(0,184,148,0.7)', borderRadius: 4, maxBarThickness: 20 }, { label: 'Despesas', data: data.months.map(m => m.despesas), backgroundColor: 'rgba(225,112,85,0.7)', borderRadius: 4, maxBarThickness: 20 }] }, options: { ...chartOpts, scales: { x: { grid: { display: false }, ticks: { color: '#55557a', font: { family: 'Inter', size: 10 } } }, y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', font: { family: 'Inter', size: 10 }, callback: v => formatCurrency(v) } } } } });
  }

  // Line chart: cumulative balance
  const balC = document.getElementById('annual-balance-container');
  balC.innerHTML = '<canvas id="annual-balance-chart"></canvas>';
  annualBalanceChart = destroyChart(annualBalanceChart);
  if (hasData) {
    let cum = 0;
    const cumData = data.months.map(m => { cum += m.saldo; return cum; });
    const ctx2 = document.getElementById('annual-balance-chart').getContext('2d');
    const g = ctx2.createLinearGradient(0,0,0,280); g.addColorStop(0,'rgba(0,206,201,0.3)'); g.addColorStop(1,'rgba(0,206,201,0)');
    annualBalanceChart = new Chart(ctx2, { type: 'line', data: { labels: data.months.map(m => m.name.slice(0,3)), datasets: [{ label: 'Saldo Acumulado', data: cumData, borderColor: '#00cec9', backgroundColor: g, fill: true, tension: 0.4, pointRadius: 3, pointBackgroundColor: '#00cec9', borderWidth: 2.5 }] }, options: { ...chartOpts, scales: { x: { grid: { display: false }, ticks: { color: '#55557a' } }, y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', callback: v => formatCurrency(v) } } }, plugins: { ...chartOpts.plugins, legend: { display: false } } } });
  }

  // Horizontal bar: top 5 categories
  const topC = document.getElementById('annual-top-container');
  topC.innerHTML = '<canvas id="annual-top-chart"></canvas>';
  annualTopChart = destroyChart(annualTopChart);
  if (data.top_categories.length) {
    annualTopChart = new Chart(document.getElementById('annual-top-chart').getContext('2d'), { type: 'bar', data: { labels: data.top_categories.map(c => c.category), datasets: [{ data: data.top_categories.map(c => c.total), backgroundColor: data.top_categories.map(c => getCategoryInfo(c.category).color + 'cc'), borderRadius: 4, maxBarThickness: 24 }] }, options: { ...chartOpts, indexAxis: 'y', scales: { x: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', callback: v => formatCurrency(v) } }, y: { grid: { display: false }, ticks: { color: '#a0a0b8', font: { family: 'Inter', size: 11 } } } }, plugins: { ...chartOpts.plugins, legend: { display: false } } } });
  }

  // Line: economy trend
  const ecoC = document.getElementById('annual-economy-container');
  ecoC.innerHTML = '<canvas id="annual-economy-chart"></canvas>';
  annualEconomyChart = destroyChart(annualEconomyChart);
  if (hasData) {
    annualEconomyChart = new Chart(document.getElementById('annual-economy-chart').getContext('2d'), { type: 'line', data: { labels: data.months.map(m => m.name.slice(0,3)), datasets: [{ label: 'Economia %', data: data.months.map(m => m.economia), borderColor: '#fdcb6e', backgroundColor: 'rgba(253,203,110,0.1)', fill: true, tension: 0.4, pointRadius: 3, pointBackgroundColor: '#fdcb6e', borderWidth: 2 }] }, options: { ...chartOpts, scales: { x: { grid: { display: false }, ticks: { color: '#55557a' } }, y: { grid: { color: 'rgba(255,255,255,0.03)' }, ticks: { color: '#55557a', callback: v => v + '%' } } }, plugins: { ...chartOpts.plugins, legend: { display: false } } } });
  }
}

// ========================
// EXPORT / IMPORT / MIGRATE
// ========================
async function exportData() {
  try {
    const data = await API.exportData();
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
    a.download = `findash_backup_${getTodayStr()}.json`; document.body.appendChild(a); a.click(); document.body.removeChild(a);
    showToast('Dados exportados!', 'success');
  } catch {}
}

function importDataFromFile(file) {
  const reader = new FileReader();
  reader.onload = async (e) => {
    try {
      const data = JSON.parse(e.target.result);
      await API.importData(data);
      showToast('Dados importados com sucesso!', 'success');
      renderDashboard(); if (currentTab === 'transactions') renderAllTransactions();
    } catch { showToast('Erro ao importar arquivo.', 'error'); }
  };
  reader.readAsText(file);
}

function checkLocalStorageMigration() {
  const localTx = localStorage.getItem('findash_transactions');
  const localGoals = localStorage.getItem('findash_goals');
  if (!localTx && !localGoals) return;
  const txArr = localTx ? JSON.parse(localTx) : [];
  const goalsArr = localGoals ? JSON.parse(localGoals) : [];
  if (txArr.length === 0 && goalsArr.length === 0) return;
  const banner = document.getElementById('migration-banner');
  banner.style.display = '';
  document.getElementById('btn-migrate').onclick = async () => {
    try {
      const importGoals = goalsArr.map(g => ({ category: g.category, limit_value: g.limit || g.limit_value }));
      await API.importData({ transactions: txArr, goals: importGoals });
      localStorage.removeItem('findash_transactions');
      localStorage.removeItem('findash_goals');
      banner.style.display = 'none';
      showToast(`${txArr.length} transações migradas com sucesso!`, 'success');
      renderDashboard();
    } catch {}
  };
  document.getElementById('btn-dismiss-migrate').onclick = () => {
    localStorage.removeItem('findash_transactions');
    localStorage.removeItem('findash_goals');
    banner.style.display = 'none';
  };
}

// ========================
// POPULATE SELECTS
// ========================
function populateCategorySelects() {
  const opts = CATEGORIES.map(c => `<option value="${c.name}">${c.icon} ${c.name}</option>`).join('');
  document.getElementById('tx-category').innerHTML = '<option value="">Selecione...</option>' + opts;
  document.getElementById('rec-category').innerHTML = '<option value="">Selecione...</option>' + opts;
  document.getElementById('tx-filter-category').innerHTML = '<option value="">Todas as Categorias</option>' + opts;
}

// ========================
// EVENT LISTENERS
// ========================
function setupEventListeners() {
  document.getElementById('prev-month').addEventListener('click', () => changeMonth(-1));
  document.getElementById('next-month').addEventListener('click', () => changeMonth(1));
  document.getElementById('prev-year').addEventListener('click', () => changeYear(-1));
  document.getElementById('next-year').addEventListener('click', () => changeYear(1));
  document.querySelectorAll('.tab-btn').forEach(b => b.addEventListener('click', () => switchTab(b.dataset.tab)));

  // Transaction
  document.getElementById('btn-add-from-dashboard').addEventListener('click', () => openTransactionModal());
  document.getElementById('btn-add-transaction').addEventListener('click', () => openTransactionModal());
  document.getElementById('transaction-form').addEventListener('submit', handleTransactionSubmit);
  document.getElementById('modal-tx-close').addEventListener('click', closeTransactionModal);
  document.getElementById('modal-tx-cancel').addEventListener('click', closeTransactionModal);
  document.getElementById('transaction-modal').addEventListener('click', e => { if (e.target === e.currentTarget) closeTransactionModal(); });

  // Goals
  document.getElementById('btn-set-goals').addEventListener('click', openGoalsModal);
  document.getElementById('modal-goals-close').addEventListener('click', closeGoalsModal);
  document.getElementById('modal-goals-cancel').addEventListener('click', closeGoalsModal);
  document.getElementById('modal-goals-save').addEventListener('click', saveGoalsFromModal);
  document.getElementById('goals-modal').addEventListener('click', e => { if (e.target === e.currentTarget) closeGoalsModal(); });

  // Recurring
  document.getElementById('btn-recurring').addEventListener('click', openRecurringModal);
  document.getElementById('modal-recurring-close').addEventListener('click', closeRecurringModal);
  document.getElementById('recurring-modal').addEventListener('click', e => { if (e.target === e.currentTarget) closeRecurringModal(); });
  document.getElementById('btn-add-recurring').addEventListener('click', () => openRecurringForm());
  document.getElementById('btn-generate-recurring').addEventListener('click', generateRecurringForMonth);
  document.getElementById('recurring-form').addEventListener('submit', handleRecurringSubmit);
  document.getElementById('modal-rec-form-close').addEventListener('click', closeRecurringForm);
  document.getElementById('modal-rec-form-cancel').addEventListener('click', closeRecurringForm);
  document.getElementById('recurring-form-modal').addEventListener('click', e => { if (e.target === e.currentTarget) closeRecurringForm(); });

  // Search & filters
  document.getElementById('tx-search').addEventListener('input', renderAllTransactions);
  document.getElementById('tx-filter-category').addEventListener('change', renderAllTransactions);
  document.getElementById('tx-filter-type').addEventListener('change', renderAllTransactions);

  // Export / Import
  document.getElementById('btn-export').addEventListener('click', exportData);
  document.getElementById('btn-import').addEventListener('click', () => document.getElementById('import-file').click());
  document.getElementById('import-file').addEventListener('change', e => { if (e.target.files.length) { importDataFromFile(e.target.files[0]); e.target.value = ''; } });

  // ESC key
  document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeTransactionModal(); closeGoalsModal(); closeRecurringModal(); closeRecurringForm(); } });
}

// ========================
// INIT
// ========================
async function init() {
  populateCategorySelects();
  updateMonthLabel();
  updateYearLabel();
  setupEventListeners();
  checkLocalStorageMigration();
  await renderDashboard();
}

document.addEventListener('DOMContentLoaded', init);

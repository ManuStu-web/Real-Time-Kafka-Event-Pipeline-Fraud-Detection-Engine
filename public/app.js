// Real-Time Kafka Pipeline Frontend Application

const state = {
  metrics: {},
  orders: [],
  dlq: [],
  health: {},
  currentTab: 'orders',
  searchTerm: '',
  statusFilter: 'ALL',
  isPolling: true,
  pollInterval: null
};

// DOM Elements
const elements = {
  // Status pills
  systemStatus: document.getElementById('systemStatus'),
  executionMode: document.getElementById('executionMode'),
  brokerInfo: document.getElementById('brokerInfo'),
  
  // KPIs
  kpiTotalOrders: document.getElementById('kpiTotalOrders'),
  kpiTotalVolume: document.getElementById('kpiTotalVolume'),
  kpiAvgValue: document.getElementById('kpiAvgValue'),
  kpiVerified: document.getElementById('kpiVerified'),
  kpiFlagged: document.getElementById('kpiFlagged'),
  kpiDLQ: document.getElementById('kpiDLQ'),
  
  // Partitions
  p0Fill: document.getElementById('p0Fill'),
  p0Percent: document.getElementById('p0Percent'),
  p1Fill: document.getElementById('p1Fill'),
  p1Percent: document.getElementById('p1Percent'),
  p2Fill: document.getElementById('p2Fill'),
  p2Percent: document.getElementById('p2Percent'),
  
  // Tables & Tabs
  ordersTableBody: document.getElementById('ordersTableBody'),
  dlqTableBody: document.getElementById('dlqTableBody'),
  ordersTableWrapper: document.getElementById('ordersTableWrapper'),
  dlqTableWrapper: document.getElementById('dlqTableWrapper'),
  tabOrders: document.getElementById('tabOrders'),
  tabDLQ: document.getElementById('tabDLQ'),
  searchInput: document.getElementById('searchInput'),
  statusFilterSelect: document.getElementById('statusFilterSelect'),
  
  // Forms & Actions
  customOrderForm: document.getElementById('customOrderForm'),
  togglePollingBtn: document.getElementById('togglePollingBtn'),
  btnStream5: document.getElementById('btnStream5'),
  btnStream15: document.getElementById('btnStream15'),
  btnStreamSpike: document.getElementById('btnStreamSpike'),
  btnSendCorrupt: document.getElementById('btnSendCorrupt'),
  
  // Modal
  modalBackdrop: document.getElementById('modalBackdrop'),
  modalCloseBtn: document.getElementById('modalCloseBtn'),
  modalTitle: document.getElementById('modalTitle'),
  modalJsonViewer: document.getElementById('modalJsonViewer'),
  
  // Toast
  toastContainer: document.getElementById('toastContainer')
};

// Initialize Dashboard
document.addEventListener('DOMContentLoaded', () => {
  initEventListeners();
  fetchData();
  startPolling();
});

function initEventListeners() {
  // Tab switching
  elements.tabOrders.addEventListener('click', () => switchTab('orders'));
  elements.tabDLQ.addEventListener('click', () => switchTab('dlq'));

  // Search & Filter
  elements.searchInput.addEventListener('input', (e) => {
    state.searchTerm = e.target.value.toLowerCase().trim();
    renderTables();
  });

  elements.statusFilterSelect.addEventListener('change', (e) => {
    state.statusFilter = e.target.value;
    renderTables();
  });

  // Toggle Polling
  elements.togglePollingBtn.addEventListener('click', () => {
    state.isPolling = !state.isPolling;
    if (state.isPolling) {
      elements.togglePollingBtn.innerHTML = '⏸️ Pause Polling';
      startPolling();
      showToast('Live stream polling resumed', 'success');
    } else {
      elements.togglePollingBtn.innerHTML = '▶️ Resume Polling';
      clearInterval(state.pollInterval);
      showToast('Live stream polling paused');
    }
  });

  // Stream generator presets
  elements.btnStream5.addEventListener('click', () => triggerBatchStream(5, 200, 0.2));
  elements.btnStream15.addEventListener('click', () => triggerBatchStream(15, 250, 0.35));
  elements.btnStreamSpike.addEventListener('click', () => triggerBatchStream(8, 200, 0.85));

  // Send Corrupt payload to test DLQ
  elements.btnSendCorrupt.addEventListener('click', sendCorruptPayload);

  // Custom Order Form Submission
  elements.customOrderForm.addEventListener('submit', handleCustomOrderSubmit);

  // Modal close
  elements.modalCloseBtn.addEventListener('click', closeModal);
  elements.modalBackdrop.addEventListener('click', (e) => {
    if (e.target === elements.modalBackdrop) closeModal();
  });
}

function startPolling() {
  clearInterval(state.pollInterval);
  state.pollInterval = setInterval(fetchData, 1500);
}

// Fetch all pipeline telemetry from Express API
async function fetchData() {
  try {
    const [healthRes, metricsRes, ordersRes, dlqRes] = await Promise.all([
      fetch('/health'),
      fetch('/api/metrics'),
      fetch('/api/orders?limit=100'),
      fetch('/api/dlq?limit=50')
    ]);

    if (healthRes.ok) state.health = await healthRes.json();
    if (metricsRes.ok) state.metrics = await metricsRes.json();
    if (ordersRes.ok) {
      const data = await ordersRes.json();
      state.orders = data.orders || [];
    }
    if (dlqRes.ok) {
      const data = await dlqRes.json();
      state.dlq = data.records || [];
    }

    renderKPIs();
    renderPartitionDistribution();
    renderTables();
  } catch (err) {
    console.warn('[Dashboard Polling Error]', err);
  }
}

// Render Top KPI Cards and System Status
function renderKPIs() {
  // System Status Pill
  if (state.health.status === 'UP') {
    elements.systemStatus.innerHTML = '<span class="pulse-dot"></span> Pipeline: Healthy';
  } else {
    elements.systemStatus.innerHTML = '<span style="color:#ef4444">● Offline</span>';
  }

  elements.executionMode.textContent = `Mode: ${(state.health.executionMode || 'MOCK').toUpperCase()}`;
  elements.brokerInfo.textContent = `Broker: ${state.health.broker || 'localhost:9092'}`;

  // KPI Metrics
  const m = state.metrics;
  elements.kpiTotalOrders.textContent = (m.totalProcessed || 0).toLocaleString();
  elements.kpiTotalVolume.textContent = `$${(m.totalVolumeUsd || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  elements.kpiAvgValue.textContent = `$${(m.averageOrderValue || 0).toFixed(2)}`;
  elements.kpiVerified.textContent = (m.verifiedOrdersCount || 0).toLocaleString();
  elements.kpiFlagged.textContent = (m.flaggedOrdersCount || 0).toLocaleString();
  elements.kpiDLQ.textContent = (m.dlqCount || 0).toLocaleString();
}

// Calculate and render partition distribution across Partitions 0, 1, and 2
function renderPartitionDistribution() {
  const counts = { p0: 0, p1: 0, p2: 0 };
  
  state.orders.forEach(o => {
    // Partition is computed based on hash(userId) % 3
    const hash = Math.abs(hashString(o.userId || '')) % 3;
    if (hash === 0) counts.p0++;
    else if (hash === 1) counts.p1++;
    else counts.p2++;
  });

  const total = counts.p0 + counts.p1 + counts.p2 || 1;
  const p0Pct = Math.round((counts.p0 / total) * 100);
  const p1Pct = Math.round((counts.p1 / total) * 100);
  const p2Pct = 100 - p0Pct - p1Pct;

  elements.p0Fill.style.width = `${p0Pct}%`;
  elements.p0Percent.textContent = `${p0Pct}% (${counts.p0})`;

  elements.p1Fill.style.width = `${p1Pct}%`;
  elements.p1Percent.textContent = `${p1Pct}% (${counts.p1})`;

  elements.p2Fill.style.width = `${p2Pct}%`;
  elements.p2Percent.textContent = `${p2Pct}% (${counts.p2})`;
}

// Render Orders Table or DLQ Table based on active tab and filters
function renderTables() {
  if (state.currentTab === 'orders') {
    renderOrdersTable();
  } else {
    renderDLQTable();
  }
}

function renderOrdersTable() {
  let filtered = state.orders;

  // Search filter
  if (state.searchTerm) {
    filtered = filtered.filter(o => 
      (o.orderId && o.orderId.toLowerCase().includes(state.searchTerm)) ||
      (o.userId && o.userId.toLowerCase().includes(state.searchTerm)) ||
      (o.productName && o.productName.toLowerCase().includes(state.searchTerm))
    );
  }

  // Status filter
  if (state.statusFilter !== 'ALL') {
    filtered = filtered.filter(o => o.status === state.statusFilter);
  }

  if (filtered.length === 0) {
    elements.ordersTableBody.innerHTML = `
      <tr>
        <td colspan="8" style="text-align: center; color: var(--text-dim); padding: 2rem;">
          No order events found. Click "Stream 5 Orders" above to simulate real-time traffic!
        </td>
      </tr>
    `;
    return;
  }

  elements.ordersTableBody.innerHTML = filtered.map(order => {
    const isFlagged = order.status && order.status.startsWith('FLAGGED');
    const isReview = order.status === 'REQUIRES_REVIEW';
    const badgeClass = isFlagged ? 'badge-flagged' : (isReview ? 'badge-review' : 'badge-verified');
    const partitionNum = Math.abs(hashString(order.userId || '')) % 3;
    const timeFormatted = order.timestamp ? new Date(order.timestamp).toLocaleTimeString() : 'Just now';

    return `
      <tr style="cursor: pointer;" onclick="openOrderModal('${order.orderId}')">
        <td style="font-family: monospace; color: var(--border-highlight);">${order.orderId ? order.orderId.substring(0, 8) : 'N/A'}...</td>
        <td style="font-weight: 600;">${escapeHtml(order.userId || 'N/A')}</td>
        <td>${escapeHtml(order.productName || 'Item')} <span style="font-size: 0.72rem; color: var(--text-dim);">${order.category ? `(${order.category})` : ''}</span></td>
        <td style="font-weight: 700; color: ${order.totalAmount >= 1000 ? '#ef4444' : 'var(--text-main)'};">
          $${(Number(order.totalAmount) || 0).toFixed(2)}
        </td>
        <td>
          <span class="badge badge-partition">Part ${partitionNum}</span>
        </td>
        <td>
          <span style="font-weight: 700; color: ${order.riskScore >= 0.6 ? '#ef4444' : '#10b981'};">
            ${(Number(order.riskScore) || 0).toFixed(2)}
          </span>
        </td>
        <td>
          <span class="badge ${badgeClass}">${order.status || 'VERIFIED'}</span>
        </td>
        <td style="color: var(--text-dim); font-size: 0.75rem;">${timeFormatted}</td>
      </tr>
    `;
  }).join('');
}

function renderDLQTable() {
  if (state.dlq.length === 0) {
    elements.dlqTableBody.innerHTML = `
      <tr>
        <td colspan="5" style="text-align: center; color: var(--text-dim); padding: 2rem;">
          Dead Letter Queue is clean. Click "Test Broken Payload" to simulate poison-pill isolation!
        </td>
      </tr>
    `;
    return;
  }

  elements.dlqTableBody.innerHTML = state.dlq.map(entry => {
    return `
      <tr style="cursor: pointer;" onclick="openDLQModal('${entry.id}')">
        <td style="font-family: monospace; color: var(--accent-amber);">#${entry.id}</td>
        <td style="color: var(--accent-red); font-weight: 600;">${escapeHtml(entry.errorMessage || 'Validation Error')}</td>
        <td><span class="badge badge-partition">Part ${entry.partition !== undefined ? entry.partition : 'N/A'}</span></td>
        <td style="font-family: monospace; font-size: 0.72rem; max-width: 250px; overflow: hidden; text-overflow: ellipsis;">
          ${escapeHtml(entry.rawPayload || '')}
        </td>
        <td style="color: var(--text-dim); font-size: 0.75rem;">
          ${entry.failedAt ? new Date(entry.failedAt).toLocaleTimeString() : 'Just now'}
        </td>
      </tr>
    `;
  }).join('');
}

// Switch between Active Orders and Dead Letter Queue tabs
function switchTab(tab) {
  state.currentTab = tab;
  if (tab === 'orders') {
    elements.tabOrders.classList.add('active');
    elements.tabDLQ.classList.remove('active');
    elements.ordersTableWrapper.style.display = 'block';
    elements.dlqTableWrapper.style.display = 'none';
    elements.statusFilterSelect.style.display = 'inline-block';
  } else {
    elements.tabDLQ.classList.add('active');
    elements.tabOrders.classList.remove('active');
    elements.ordersTableWrapper.style.display = 'none';
    elements.dlqTableWrapper.style.display = 'block';
    elements.statusFilterSelect.style.display = 'none';
  }
  renderTables();
}

// Trigger Batch Stream of Orders via API
async function triggerBatchStream(count, delayMs, anomalyRate) {
  try {
    showToast(`Streaming ${count} order events into Kafka...`, 'info');
    const res = await fetch('/api/orders/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count, delayMs, anomalyRate })
    });
    if (res.ok) {
      showToast(`Successfully published ${count} events to topic: order_events`, 'success');
      setTimeout(fetchData, 300);
    } else {
      showToast('Failed to trigger stream generation', 'error');
    }
  } catch (err) {
    showToast(`Error: ${err.message}`, 'error');
  }
}

// Handle Custom Order Form
async function handleCustomOrderSubmit(e) {
  e.preventDefault();
  const userId = document.getElementById('formUserId').value;
  const productName = document.getElementById('formProduct').value;
  const category = document.getElementById('formCategory').value;
  const quantity = parseInt(document.getElementById('formQty').value, 10);
  const pricePerUnit = parseFloat(document.getElementById('formPrice').value);
  const totalAmount = Math.round(quantity * pricePerUnit * 100) / 100;

  const payload = {
    userId,
    productName,
    category,
    quantity,
    pricePerUnit,
    totalAmount,
    currency: 'USD'
  };

  try {
    const res = await fetch('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (res.ok) {
      const data = await res.json();
      const statusNote = totalAmount >= 1000 ? ' (High-Value Fraud Alert!)' : '';
      showToast(`Order published to Kafka! ${statusNote}`, totalAmount >= 1000 ? 'error' : 'success');
      fetchData();
    } else {
      const err = await res.json();
      showToast(`Validation error: ${err.error}`, 'error');
    }
  } catch (err) {
    showToast(`Network error: ${err.message}`, 'error');
  }
}

// Send Corrupt payload to demonstrate DLQ resilience
async function sendCorruptPayload() {
  try {
    showToast('Injecting malformed payload into pipeline...', 'info');
    // Send invalid payload missing required userId and with negative price
    const res = await fetch('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        productName: 'Corrupted Message Test',
        pricePerUnit: -99.99
      })
    });
    showToast('Poison pill isolated into Dead Letter Queue!', 'info');
    switchTab('dlq');
    setTimeout(fetchData, 400);
  } catch (err) {
    showToast(`Error: ${err.message}`, 'error');
  }
}

// Open Order Details Modal
window.openOrderModal = function(orderId) {
  const order = state.orders.find(o => o.orderId === orderId);
  if (!order) return;

  elements.modalTitle.textContent = `Order Details: ${order.orderId}`;
  elements.modalJsonViewer.textContent = JSON.stringify(order, null, 2);
  elements.modalBackdrop.classList.add('open');
};

// Open DLQ Details Modal
window.openDLQModal = function(id) {
  const entry = state.dlq.find(d => String(d.id) === String(id));
  if (!entry) return;

  elements.modalTitle.textContent = `Dead Letter Queue Record #${entry.id}`;
  elements.modalJsonViewer.textContent = JSON.stringify(entry, null, 2);
  elements.modalBackdrop.classList.add('open');
};

function closeModal() {
  elements.modalBackdrop.classList.remove('open');
}

// Toast helper
function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${type === 'success' ? '✅' : (type === 'error' ? '⚠️' : '⚡')}</span> <span>${escapeHtml(message)}</span>`;
  elements.toastContainer.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 250);
  }, 3500);
}

// Utility: Hash string for deterministic partition mapping
function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

// Utility: Escape HTML
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

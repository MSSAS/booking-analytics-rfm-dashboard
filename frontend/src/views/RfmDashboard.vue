<template>
  <div class="rfm-page">
    <h1>RFM-сегментация клиентов</h1>

    <section class="rfm-summary" v-if="summary">
      <h2>Сводка</h2>
      <div class="cards">
        <div class="card">
          <div class="label">Всего клиентов</div>
          <div class="value">{{ summary.customers_total.toLocaleString() }}</div>
        </div>
        <div class="card">
          <div class="label">Средний Recency (дней)</div>
          <div class="value">{{ summary.avg_recency.toFixed(1) }}</div>
        </div>
        <div class="card">
          <div class="label">Средняя частота (визитов)</div>
          <div class="value">{{ summary.avg_frequency.toFixed(2) }}</div>
        </div>
        <div class="card">
          <div class="label">Средний Monetary (взвеш. ночей)</div>
          <div class="value">{{ summary.avg_monetary.toFixed(1) }}</div>
        </div>
      </div>
    </section>

    <section class="rfm-chart" v-if="classDistribution.length">
      <h2>Распределение клиентов по RFM-классам</h2>
      <canvas ref="rfmChartCanvas"></canvas>
    </section>

    <section class="rfm-top" v-if="topCustomers.length">
      <h2>Топ-клиенты по ценности</h2>
      <table>
        <thead>
          <tr>
            <th>Guest ID</th>
            <th>Recency</th>
            <th>Frequency</th>
            <th>Monetary</th>
            <th>RFM-класс</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="c in topCustomers" :key="c.guest_id">
            <td>{{ c.guest_id }}</td>
            <td>{{ c.recency }}</td>
            <td>{{ c.frequency }}</td>
            <td>{{ c.monetary.toFixed(1) }}</td>
            <td>{{ c.rfm_class }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <p v-if="loading">Загрузка данных…</p>
    <p v-if="error" class="error">Ошибка: {{ error }}</p>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import {
  Chart,
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
} from 'chart.js'

Chart.register(BarController, BarElement, CategoryScale, LinearScale, Tooltip, Legend)

const summary = ref(null)
const classDistribution = ref([])
const topCustomers = ref([])
const loading = ref(false)
const error = ref(null)

const rfmChartCanvas = ref(null)
let rfmChartInstance = null

const API_BASE = 'http://localhost:8000'

async function fetchSummary() {
  const res = await fetch(`${API_BASE}/api/rfm/summary`)
  if (!res.ok) throw new Error('Не удалось загрузить summary')
  summary.value = await res.json()
}

async function fetchClassDistribution() {
  const res = await fetch(`${API_BASE}/api/rfm/class-distribution`)
  if (!res.ok) throw new Error('Не удалось загрузить class distribution')
  classDistribution.value = await res.json()
}

async function fetchTopCustomers() {
  const res = await fetch(`${API_BASE}/api/rfm/top-customers?limit=20`)
  if (!res.ok) throw new Error('Не удалось загрузить top customers')
  topCustomers.value = await res.json()
}

function renderChart() {
  if (!rfmChartCanvas.value) return
  if (rfmChartInstance) rfmChartInstance.destroy()

  const labels = classDistribution.value.map((item) => item.rfm_class)
  const counts = classDistribution.value.map((item) => item.customers_count)

  rfmChartInstance = new Chart(rfmChartCanvas.value, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Количество клиентов',
          data: counts,
          backgroundColor: "#ffffff",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          title: { display: true, text: 'RFM-класс' },
        },
        y: {
          title: { display: true, text: 'Количество клиентов' },
          beginAtZero: true,
        },
      },
    },
  })
}

onMounted(async () => {
  loading.value = true
  try {
    await Promise.all([
      fetchSummary(),
      fetchClassDistribution(),
      fetchTopCustomers(),
    ])
    renderChart()
  } catch (e) {
    console.error(e)
    error.value = e.message || 'Ошибка загрузки данных'
  } finally {
    loading.value = false
  }
})

onBeforeUnmount(() => {
  if (rfmChartInstance) rfmChartInstance.destroy()
})
</script>

<style scoped>
.rfm-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 8px 24px 32px;
}

h1 {
  font-size: 24px;
  margin-bottom: 16px;
}

h2 {
  font-size: 18px;
  margin-bottom: 12px;
}

.rfm-summary .cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}

.card {
  padding: 14px 16px;
  border-radius: 12px;
  background: #020617;
  color: #f9fafb;
  box-shadow: 0 4px 10px rgba(255, 255, 255, 0.3);
}

.card .label {
  font-size: 12px;
  opacity: 0.8;
  margin-bottom: 6px;
}

.card .value {
  font-size: 20px;
  font-weight: 600;
}

.rfm-chart {
  margin: 24px 0;
  height: 360px;
}

.rfm-top table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 16px;
  font-size: 13px;
}

.rfm-top th,
.rfm-top td {
  border: 1px solid #1e293b;
  padding: 6px 8px;
  text-align: left;
}

.rfm-top th {
  background: #020617;
}

.error {
  color: #ef4444;
  margin-top: 16px;
}
</style>

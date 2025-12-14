<script setup>
import { RouterLink, RouterView } from 'vue-router'
</script>

<template>
  <div class="layout">
    <aside class="sidebar">
      <h1 class="logo">Booking Analytics</h1>
      <nav class="nav">
        <RouterLink to="/" class="nav-link">Главная</RouterLink>
        <RouterLink to="/rfm" class="nav-link">RFM-дашборд</RouterLink>
      </nav>
    </aside>

    <main class="content">
      <RouterView />
    </main>
  </div>
</template>

<style scoped>
.layout {
  display: grid;
  grid-template-columns: 260px 1fr;
  min-height: 100vh;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI",
    sans-serif;
}

.sidebar {
  background: #020617;
  color: #e5e7eb;
  padding: 24px 20px;
  box-shadow: 2px 0 10px rgba(0, 0, 0, 0.3);
}

.logo {
  font-size: 20px;
  font-weight: 700;
  margin-bottom: 24px;
}

.nav {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.nav-link {
  color: #cbd5f5;
  text-decoration: none;
  padding: 8px 10px;
  border-radius: 8px;
  font-size: 14px;
}

.nav-link.router-link-active {
  background: #1d4ed8;
  color: #f9fafb;
}

.content {
  background: #020617;
  color: #e5e7eb;
  padding: 24px;
}
</style>

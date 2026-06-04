// Boot: loads initial data and starts all polling loops

function tickClock() {
  document.getElementById('clock').textContent = fmtFullIST(new Date()) + ' IST';
}

async function boot() {
  tickClock();
  setInterval(tickClock, 1000);

  try {
    const t = await api('/api/topics');
    STATE.topics = t.topics || [];
    STATE.environments = t.environments || [];
  } catch (e) {
    document.getElementById('team-grid').innerHTML = `<div class="loading">Failed to load topics: ${e.message}</div>`;
    return;
  }

  try {
    const p = await api('/api/panels');
    STATE.panels = p.panels || [];
  } catch (e) {
    document.getElementById('team-grid').innerHTML = `<div class="loading">Failed to load panels: ${e.message}</div>`;
    return;
  }

  STATE.jobs = _buildJobs();
  STATE.jobs_by_id = Object.fromEntries(STATE.jobs.map(j => [j.job_id, j]));
  STATE.selected_job_idx = 0;
  renderTeamGrid();
  reloadAllSparklines();

  let pollMs = 15000;
  try {
    const health = await api('/api/health');
    pollMs = (health.ui_poll_interval_seconds || 15) * 1000;
    document.getElementById('poll-label').textContent = `Polling ${health.poll_interval_seconds}s`;
  } catch (_) {}

  refreshStatus();
  refreshAlerts();
  fetchBreachCount();
  setInterval(refreshStatus, pollMs);
  setInterval(refreshAlerts, pollMs);
  setInterval(fetchBreachCount, 300000);
  setInterval(() => {
    if (STATE.view === 'detail') reloadAllPanels();
    else reloadAllSparklines();
  }, 120000);

  _aiCheckAvailability();
}

boot();

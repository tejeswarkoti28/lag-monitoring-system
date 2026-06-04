// Job grid, team cards, sparklines, detail modal, panel charts

const SPARKLINE_MINUTES = 1440;

const MODAL_RANGES = [
  { key: '30m', minutes: 30 },
  { key: '3h',  minutes: 180 },
  { key: '6h',  minutes: 360 },
  { key: '24h', minutes: 1440 },
  { key: '7d',  minutes: 10080 },
  { key: '30d', minutes: 43200 },
  { key: '6mo', minutes: 259200 },
];
const DEFAULT_MODAL_MINUTES = 30;
let MODAL_PANEL_MINUTES = {};

const MODAL_BACKDROP = document.getElementById('job-modal-backdrop');
const MODAL_CLOSE    = document.getElementById('modal-close');

function _buildJobs() {
  const out = [];
  for (const t of STATE.topics) {
    const envs = (t.environments && t.environments.length) ? t.environments : STATE.environments;
    for (const env of envs) {
      out.push({
        topic: t.topic,
        consumer_group: t.consumer_group,
        team: t.team,
        channel: t.channel,
        description: t.description || '',
        prom_job: t.job || '',
        ooa: t.ooa || '',
        oop: t.oop || '',
        environment: env,
        job_id: `${t.topic}::${env}`,
      });
    }
  }
  return out;
}

function _groupByTeam(jobs) {
  const map = new Map();
  for (const j of jobs) {
    const key = j.team || 'Unassigned';
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(j);
  }
  return map;
}

function renderTeamGrid() {
  const root = document.getElementById('team-grid');
  if (!STATE.jobs.length) {
    root.innerHTML = `
      <div class="grid-head">
        <span class="grid-title">Consumer-group jobs · live status</span>
        <span class="grid-meta" id="grid-last-update">Last update: --</span>
      </div>
      <div class="loading" style="padding:24px;background:var(--bg-1);border:1px solid var(--line);border-top:0;border-radius:0 0 8px 8px;">No jobs in catalog.</div>
    `;
    return;
  }
  const groups = _groupByTeam(STATE.jobs);
  let sectionsHtml = '';
  for (const [team, members] of groups) {
    const total = members.length;
    const breaching = members.filter(j => (STATE.job_state[j.job_id] || {}).status === 'breach').length;
    const healthy = total - breaching;
    const channel = members[0].channel || '';
    let cardsHtml = '';
    for (let i = 0; i < members.length; i++) {
      const j = members[i];
      const idx = STATE.jobs.indexOf(j);
      const st = STATE.job_state[j.job_id] || { lag: null, status: 'loading' };
      const isLoading = st.status === 'loading';
      const statusCls   = isLoading ? 'loading' : (st.status === 'breach' ? 'breach' : 'healthy');
      const statusLabel = isLoading ? '⋯ LOADING' : (st.status === 'breach' ? '▲ BREACH' : '✓ HEALTHY');
      const lagDisplay  = isLoading ? '--' : _fmtCardLag(st.lag).v;
      const unitDisplay = isLoading ? '' : _fmtCardLag(st.lag).u;
      cardsHtml += `
        <div class="job-card${st.status === 'breach' ? ' breach' : ''}" data-idx="${idx}">
          <div class="card-head">
            <span class="card-title">${j.topic}</span>
            <span class="card-env">${j.environment.toUpperCase()}</span>
          </div>
          <div class="card-sub">${j.team || ''} · ${j.channel || ''}</div>
          <div id="card-spark-${idx}" class="card-spark-empty">no data</div>
          <div class="card-foot">
            <span><span class="card-value">${lagDisplay}</span><span class="card-value-unit">${unitDisplay}</span></span>
            <span class="card-pill ${statusCls}">${statusLabel}</span>
          </div>
        </div>`;
    }
    sectionsHtml += `
      <div class="team-section">
        <div class="team-head">
          <span class="team-name">${team.toUpperCase()}</span>
          <span class="team-chip">${channel}</span>
          <span class="team-counts">
            <span class="count breach${breaching > 0 ? ' has' : ''}">${breaching} BREACHING</span>
            <span class="count healthy">${healthy} HEALTHY</span>
            <span class="sep">· ${total} job${total === 1 ? '' : 's'}</span>
          </span>
        </div>
        <div class="cards-grid">${cardsHtml}</div>
      </div>`;
  }
  root.innerHTML = `
    <div class="grid-head">
      <span class="grid-title">Consumer-group jobs · live status</span>
      <span class="grid-meta" id="grid-last-update">Last update: --</span>
    </div>
    ${sectionsHtml}`;
  root.querySelectorAll('.job-card').forEach(card => {
    card.addEventListener('click', () => {
      STATE.selected_job_idx = parseInt(card.dataset.idx, 10);
      openDetailView();
    });
  });
}

async function loadCardSparkline(job, minutes) {
  const idx = STATE.jobs.indexOf(job);
  if (idx < 0) return;
  const slot = document.getElementById(`card-spark-${idx}`);
  if (!slot) return;
  try {
    const params = new URLSearchParams({ minutes, env: job.environment, topic: job.topic, consumer_group: job.consumer_group });
    if (job.prom_job) params.set('prom_job', job.prom_job);
    if (job.ooa) params.set('ooa', job.ooa);
    if (job.oop) params.set('oop', job.oop);
    const data = await api(`/api/panel/consumer_group_lag/range?${params.toString()}`);
    const points = (data.points || []).map(p => p.value || 0);
    STATE.job_state[job.job_id] = STATE.job_state[job.job_id] || {};
    if (points.length) {
      STATE.job_state[job.job_id].sparkline = points;
      drawSparkline(slot, points);
    }
  } catch (_) {}
}

async function reloadAllSparklines() {
  for (const j of STATE.jobs) {
    loadCardSparkline(j, SPARKLINE_MINUTES).catch(() => {});
  }
}

function openDetailView() {
  STATE.view = 'detail';
  MODAL_PANEL_MINUTES = {};
  for (const p of STATE.panels) MODAL_PANEL_MINUTES[p.id] = DEFAULT_MODAL_MINUTES;
  renderDetailModal();
  MODAL_BACKDROP.classList.add('open');
  document.body.style.overflow = 'hidden';
  reloadAllPanels();
}

function closeDetailView() {
  STATE.view = 'grid';
  MODAL_BACKDROP.classList.remove('open');
  document.body.style.overflow = '';
  reloadAllSparklines();
}

MODAL_CLOSE.addEventListener('click', closeDetailView);
MODAL_BACKDROP.addEventListener('click', (ev) => { if (ev.target === MODAL_BACKDROP) closeDetailView(); });
document.addEventListener('keydown', (ev) => { if (ev.key === 'Escape' && STATE.view === 'detail') closeDetailView(); });

function renderDetailModal() {
  const job = STATE.jobs[STATE.selected_job_idx];
  if (!job) return;
  const threshold = (STATE.summary && STATE.summary.threshold) || 0;
  document.getElementById('modal-title').innerHTML =
    `${job.topic}<span class="sep">::</span>${job.environment.toLowerCase()}`;
  document.getElementById('modal-info').innerHTML = `
    <span class="info-item">${job.team || '--'} · ${job.channel || '--'}</span>
    <span class="info-item">Consumer group:<b>${job.consumer_group}</b></span>
    <span class="info-item">Environment:<b>${job.environment.toUpperCase()}</b></span>
    <span class="info-item">Threshold:<b>${FMT_AXIS(threshold)}</b></span>
    ${job.description ? `<span class="info-item">Description:<b>${job.description}</b></span>` : ''}`;
  const st = STATE.job_state[job.job_id] || {};
  const cur = _fmtCardLag(st.lag || 0);
  document.getElementById('modal-current').innerHTML =
    `Current lag (max of both): <b>${cur.v} ${cur.u}</b>`;
  const body = document.getElementById('modal-body');
  let html = '';
  for (const p of STATE.panels) {
    const minutes = MODAL_PANEL_MINUTES[p.id] || DEFAULT_MODAL_MINUTES;
    let rangeHtml = '';
    for (const r of MODAL_RANGES) {
      rangeHtml += `<button class="${r.minutes === minutes ? 'active' : ''}" data-panel="${p.id}" data-minutes="${r.minutes}">${r.key}</button>`;
    }
    html += `
      <div class="chart-card" id="chart-card-${p.id}">
        <div class="chart-card-head">
          <span class="chart-card-title">${p.title.toUpperCase()}<span class="current" id="chart-current-${p.id}"></span></span>
          <span class="chart-range">${rangeHtml}</span>
        </div>
        <div class="chart-loading">Loading…</div>
        <div class="chart-foot">
          <span class="chart-legend"><span class="dot" style="background:${p.color}"></span><span>${p.title}</span></span>
          <span class="chart-stats" id="chart-stats-${p.id}">--</span>
        </div>
      </div>`;
  }
  body.innerHTML = html;
  body.querySelectorAll('.chart-range button').forEach(btn => {
    btn.addEventListener('click', () => {
      const pid = btn.dataset.panel;
      const m = parseInt(btn.dataset.minutes, 10);
      MODAL_PANEL_MINUTES[pid] = m;
      btn.parentNode.querySelectorAll('button').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const panel = STATE.panels.find(x => x.id === pid);
      if (panel) loadPanel(panel, STATE.jobs[STATE.selected_job_idx], m);
    });
  });
}

async function reloadAllPanels() {
  const job = STATE.jobs[STATE.selected_job_idx];
  if (!job) return;
  for (const p of STATE.panels) {
    loadPanel(p, job, MODAL_PANEL_MINUTES[p.id] || DEFAULT_MODAL_MINUTES).catch(() => {});
  }
}

async function loadPanel(panel, job, minutes) {
  const slot = document.getElementById(`chart-card-${panel.id}`);
  if (!slot) return;
  const currentEl = document.getElementById(`chart-current-${panel.id}`);
  const params = new URLSearchParams({ minutes });
  if (panel.scope.includes('env'))            params.set('env', job.environment);
  if (panel.scope.includes('topic'))          params.set('topic', job.topic);
  if (panel.scope.includes('consumer_group')) params.set('consumer_group', job.consumer_group);
  if (job.prom_job) params.set('prom_job', job.prom_job);
  if (job.ooa)      params.set('ooa', job.ooa);
  if (job.oop)      params.set('oop', job.oop);
  const ph = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
  if (ph) ph.outerHTML = `<div class="chart-loading">Loading…</div>`;
  if (currentEl) currentEl.innerHTML = '';
  let data;
  try {
    data = await api(`/api/panel/${panel.id}/range?${params.toString()}`);
  } catch (e) {
    const ph2 = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
    if (ph2) ph2.outerHTML = `<div class="chart-empty">Error: ${e.message}</div>`;
    return;
  }
  const points = data.points || [];
  const statsEl = document.getElementById(`chart-stats-${panel.id}`);
  if (currentEl) {
    currentEl.innerHTML = points.length
      ? `CURRENT <b>${FMT_K(points[points.length - 1].value)}</b>`
      : '<span style="color:var(--text-mute)">no data</span>';
  }
  if (statsEl) {
    if (points.length) {
      const values = points.map(p => p.value);
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      statsEl.innerHTML = `
        <span>Mean<b>${FMT_K(mean)}</b></span>
        <span>Last<b>${FMT_K(values[values.length - 1])}</b></span>
        <span>Max<b>${FMT_K(Math.max(...values))}</b></span>
        <span>Min<b>${FMT_K(Math.min(...values))}</b></span>`;
    } else {
      statsEl.innerHTML = '<span style="color:var(--text-mute)">no data</span>';
    }
  }
  const ph3 = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
  if (!points.length) {
    if (ph3) ph3.outerHTML = `<div class="chart-empty">No data for this range</div>`;
    return;
  }
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.classList.add('chart-svg');
  svg.setAttribute('preserveAspectRatio', 'none');
  if (ph3) ph3.replaceWith(svg);
  drawChart(svg, points, data);
}

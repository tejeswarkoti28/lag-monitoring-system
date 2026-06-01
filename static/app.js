// =============================================================================
// State + utilities
// =============================================================================
// Sparklines on the team-grid cards always show this many minutes. Each modal
// chart picks its own range from the per-chart range bar.
const SPARKLINE_MINUTES = 1440; // 24h — fetched from Prometheus, not SQLite

const STATE = {
  topics: [],          // [{topic, consumer_group, team, channel}]
  environments: [],    // ['eus', 'scus']
  panels: [],          // panel definitions from /api/panels
  jobs: [],            // [{topic, consumer_group, team, channel, environment, job_id}]
  jobs_by_id: {},      // job_id -> job
  job_state: {},       // job_id -> { lag, status, sparkline: [..numbers..] }
  summary: {},         // last /api/status summary (incl. threshold for modal)
  view: 'grid',        // 'grid' | 'detail'
  selected_job_idx: 0,
  poll_handle: null,
  seen_alert_ids: new Set(),
};

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

const FMT_K = (n) => {
  if (n === null || n === undefined || isNaN(n)) return '0';
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return (n / 1e3).toFixed(2) + 'K';
  if (abs < 1 && abs > 0) return n.toFixed(2);
  return Math.round(n).toString();
};

// Compact integer formatter for chart axis labels: "200K" not "200.00K".
// Uses 1 decimal only when needed (e.g. 1.5M) and trims trailing .0.
const FMT_AXIS = (n) => {
  if (n === null || n === undefined || isNaN(n)) return '0';
  const abs = Math.abs(n);
  if (abs === 0) return '0';
  if (abs >= 1e9) {
    const v = n / 1e9;
    return (v >= 10 ? Math.round(v).toString() : v.toFixed(1).replace(/\.0$/, '')) + 'B';
  }
  if (abs >= 1e6) {
    const v = n / 1e6;
    return (v >= 10 ? Math.round(v).toString() : v.toFixed(1).replace(/\.0$/, '')) + 'M';
  }
  if (abs >= 1e3) {
    return Math.round(n / 1e3) + 'K';
  }
  return Math.round(n).toString();
};

// IST = UTC + 5:30
const IST_OFFSET_MIN = 330;
function _toIST(d) { return new Date(d.getTime() + IST_OFFSET_MIN * 60000); }
function fmtClockIST(d) {
  const i = _toIST(d);
  return String(i.getUTCHours()).padStart(2,'0') + ':' + String(i.getUTCMinutes()).padStart(2,'0');
}
function fmtFullIST(d) {
  const i = _toIST(d);
  const date = i.getUTCFullYear() + '-' + String(i.getUTCMonth()+1).padStart(2,'0') + '-' + String(i.getUTCDate()).padStart(2,'0');
  return date + ' ' + fmtClockIST(d) + ':' + String(i.getUTCSeconds()).padStart(2,'0');
}
function fmtDateIST(d) {
  const i = _toIST(d);
  return String(i.getUTCMonth()+1).padStart(2,'0') + '-' + String(i.getUTCDate()).padStart(2,'0');
}
// Pick a "nice" tick step (in seconds) so the chart shows ~10-12 labels
// across the visible time span.
function _chooseTimeStep(spanSec) {
  const STEPS = [
    60,        // 1 min
    300,       // 5 min
    600,       // 10 min
    1800,      // 30 min
    3600,      // 1 h
    7200,      // 2 h
    21600,     // 6 h
    43200,     // 12 h
    86400,     // 1 d
    259200,    // 3 d
    604800,    // 7 d
    2592000,   // 30 d
  ];
  const TARGET = 12;
  for (const s of STEPS) {
    if (spanSec / s <= TARGET) return s;
  }
  return STEPS[STEPS.length - 1];
}
function relTime(iso) {
  const d = new Date(iso);
  const s = Math.floor((Date.now() - d.getTime()) / 1000);
  if (s < 5)    return 'just now';
  if (s < 60)   return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
}

// =============================================================================
// Job catalog — fan out (topic, consumer_group) across environments
// =============================================================================
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

// =============================================================================
// Team-grouped card grid
// =============================================================================
function _groupByTeam(jobs) {
  const map = new Map();
  for (const j of jobs) {
    const key = j.team || 'Unassigned';
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(j);
  }
  return map;
}

function _fmtCardLag(lag) {
  // Auto-pick unit so a 5K lag isn't printed as "0.00 M"
  const abs = Math.abs(lag || 0);
  if (abs >= 1e9) return { v: (lag / 1e9).toFixed(2), u: 'B msgs' };
  if (abs >= 1e6) return { v: (lag / 1e6).toFixed(2), u: 'M msgs' };
  if (abs >= 1e3) return { v: (lag / 1e3).toFixed(2), u: 'K msgs' };
  return { v: Math.round(lag || 0).toString(), u: 'msgs' };
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
    const breaching = members.filter(j => (STATE.job_state[j.job_id]||{}).status === 'breach').length;
    const healthy = total - breaching;
    const channel = members[0].channel || '';
    const breachClass = breaching > 0 ? ' has' : '';

    let cardsHtml = '';
    for (let i = 0; i < members.length; i++) {
      const j = members[i];
      const idx = STATE.jobs.indexOf(j);
      const st = STATE.job_state[j.job_id] || { lag: 0, status: 'ok' };
      const statusCls = st.status === 'breach' ? 'breach' : 'healthy';
      const statusLabel = st.status === 'breach' ? '▲ BREACH' : '✓ HEALTHY';
      const cardCls = st.status === 'breach' ? ' breach' : '';
      const slotId = `card-spark-${idx}`;
      cardsHtml += `
        <div class="job-card${cardCls}" data-idx="${idx}">
          <div class="card-head">
            <span class="card-title">${j.topic}</span>
            <span class="card-env">${j.environment.toUpperCase()}</span>
          </div>
          <div class="card-sub">${j.team || ''} · ${j.channel || ''}</div>
          <div id="${slotId}" class="card-spark-empty">no data</div>
          <div class="card-foot">
            <span><span class="card-value">${_fmtCardLag(st.lag).v}</span><span class="card-value-unit">${_fmtCardLag(st.lag).u}</span></span>
            <span class="card-pill ${statusCls}">${statusLabel}</span>
          </div>
        </div>
      `;
    }

    sectionsHtml += `
      <div class="team-section">
        <div class="team-head">
          <span class="team-name">${team.toUpperCase()}</span>
          <span class="team-chip">${channel}</span>
          <span class="team-counts">
            <span class="count breach${breachClass}">${breaching} BREACHING</span>
            <span class="count healthy">${healthy} HEALTHY</span>
            <span class="sep">· ${total} job${total === 1 ? '' : 's'}</span>
          </span>
        </div>
        <div class="cards-grid">${cardsHtml}</div>
      </div>
    `;
  }

  root.innerHTML = `
    <div class="grid-head">
      <span class="grid-title">Consumer-group jobs · live status</span>
      <span class="grid-meta" id="grid-last-update">Last update: --</span>
    </div>
    ${sectionsHtml}
  `;

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
  let data;
  try {
    const params = new URLSearchParams({ minutes, env: job.environment, topic: job.topic, consumer_group: job.consumer_group });
    if (job.prom_job) params.set('prom_job', job.prom_job);
    if (job.ooa) params.set('ooa', job.ooa);
    if (job.oop) params.set('oop', job.oop);
    data = await api(`/api/panel/consumer_group_lag/range?${params.toString()}`);
  } catch (e) {
    return;
  }
  const points = (data.points || []).map(p => p.value || 0);
  STATE.job_state[job.job_id] = STATE.job_state[job.job_id] || {};
  if (points.length) {
    STATE.job_state[job.job_id].sparkline = points;
    drawSparkline(slot, points);
  }
}

function drawSparkline(container, values) {
  if (!values.length) return;
  const W = 200, H = 50, pad = 2;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(0.0001, max - min);
  const innerW = W - 2 * pad;
  const innerH = H - 2 * pad;
  const xOf = (i) => pad + (i / Math.max(1, values.length - 1)) * innerW;
  const yOf = (v) => pad + (1 - (v - min) / range) * innerH;
  let d = '';
  for (let i = 0; i < values.length; i++) {
    d += (i ? 'L' : 'M') + xOf(i).toFixed(1) + ',' + yOf(values[i]).toFixed(1);
  }
  const svg = `
    <svg class="card-spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
      <path d="${d}" fill="none" stroke="${'var(--blue)'}" stroke-width="1.2" />
    </svg>
  `;
  container.outerHTML = svg.replace('class="card-spark"', `class="card-spark" id="${container.id}"`);
}

async function reloadAllSparklines() {
  for (const j of STATE.jobs) {
    loadCardSparkline(j, SPARKLINE_MINUTES).catch(err => {
      console.error('sparkline load failed', j.job_id, err);
    });
  }
}

// =============================================================================
// Job detail modal (Grafana-style charts with threshold + breach fill)
// =============================================================================
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
// Per-panel range state lives only while the modal is open.
let MODAL_PANEL_MINUTES = {};
const MODAL_BACKDROP = document.getElementById('job-modal-backdrop');
const MODAL_CLOSE = document.getElementById('modal-close');

function openDetailView() {
  STATE.view = 'detail';
  // Default each panel to 30m; user can change per-chart from the inline range bar
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
MODAL_BACKDROP.addEventListener('click', (ev) => {
  if (ev.target === MODAL_BACKDROP) closeDetailView();
});
document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape' && STATE.view === 'detail') closeDetailView();
});

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
    ${job.description ? `<span class="info-item">Description:<b>${job.description}</b></span>` : ''}
  `;
  // Current lag will be filled in once data arrives; show last known status now
  const st = STATE.job_state[job.job_id] || {};
  const cur = _fmtCardLag(st.lag || 0);
  document.getElementById('modal-current').innerHTML =
    `Current lag (max of both): <b>${cur.v} ${cur.u}</b>`;

  // Render one chart-card per panel
  const body = document.getElementById('modal-body');
  let html = '';
  for (const p of STATE.panels) {
    const minutes = MODAL_PANEL_MINUTES[p.id] || DEFAULT_MODAL_MINUTES;
    let rangeHtml = '';
    for (const r of MODAL_RANGES) {
      const active = r.minutes === minutes ? ' active' : '';
      rangeHtml += `<button class="${active.trim()}" data-panel="${p.id}" data-minutes="${r.minutes}">${r.key}</button>`;
    }
    html += `
      <div class="chart-card" id="chart-card-${p.id}">
        <div class="chart-card-head">
          <span class="chart-card-title">
            ${p.title.toUpperCase()}
            <span class="current" id="chart-current-${p.id}"></span>
          </span>
          <span class="chart-range">${rangeHtml}</span>
        </div>
        <div class="chart-loading">Loading…</div>
        <div class="chart-foot">
          <span class="chart-legend">
            <span class="dot" style="background:${p.color}"></span>
            <span>${p.title}</span>
          </span>
          <span class="chart-stats" id="chart-stats-${p.id}">--</span>
        </div>
      </div>
    `;
  }
  body.innerHTML = html;
  body.querySelectorAll('.chart-range button').forEach(btn => {
    btn.addEventListener('click', () => {
      const pid = btn.dataset.panel;
      const m = parseInt(btn.dataset.minutes, 10);
      MODAL_PANEL_MINUTES[pid] = m;
      // Toggle active class within this chart's range row
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
    const m = MODAL_PANEL_MINUTES[p.id] || DEFAULT_MODAL_MINUTES;
    loadPanel(p, job, m).catch(err => {
      console.error('panel load failed', p.id, job.job_id, err);
    });
  }
}

async function loadPanel(panel, job, minutes) {
  const slot = document.getElementById(`chart-card-${panel.id}`);
  if (!slot) return;
  const currentEl = document.getElementById(`chart-current-${panel.id}`);

  const params = new URLSearchParams();
  params.set('minutes', minutes);
  if (panel.scope.includes('env')) params.set('env', job.environment);
  if (panel.scope.includes('topic')) params.set('topic', job.topic);
  if (panel.scope.includes('consumer_group')) params.set('consumer_group', job.consumer_group);
  if (job.prom_job) params.set('prom_job', job.prom_job);
  if (job.ooa) params.set('ooa', job.ooa);
  if (job.oop) params.set('oop', job.oop);

  // Replace any chart inside the slot with a loading placeholder while we fetch
  const placeholder = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
  if (placeholder) placeholder.outerHTML = `<div class="chart-loading">Loading…</div>`;
  if (currentEl) currentEl.innerHTML = '';

  let data;
  try {
    data = await api(`/api/panel/${panel.id}/range?${params.toString()}`);
  } catch (e) {
    const ph = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
    if (ph) ph.outerHTML = `<div class="chart-empty">Error: ${e.message}</div>`;
    return;
  }

  const points = data.points || [];
  const statsEl = document.getElementById(`chart-stats-${panel.id}`);
  if (currentEl) {
    if (points.length) {
      const last = points[points.length - 1].value;
      currentEl.innerHTML = `CURRENT <b>${FMT_K(last)}</b>`;
    } else {
      currentEl.innerHTML = '<span style="color:var(--text-mute)">no data</span>';
    }
  }
  if (statsEl) {
    if (points.length) {
      const values = points.map(p => p.value);
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const last = values[values.length - 1];
      const max = Math.max(...values);
      const min = Math.min(...values);
      statsEl.innerHTML = `
        <span>Mean<b>${FMT_K(mean)}</b></span>
        <span>Last<b>${FMT_K(last)}</b></span>
        <span>Max<b>${FMT_K(max)}</b></span>
        <span>Min<b>${FMT_K(min)}</b></span>
      `;
    } else {
      statsEl.innerHTML = '<span style="color:var(--text-mute)">no data</span>';
    }
  }

  const ph2 = slot.querySelector('.chart-loading, .chart-svg, .chart-empty');
  if (!points.length) {
    if (ph2) ph2.outerHTML = `<div class="chart-empty">No data for this range</div>`;
    return;
  }
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.classList.add('chart-svg');
  svg.setAttribute('preserveAspectRatio', 'none');
  if (ph2) ph2.replaceWith(svg);
  drawChart(svg, points, data);
}

// =============================================================================
// Grafana-style chart with threshold + breach fill + current-value callout
// =============================================================================
function drawChart(svg, points, data) {
  const W = 880, H = 220, padL = 56, padR = 16, padT = 10, padB = 36;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.innerHTML = '';

  const tsArr = points.map(p => p.ts);
  const valArr = points.map(p => p.value);
  const tMin = tsArr[0], tMax = tsArr[tsArr.length - 1];
  const tSpan = Math.max(1, tMax - tMin);
  const showThr = !!data.show_threshold && typeof data.threshold === 'number';
  const thr = data.threshold || 0;

  let dataMax = Math.max(...valArr);
  let dataMin = Math.min(...valArr);
  if (data.y_min != null) dataMin = Math.min(dataMin, data.y_min);
  if (data.y_max != null) dataMax = Math.max(dataMax, data.y_max);
  // Only force the threshold into the y-axis when the data is in the same
  // ballpark (>=30% of threshold). If the data is way below, zoom to data only.
  const thrInRange = showThr && dataMax >= thr * 0.3;
  if (thrInRange) dataMax = Math.max(dataMax, thr * 1.2);
  if (dataMax === dataMin) dataMax = dataMin + 1;
  const yMin = data.y_min != null ? data.y_min : Math.max(0, dataMin);
  const yMax = data.y_max != null ? data.y_max : dataMax * 1.05;
  const yRange = Math.max(0.0001, yMax - yMin);

  const innerW = W - padL - padR;
  const innerH = H - padT - padB;
  const xOf = (t) => padL + ((t - tMin) / tSpan) * innerW;
  const yOf = (v) => padT + (1 - (v - yMin) / yRange) * innerH;
  const baselineY = yOf(yMin);

  // Y-axis grid + labels
  const niceStep = _niceStep(yRange);
  let yAxis = '';
  for (let v = Math.ceil(yMin / niceStep) * niceStep; v <= yMax + 0.0001; v += niceStep) {
    const y = yOf(v);
    yAxis += `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${padL+innerW}" y2="${y.toFixed(1)}" stroke="#1a1f27" stroke-width="0.5" />`;
    yAxis += `<text x="${padL-6}" y="${(y+3.5).toFixed(1)}" fill="#8b949e" font-size="9.5" font-family="JetBrains Mono,monospace" text-anchor="end">${FMT_AXIS(v)}</text>`;
  }

  // X-axis labels
  const stepSec = _chooseTimeStep(tSpan);
  const istOffsetSec = IST_OFFSET_MIN * 60;
  const firstTickIst = Math.ceil((tMin + istOffsetSec) / stepSec) * stepSec;
  const minLabelPx = 56;
  let xAxis = '';
  let lastLabelX = -Infinity;
  for (let tIst = firstTickIst; tIst <= tMax + istOffsetSec; tIst += stepSec) {
    const t = tIst - istOffsetSec;
    const x = xOf(t);
    if (x < padL - 1 || x > padL + innerW + 1) continue;
    xAxis += `<line x1="${x.toFixed(1)}" y1="${padT}" x2="${x.toFixed(1)}" y2="${padT+innerH}" stroke="#1a1f27" stroke-width="0.5" />`;
    if (x - lastLabelX >= minLabelPx) {
      const d = new Date(t * 1000);
      const label = stepSec >= 86400 ? fmtDateIST(d) : fmtClockIST(d);
      xAxis += `<text x="${x.toFixed(1)}" y="${(padT+innerH+14).toFixed(1)}" fill="#8b949e" font-size="9.5" font-family="JetBrains Mono,monospace" text-anchor="middle">${label}</text>`;
      lastLabelX = x;
    }
  }

  // Build the line path
  const linePath = points.map((p, i) =>
    (i ? 'L' : 'M') + xOf(p.ts).toFixed(1) + ',' + yOf(p.value).toFixed(1)
  ).join('');

  const lineColor = data.color || '#58a6ff';
  let breachFill = '';
  let thresholdLine = '';
  let gradId = '';
  const showThrUI = showThr && thr >= yMin && thr <= yMax;
  if (showThrUI) {
    const thrY = yOf(thr);
    let top = '';
    for (let i = 0; i < points.length; i++) {
      const yv = yOf(Math.max(valArr[i], thr));
      top += (i ? 'L' : 'M') + xOf(tsArr[i]).toFixed(1) + ',' + yv.toFixed(1);
    }
    let bottom = '';
    for (let i = points.length - 1; i >= 0; i--) {
      bottom += `L${xOf(tsArr[i]).toFixed(1)},${thrY.toFixed(1)}`;
    }
    const breachAreaPath = top + bottom + 'Z';

    gradId = `gr-breach-${Math.random().toString(36).slice(2, 8)}`;
    breachFill = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#f85149" stop-opacity="0.05" />
          <stop offset="100%" stop-color="#f85149" stop-opacity="0.45" />
        </linearGradient>
      </defs>
      <path d="${breachAreaPath}" fill="url(#${gradId})" stroke="none" />
    `;
    thresholdLine = `
      <line x1="${padL}" y1="${thrY.toFixed(1)}" x2="${padL+innerW}" y2="${thrY.toFixed(1)}"
            stroke="#f85149" stroke-width="1.2" stroke-dasharray="5,4" opacity="0.9" />
      <g transform="translate(${(padL+innerW-92).toFixed(1)}, ${(thrY-9).toFixed(1)})">
        <rect width="88" height="16" rx="3" fill="#1a0d0d" stroke="#6d2c2c" />
        <text x="44" y="11.5" fill="#f85149" font-size="10" font-family="JetBrains Mono,monospace" text-anchor="middle" font-weight="700">threshold ${FMT_AXIS(thr)}</text>
      </g>
    `;
  }

  // Subtle area fill under the line
  const safePathTop = points.map((p, i) =>
    (i ? 'L' : 'M') + xOf(p.ts).toFixed(1) + ',' + yOf(showThrUI ? Math.min(p.value, thr) : p.value).toFixed(1)
  ).join('');
  const safeAreaPath = safePathTop +
    `L${xOf(tMax).toFixed(1)},${baselineY.toFixed(1)}` +
    `L${xOf(tMin).toFixed(1)},${baselineY.toFixed(1)}Z`;
  const safeGradId = `gr-safe-${Math.random().toString(36).slice(2, 8)}`;
  const safeFill = `
    <defs>
      <linearGradient id="${safeGradId}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="${lineColor}" stop-opacity="0.30" />
        <stop offset="100%" stop-color="${lineColor}" stop-opacity="0.04" />
      </linearGradient>
    </defs>
    <path d="${safeAreaPath}" fill="url(#${safeGradId})" stroke="none" />
  `;

  // Current-value callout near the rightmost point
  const lastT = tsArr[tsArr.length - 1];
  const lastV = valArr[valArr.length - 1];
  const lastX = xOf(lastT);
  const lastY = yOf(lastV);
  const isBreaching = showThr && lastV >= thr;
  const dotColor = isBreaching ? '#f85149' : lineColor;
  const calloutLabel = `${FMT_AXIS(lastV)} @ ${fmtClockIST(new Date(lastT * 1000))}`;
  const calloutW = Math.max(82, calloutLabel.length * 6 + 12);
  let calloutX = lastX - calloutW - 8;
  if (calloutX < padL + 4) calloutX = lastX + 8;
  const calloutY = Math.max(padT + 6, Math.min(padT + innerH - 22, lastY + 10));
  const callout = `
    <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="3" fill="${dotColor}">
      <animate attributeName="r" values="3;5.5;3" dur="1.6s" repeatCount="indefinite" />
      <animate attributeName="opacity" values="1;0.55;1" dur="1.6s" repeatCount="indefinite" />
    </circle>
    <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="2.2" fill="${dotColor}" />
    <g transform="translate(${calloutX.toFixed(1)}, ${calloutY.toFixed(1)})">
      <rect width="${calloutW}" height="17" rx="3" fill="#0e1218" stroke="${isBreaching ? '#6d2c2c' : '#2e3742'}" />
      <text x="${calloutW/2}" y="12" fill="${isBreaching ? '#f85149' : '#e6edf3'}" font-size="10" font-family="JetBrains Mono,monospace" text-anchor="middle" font-weight="700">${calloutLabel}</text>
    </g>
  `;

  // Axis labels
  const axisLabels = `
    <text x="14" y="${(padT+innerH/2).toFixed(1)}" fill="#8b949e" font-size="10.5" font-family="JetBrains Mono,monospace" text-anchor="middle" transform="rotate(-90, 14, ${(padT+innerH/2).toFixed(1)})">Lag</text>
    <text x="${(padL+innerW/2).toFixed(1)}" y="${(padT+innerH+30).toFixed(1)}" fill="#8b949e" font-size="10.5" font-family="JetBrains Mono,monospace" text-anchor="middle">Time (IST)</text>
  `;

  svg.innerHTML = `
    ${yAxis}
    ${xAxis}
    ${safeFill}
    ${breachFill}
    ${thresholdLine}
    <path d="${linePath}" fill="none" stroke="${lineColor}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round" />
    ${callout}
    ${axisLabels}
  `;
}

function _niceStep(range, targetTicks = 4) {
  if (range <= 0) return 1;
  const raw = range / targetTicks;
  const exp = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / exp;
  let mul;
  if (norm < 1.5) mul = 1;
  else if (norm < 3) mul = 2;
  else if (norm < 7) mul = 5;
  else mul = 10;
  return mul * exp;
}

// =============================================================================
// Status / stats strip / health
// =============================================================================
async function refreshStatus() {
  try {
    const data = await api('/api/status');
    STATE.summary = data.summary || {};
    document.getElementById('stat-monitored').textContent = data.summary.monitored;
    document.getElementById('stat-breaching').textContent = data.summary.breaching;
    document.getElementById('stat-healthy').textContent = data.summary.healthy;
    document.getElementById('stat-alerts').textContent = data.summary.alerts_24h;
    document.getElementById('stat-envs').textContent = STATE.environments.join(' + ');
    updateSlackIndicator(data.summary.slack_configured);

    // Per-job state (lag + status). Cards re-render when status flips.
    let statusChanged = false;
    for (const j of (data.jobs || [])) {
      const prev = STATE.job_state[j.job_id] || {};
      if (prev.status !== j.status) statusChanged = true;
      STATE.job_state[j.job_id] = {
        ...prev,
        lag: j.lag || 0,
        status: j.status,
      };
    }

    if (STATE.view === 'grid') {
      const lu = document.getElementById('grid-last-update');
      if (lu && data.summary.last_poll_at) {
        lu.textContent = `Last update: ${relTime(data.summary.last_poll_at)}`;
      }
      if (statusChanged) {
        renderTeamGrid();
        reloadAllSparklines();
      } else {
        // Cheap path: update each card's lag value + pill in place
        for (const j of (data.jobs || [])) {
          const idx = STATE.jobs.findIndex(x => x.job_id === j.job_id);
          if (idx < 0) continue;
          const card = document.querySelector(`.job-card[data-idx="${idx}"]`);
          if (!card) continue;
          const valEl = card.querySelector('.card-value');
          if (valEl) {
            const f = _fmtCardLag(j.lag || 0);
            valEl.textContent = f.v;
            const unitEl = card.querySelector('.card-value-unit');
            if (unitEl) unitEl.textContent = f.u;
          }
        }
      }
    } else if (STATE.view === 'detail') {
      // Modal stays open — refresh the live "current lag" line on top
      const job = STATE.jobs[STATE.selected_job_idx];
      const cur = document.getElementById('modal-current');
      if (cur && job) {
        const lag = (STATE.job_state[job.job_id] || {}).lag || 0;
        const f = _fmtCardLag(lag);
        cur.innerHTML = `Current lag (max of both): <b>${f.v} ${f.u}</b>`;
      }
    }
  } catch (e) {
    console.error('status fetch failed', e);
  }
}

function updateSlackIndicator(on) {
  const el = document.getElementById('slack-status');
  el.innerHTML = on
    ? '<span class="dot slack-on"></span><span>Slack: configured</span>'
    : '<span class="dot"></span><span>Slack: not configured</span>';
}

// Test Slack button
const SLACK_TEST_BTN = document.getElementById('slack-test-btn');
async function _runSlackTest() {
  if (!SLACK_TEST_BTN) return;
  SLACK_TEST_BTN.disabled = true;
  SLACK_TEST_BTN.classList.remove('success', 'error');
  const original = SLACK_TEST_BTN.textContent;
  SLACK_TEST_BTN.textContent = 'Sending…';
  try {
    const r = await fetch('/api/slack/test', { method: 'POST' });
    const data = await r.json();
    if (!r.ok || !data.ok) {
      SLACK_TEST_BTN.classList.add('error');
      const failed = (data.results || []).filter(x => !x.delivered).map(x => `${x.label} (${x.detail})`).join(', ');
      SLACK_TEST_BTN.textContent = failed ? `Failed: ${failed}` : (data.error || 'Failed');
    } else {
      SLACK_TEST_BTN.classList.add('success');
      const labels = (data.results || []).map(x => x.label).join(', ');
      SLACK_TEST_BTN.textContent = `Sent ✓ (${labels})`;
    }
  } catch (e) {
    SLACK_TEST_BTN.classList.add('error');
    SLACK_TEST_BTN.textContent = `Network error: ${e.message}`;
  } finally {
    setTimeout(() => {
      SLACK_TEST_BTN.disabled = false;
      SLACK_TEST_BTN.classList.remove('success', 'error');
      SLACK_TEST_BTN.textContent = original;
    }, 6000);
  }
}
if (SLACK_TEST_BTN) SLACK_TEST_BTN.addEventListener('click', _runSlackTest);

// =============================================================================
// Alert feed
// =============================================================================
async function refreshAlerts() {
  try {
    const data = await api('/api/alerts?limit=80');
    const feed = document.getElementById('feed');
    if (!data.alerts.length) {
      feed.innerHTML = '<div class="empty-state">No alerts yet — system is monitoring.</div>';
      document.getElementById('feed-count').textContent = '0 alerts';
      return;
    }
    feed.innerHTML = data.alerts.map(renderAlert).join('');
    document.getElementById('feed-count').textContent = `${data.alerts.length} alerts (24h)`;
  } catch (e) {
    console.error('alerts fetch failed', e);
  }
}

function renderAlert(a) {
  const t = new Date(a.created_at);
  return `
    <div class="alert">
      <div class="alert-head">
        <span class="alert-type ${a.alert_type}">${a.alert_type === 'breach' ? '▲ BREACH' : '✓ RESOLVED'}</span>
        <span class="alert-time">${fmtClockIST(t)} IST</span>
      </div>
      <div class="alert-topic">${a.topic} · ${(a.environment||'').toUpperCase()}</div>
      <div class="alert-team">${a.team || ''} · ${a.channel || ''}</div>
      <div class="alert-lag">Lag: ${FMT_K(a.lag_value)}${a.delivered_to_slack ? '<span class="alert-delivered">DELIVERED ✓</span>' : ''}</div>
    </div>
  `;
}

// =============================================================================
// AI assistant
// =============================================================================
const AI_FAB = document.getElementById('ai-fab');
const AI_PANEL = document.getElementById('ai-panel');
const AI_CLOSE = document.getElementById('ai-close');
const AI_MSGS = document.getElementById('ai-msgs');
const AI_INPUT = document.getElementById('ai-input');
const AI_SEND = document.getElementById('ai-send');
const AI_SUGGEST = document.getElementById('ai-suggest');
const AI_STATUS_SUB = document.getElementById('ai-status-sub');

let _aiHistory = [];
let _aiBusy = false;
let _aiAvailable = null;

function _aiEscape(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function _aiRenderMarkdown(text) {
  let s = _aiEscape(text);
  s = s.replace(/```([\s\S]*?)```/g, (_, code) => `<pre>${code.replace(/^\n/,'')}</pre>`);
  s = s.replace(/`([^`\n]+)`/g, '<code>$1</code>');
  s = s.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
  const lines = s.split(/\n/);
  let out = '', inList = false;
  for (const ln of lines) {
    const m = /^\s*[-*]\s+(.*)$/.exec(ln);
    if (m) {
      if (!inList) { out += '<ul style="margin:6px 0;padding-left:20px;">'; inList = true; }
      out += `<li>${m[1]}</li>`;
    } else {
      if (inList) { out += '</ul>'; inList = false; }
      out += ln + '<br>';
    }
  }
  if (inList) out += '</ul>';
  return out.replace(/(<br>\s*){2,}/g, '<br><br>').replace(/<br>$/, '');
}
function _aiAddMessage(role, content, opts = {}) {
  const div = document.createElement('div');
  div.className = `ai-msg ${role}`;
  if (role === 'assistant') {
    div.innerHTML = _aiRenderMarkdown(content);
    if (opts.toolCalls && opts.toolCalls.length) {
      const tagDiv = document.createElement('div');
      tagDiv.style.cssText = 'margin-top:8px;padding-top:8px;border-top:1px dashed var(--line);';
      for (const tc of opts.toolCalls) {
        const tag = document.createElement('span');
        tag.className = 'ai-tool-tag';
        tag.textContent = tc.name;
        tagDiv.appendChild(tag);
      }
      div.appendChild(tagDiv);
    }
  } else {
    div.textContent = content;
  }
  AI_MSGS.appendChild(div);
  AI_MSGS.scrollTop = AI_MSGS.scrollHeight;
}
function _aiAddThinking() {
  const div = document.createElement('div');
  div.className = 'ai-thinking'; div.id = 'ai-thinking';
  div.innerHTML = '<span></span><span></span><span></span>';
  AI_MSGS.appendChild(div);
  AI_MSGS.scrollTop = AI_MSGS.scrollHeight;
}
function _aiRemoveThinking() {
  const t = document.getElementById('ai-thinking');
  if (t) t.remove();
}
async function _aiCheckAvailability() {
  try {
    const r = await fetch('/api/chat/health');
    const j = await r.json();
    _aiAvailable = !!j.available;
  } catch (_e) { _aiAvailable = false; }
  if (!_aiAvailable) {
    AI_FAB.classList.add('disabled');
    AI_FAB.title = 'Chatbot unavailable — set GEMINI_API_KEY in .env';
    AI_STATUS_SUB.textContent = 'Disabled — no LLM credentials configured';
    AI_STATUS_SUB.style.color = 'var(--red)';
    AI_SEND.disabled = true; AI_INPUT.disabled = true;
    AI_INPUT.placeholder = 'Set GEMINI_API_KEY in .env to enable';
  }
}
function _aiOpen() {
  AI_PANEL.classList.add('open');
  AI_PANEL.setAttribute('aria-hidden', 'false');
  if (_aiAvailable) setTimeout(() => AI_INPUT.focus(), 200);
}
function _aiClose() {
  AI_PANEL.classList.remove('open');
  AI_PANEL.setAttribute('aria-hidden', 'true');
}
async function _aiSend(message) {
  if (_aiBusy || !message || !_aiAvailable) return;
  _aiBusy = true; AI_SEND.disabled = true; AI_INPUT.value = '';
  AI_SUGGEST.style.display = 'none';
  _aiAddMessage('user', message);
  _aiHistory.push({ role: 'user', content: message });
  _aiAddThinking();
  try {
    const r = await fetch('/api/chat', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, history: _aiHistory.slice(0, -1) }),
    });
    _aiRemoveThinking();
    if (!r.ok) {
      _aiAddMessage('error', `Request failed (${r.status}): ${(await r.text()).slice(0,300)}`);
      return;
    }
    const data = await r.json();
    _aiAddMessage('assistant', data.reply, { toolCalls: data.tool_calls });
    _aiHistory.push({ role: 'assistant', content: data.reply });
    if (_aiHistory.length > 18) _aiHistory = _aiHistory.slice(-18);
  } catch (e) {
    _aiRemoveThinking();
    _aiAddMessage('error', `Network error: ${e.message}`);
  } finally {
    _aiBusy = false; AI_SEND.disabled = false; AI_INPUT.focus();
  }
}
AI_FAB.addEventListener('click', _aiOpen);
AI_CLOSE.addEventListener('click', _aiClose);
AI_SEND.addEventListener('click', () => {
  const m = AI_INPUT.value.trim(); if (m) _aiSend(m);
});
AI_INPUT.addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter' && !ev.shiftKey) {
    ev.preventDefault();
    const m = AI_INPUT.value.trim(); if (m) _aiSend(m);
  }
});
AI_INPUT.addEventListener('input', () => {
  AI_INPUT.style.height = 'auto';
  AI_INPUT.style.height = Math.min(AI_INPUT.scrollHeight, 120) + 'px';
});
AI_SUGGEST.addEventListener('click', (ev) => {
  const btn = ev.target.closest('button[data-q]');
  if (btn) _aiSend(btn.dataset.q);
});
document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape' && AI_PANEL.classList.contains('open')) _aiClose();
});

// =============================================================================
// Boot
// =============================================================================
function tickClock() {
  const d = new Date();
  document.getElementById('clock').textContent = fmtFullIST(d) + ' IST';
}

async function boot() {
  tickClock();
  setInterval(tickClock, 1000);

  // Load topics + panels
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

  // Read the backend poll interval so the frontend stays in sync with it.
  // This means the UI refreshes at the same rate the backend fetches from Prometheus —
  // no misleading fast polling, no stale data sitting longer than one backend cycle.
  let pollMs = 15000; // fallback until /api/health responds
  try {
    const health = await api('/api/health');
    pollMs = (health.ui_poll_interval_seconds || 15) * 1000;
    document.getElementById('poll-label').textContent = `Polling ${health.poll_interval_seconds}s`;
  } catch (_) {}

  // Background pollers
  refreshStatus();
  refreshAlerts();
  setInterval(refreshStatus, pollMs);
  setInterval(refreshAlerts, pollMs);
  setInterval(() => {
    if (STATE.view === 'detail') reloadAllPanels();
    else reloadAllSparklines();
  }, 120000);

  _aiCheckAvailability();
}

boot();

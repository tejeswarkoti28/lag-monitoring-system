// Shared state, API helper, formatters, time utilities

const STATE = {
  topics: [],
  environments: [],
  panels: [],
  jobs: [],
  jobs_by_id: {},
  job_state: {},
  summary: {},
  view: 'grid',
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
  if (abs >= 1e3) return Math.round(n / 1e3) + 'K';
  return Math.round(n).toString();
};

function _fmtCardLag(lag) {
  const abs = Math.abs(lag || 0);
  if (abs >= 1e9) return { v: (lag / 1e9).toFixed(2), u: 'B msgs' };
  if (abs >= 1e6) return { v: (lag / 1e6).toFixed(2), u: 'M msgs' };
  if (abs >= 1e3) return { v: (lag / 1e3).toFixed(2), u: 'K msgs' };
  return { v: Math.round(lag || 0).toString(), u: 'msgs' };
}

const IST_OFFSET_MIN = 330;
function _toIST(d) { return new Date(d.getTime() + IST_OFFSET_MIN * 60000); }
function fmtClockIST(d) {
  const i = _toIST(d);
  return String(i.getUTCHours()).padStart(2, '0') + ':' + String(i.getUTCMinutes()).padStart(2, '0');
}
function fmtFullIST(d) {
  const i = _toIST(d);
  const date = i.getUTCFullYear() + '-' + String(i.getUTCMonth() + 1).padStart(2, '0') + '-' + String(i.getUTCDate()).padStart(2, '0');
  return date + ' ' + fmtClockIST(d) + ':' + String(i.getUTCSeconds()).padStart(2, '0');
}
function fmtDateIST(d) {
  const i = _toIST(d);
  return String(i.getUTCMonth() + 1).padStart(2, '0') + '-' + String(i.getUTCDate()).padStart(2, '0');
}

function _chooseTimeStep(spanSec) {
  const STEPS = [60, 300, 600, 1800, 3600, 7200, 21600, 43200, 86400, 259200, 604800, 2592000];
  for (const s of STEPS) { if (spanSec / s <= 12) return s; }
  return STEPS[STEPS.length - 1];
}

function relTime(iso) {
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 5)    return 'just now';
  if (s < 60)   return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  return Math.floor(s / 3600) + 'h ago';
}

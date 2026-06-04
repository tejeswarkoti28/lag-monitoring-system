// Status strip, alert feed, Slack test button

async function refreshStatus() {
  try {
    const data = await api('/api/status');
    STATE.summary = data.summary || {};
    const ready = !!data.summary.last_poll_at;
    document.getElementById('stat-monitored').textContent = ready ? data.summary.monitored : '--';
    document.getElementById('stat-breaching').textContent = ready ? data.summary.breaching : '--';
    document.getElementById('stat-healthy').textContent   = ready ? data.summary.healthy   : '--';
    document.getElementById('stat-envs').textContent = STATE.environments.join(' + ');
    updateSlackIndicator(data.summary.slack_configured);
    let statusChanged = false;
    for (const j of (data.jobs || [])) {
      const prev = STATE.job_state[j.job_id] || {};
      if (prev.status !== j.status) statusChanged = true;
      STATE.job_state[j.job_id] = {
        ...prev,
        lag: ready ? (j.lag || 0) : null,
        status: ready ? j.status : 'loading',
      };
    }
    if (STATE.view === 'grid') {
      const lu = document.getElementById('grid-last-update');
      if (lu && data.summary.last_poll_at) lu.textContent = `Last update: ${relTime(data.summary.last_poll_at)}`;
      if (statusChanged) {
        renderTeamGrid();
        reloadAllSparklines();
      } else {
        for (const j of (data.jobs || [])) {
          const idx = STATE.jobs.findIndex(x => x.job_id === j.job_id);
          if (idx < 0) continue;
          const card = document.querySelector(`.job-card[data-idx="${idx}"]`);
          if (!card) continue;
          const valEl = card.querySelector('.card-value');
          if (valEl) {
            const f = ready ? _fmtCardLag(j.lag || 0) : { v: '--', u: '' };
            valEl.textContent = f.v;
            const unitEl = card.querySelector('.card-value-unit');
            if (unitEl) unitEl.textContent = f.u;
          }
        }
      }
    } else if (STATE.view === 'detail') {
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
      SLACK_TEST_BTN.textContent = `Sent ✓ (${(data.results || []).map(x => x.label).join(', ')})`;
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

async function refreshAlerts() {
  try {
    const data = await api('/api/alerts?limit=80');
    const feed = document.getElementById('feed');
    if (!data.alerts.length) {
      feed.innerHTML = '<div class="empty-state">No Slack breach alerts sent yet.</div>';
      document.getElementById('feed-count').textContent = '0 sent';
      return;
    }
    feed.innerHTML = data.alerts.map(renderAlert).join('');
    document.getElementById('feed-count').textContent = `${data.alerts.length} sent (24h)`;
  } catch (e) {
    console.error('alerts fetch failed', e);
  }
}

async function fetchBreachCount() {
  try {
    const data = await api('/api/breach-count?hours=24');
    document.getElementById('stat-alerts').textContent = data.breach_periods ?? '--';
  } catch (_) {}
}

function renderAlert(a) {
  const t = new Date(a.created_at);
  return `
    <div class="alert">
      <div class="alert-head">
        <span class="alert-type ${a.alert_type}">${a.alert_type === 'breach' ? '▲ BREACH' : '✓ RESOLVED'}</span>
        <span class="alert-time">${fmtClockIST(t)} IST</span>
      </div>
      <div class="alert-topic">${a.topic} · ${(a.environment || '').toUpperCase()}</div>
      <div class="alert-team">${a.team || ''} · ${a.channel || ''}</div>
      <div class="alert-lag">Lag: ${FMT_K(a.lag_value)}${a.delivered_to_slack ? '<span class="alert-delivered">DELIVERED ✓</span>' : ''}</div>
    </div>`;
}

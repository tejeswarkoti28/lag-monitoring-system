// Chart drawing: sparklines and Grafana-style SVG charts

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
  const svg = `<svg class="card-spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
    <path d="${d}" fill="none" stroke="var(--blue)" stroke-width="1.2" />
  </svg>`;
  container.outerHTML = svg.replace('class="card-spark"', `class="card-spark" id="${container.id}"`);
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

  const niceStep = _niceStep(yRange);
  let yAxis = '';
  for (let v = Math.ceil(yMin / niceStep) * niceStep; v <= yMax + 0.0001; v += niceStep) {
    const y = yOf(v);
    yAxis += `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${padL + innerW}" y2="${y.toFixed(1)}" stroke="#1a1f27" stroke-width="0.5" />`;
    yAxis += `<text x="${padL - 6}" y="${(y + 3.5).toFixed(1)}" fill="#8b949e" font-size="9.5" font-family="JetBrains Mono,monospace" text-anchor="end">${FMT_AXIS(v)}</text>`;
  }

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
    xAxis += `<line x1="${x.toFixed(1)}" y1="${padT}" x2="${x.toFixed(1)}" y2="${padT + innerH}" stroke="#1a1f27" stroke-width="0.5" />`;
    if (x - lastLabelX >= minLabelPx) {
      const label = stepSec >= 86400 ? fmtDateIST(new Date(t * 1000)) : fmtClockIST(new Date(t * 1000));
      xAxis += `<text x="${x.toFixed(1)}" y="${(padT + innerH + 14).toFixed(1)}" fill="#8b949e" font-size="9.5" font-family="JetBrains Mono,monospace" text-anchor="middle">${label}</text>`;
      lastLabelX = x;
    }
  }

  const linePath = points.map((p, i) =>
    (i ? 'L' : 'M') + xOf(p.ts).toFixed(1) + ',' + yOf(p.value).toFixed(1)
  ).join('');

  const lineColor = data.color || '#58a6ff';
  let breachFill = '', thresholdLine = '';
  const showThrUI = showThr && thr >= yMin && thr <= yMax;
  if (showThrUI) {
    const thrY = yOf(thr);
    let top = '';
    for (let i = 0; i < points.length; i++) {
      top += (i ? 'L' : 'M') + xOf(tsArr[i]).toFixed(1) + ',' + yOf(Math.max(valArr[i], thr)).toFixed(1);
    }
    let bottom = '';
    for (let i = points.length - 1; i >= 0; i--) {
      bottom += `L${xOf(tsArr[i]).toFixed(1)},${thrY.toFixed(1)}`;
    }
    const gradId = `gr-breach-${Math.random().toString(36).slice(2, 8)}`;
    breachFill = `
      <defs><linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#f85149" stop-opacity="0.05" />
        <stop offset="100%" stop-color="#f85149" stop-opacity="0.45" />
      </linearGradient></defs>
      <path d="${top + bottom}Z" fill="url(#${gradId})" stroke="none" />`;
    thresholdLine = `
      <line x1="${padL}" y1="${thrY.toFixed(1)}" x2="${padL + innerW}" y2="${thrY.toFixed(1)}" stroke="#f85149" stroke-width="1.2" stroke-dasharray="5,4" opacity="0.9" />
      <g transform="translate(${(padL + innerW - 92).toFixed(1)}, ${(thrY - 9).toFixed(1)})">
        <rect width="88" height="16" rx="3" fill="#1a0d0d" stroke="#6d2c2c" />
        <text x="44" y="11.5" fill="#f85149" font-size="10" font-family="JetBrains Mono,monospace" text-anchor="middle" font-weight="700">threshold ${FMT_AXIS(thr)}</text>
      </g>`;
  }

  const safePathTop = points.map((p, i) =>
    (i ? 'L' : 'M') + xOf(p.ts).toFixed(1) + ',' + yOf(showThrUI ? Math.min(p.value, thr) : p.value).toFixed(1)
  ).join('');
  const safeAreaPath = safePathTop + `L${xOf(tMax).toFixed(1)},${baselineY.toFixed(1)}L${xOf(tMin).toFixed(1)},${baselineY.toFixed(1)}Z`;
  const safeGradId = `gr-safe-${Math.random().toString(36).slice(2, 8)}`;
  const safeFill = `
    <defs><linearGradient id="${safeGradId}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="${lineColor}" stop-opacity="0.30" />
      <stop offset="100%" stop-color="${lineColor}" stop-opacity="0.04" />
    </linearGradient></defs>
    <path d="${safeAreaPath}" fill="url(#${safeGradId})" stroke="none" />`;

  const lastT = tsArr[tsArr.length - 1];
  const lastV = valArr[valArr.length - 1];
  const lastX = xOf(lastT), lastY = yOf(lastV);
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
      <text x="${calloutW / 2}" y="12" fill="${isBreaching ? '#f85149' : '#e6edf3'}" font-size="10" font-family="JetBrains Mono,monospace" text-anchor="middle" font-weight="700">${calloutLabel}</text>
    </g>`;

  svg.innerHTML = `
    ${yAxis}${xAxis}${safeFill}${breachFill}${thresholdLine}
    <path d="${linePath}" fill="none" stroke="${lineColor}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round" />
    ${callout}
    <text x="14" y="${(padT + innerH / 2).toFixed(1)}" fill="#8b949e" font-size="10.5" font-family="JetBrains Mono,monospace" text-anchor="middle" transform="rotate(-90, 14, ${(padT + innerH / 2).toFixed(1)})">Lag</text>
    <text x="${(padL + innerW / 2).toFixed(1)}" y="${(padT + innerH + 30).toFixed(1)}" fill="#8b949e" font-size="10.5" font-family="JetBrains Mono,monospace" text-anchor="middle">Time (IST)</text>`;
}

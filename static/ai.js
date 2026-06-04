// AI assistant panel

const AI_FAB       = document.getElementById('ai-fab');
const AI_PANEL     = document.getElementById('ai-panel');
const AI_CLOSE     = document.getElementById('ai-close');
const AI_MSGS      = document.getElementById('ai-msgs');
const AI_INPUT     = document.getElementById('ai-input');
const AI_SEND      = document.getElementById('ai-send');
const AI_SUGGEST   = document.getElementById('ai-suggest');
const AI_STATUS_SUB = document.getElementById('ai-status-sub');

let _aiHistory  = [];
let _aiBusy     = false;
let _aiAvailable = null;

function _aiEscape(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _aiRenderMarkdown(text) {
  let s = _aiEscape(text);
  s = s.replace(/```([\s\S]*?)```/g, (_, code) => `<pre>${code.replace(/^\n/, '')}</pre>`);
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
  div.className = 'ai-thinking';
  div.id = 'ai-thinking';
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
  } catch (_) { _aiAvailable = false; }
  if (!_aiAvailable) {
    AI_FAB.classList.add('disabled');
    AI_FAB.title = 'Chatbot unavailable — set GEMINI_API_KEY in .env';
    AI_STATUS_SUB.textContent = 'Disabled — no LLM credentials configured';
    AI_STATUS_SUB.style.color = 'var(--red)';
    AI_SEND.disabled = true;
    AI_INPUT.disabled = true;
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
  _aiBusy = true;
  AI_SEND.disabled = true;
  AI_INPUT.value = '';
  AI_SUGGEST.style.display = 'none';
  _aiAddMessage('user', message);
  _aiHistory.push({ role: 'user', content: message });
  _aiAddThinking();
  try {
    const r = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, history: _aiHistory.slice(0, -1) }),
    });
    _aiRemoveThinking();
    if (!r.ok) {
      _aiAddMessage('error', `Request failed (${r.status}): ${(await r.text()).slice(0, 300)}`);
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
    _aiBusy = false;
    AI_SEND.disabled = false;
    AI_INPUT.focus();
  }
}

AI_FAB.addEventListener('click', _aiOpen);
AI_CLOSE.addEventListener('click', _aiClose);
AI_SEND.addEventListener('click', () => { const m = AI_INPUT.value.trim(); if (m) _aiSend(m); });
AI_INPUT.addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); const m = AI_INPUT.value.trim(); if (m) _aiSend(m); }
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

/* Ultra RPi -- Firmware tab */
(function () {
  'use strict';
  const { $ } = window.Ultra;

  let fwPollTimer = null;
  let fwLogOffset = 0;

  function formatBytes(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) {
      return (bytes / 1024).toFixed(1) + ' KB';
    }
    return (bytes / 1048576).toFixed(1) + ' MB';
  }

  async function loadFirmwareList() {
    const tbody = $('#fw-tbody');
    tbody.innerHTML = '<tr><td colspan="5" '
      + 'class="fw-empty">Loading...</td></tr>';
    try {
      const r = await fetch('/api/firmware/list');
      if (!r.ok) {
        const e = await r.json().catch(() => ({}));
        tbody.innerHTML = '<tr><td colspan="5" '
          + 'class="fw-empty">Error: '
          + (e.detail || r.statusText)
          + '</td></tr>';
        return;
      }
      const builds = await r.json();
      if (!builds.length) {
        tbody.innerHTML = '<tr><td colspan="5" '
          + 'class="fw-empty">'
          + 'No firmware found in S3</td></tr>';
        return;
      }
      tbody.innerHTML = '';
      for (const b of builds) {
        const tr = document.createElement('tr');
        if (b.is_latest) {
          tr.classList.add('fw-latest');
        }
        const dateStr = b.date
          ? new Date(b.date).toLocaleString()
          : '--';
        const label = b.is_latest
          ? b.version + ' (latest)' : b.version;
        const notes = b.notes || '';
        const esc = notes
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;');
        tr.innerHTML =
          `<td class="fw-ver">${label}</td>`
          + `<td>${dateStr}</td>`
          + `<td>${formatBytes(b.size)}</td>`
          + `<td class="fw-notes">${esc}</td>`
          + `<td><button class="btn btn-sm`
          + ` fw-flash-btn`
          + `" data-key="${b.key}">Flash</button></td>`;
        tbody.appendChild(tr);
      }
      tbody.querySelectorAll('.fw-flash-btn')
        .forEach((btn) => {
          btn.addEventListener('click', () => {
            flashFirmware(btn.dataset.key);
          });
        });
    } catch (err) {
      tbody.innerHTML = '<tr><td colspan="5" '
        + 'class="fw-empty">Error: '
        + err.message + '</td></tr>';
    }
  }

  async function flashFirmware(key) {
    const ver = key.split('/').pop()
      .replace('_ultra_mcu.bin', '');
    if (!confirm(
      `Flash firmware ${ver}?\n\n`
      + 'This will disconnect the STM32 and '
      + 'reprogram it. Do not power off during '
      + 'the flash.',
    )) return;

    $('#fw-log').textContent = '';
    fwLogOffset = 0;
    updateFlashUI(
      'downloading', 0, 'Starting...', [],
    );

    document.querySelectorAll('.fw-flash-btn')
      .forEach((b) => { b.disabled = true; });

    try {
      const r = await fetch('/api/firmware/flash', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({key}),
      });
      if (!r.ok) {
        const e = await r.json().catch(() => ({}));
        let detail = e.detail || 'Request failed';
        if (typeof detail !== 'string') {
          detail = JSON.stringify(detail);
        }
        updateFlashUI('error', 0, detail, []);
        document.querySelectorAll('.fw-flash-btn')
          .forEach((b) => { b.disabled = false; });
        return;
      }
    } catch (err) {
      updateFlashUI(
        'error', 0, err.message, [],
      );
      document.querySelectorAll('.fw-flash-btn')
        .forEach((b) => { b.disabled = false; });
      return;
    }

    startFlashPoll();
  }

  function startFlashPoll() {
    if (fwPollTimer) return;
    fwPollTimer = setInterval(pollFlashStatus, 1000);
    pollFlashStatus();
  }

  function stopFlashPoll() {
    if (fwPollTimer) {
      clearInterval(fwPollTimer);
      fwPollTimer = null;
    }
  }

  async function pollFlashStatus() {
    try {
      const r = await fetch(
        '/api/firmware/status?log_offset='
        + fwLogOffset,
      );
      if (!r.ok) return;
      const d = await r.json();
      updateFlashUI(
        d.state, d.progress, d.message, d.log,
      );
      fwLogOffset = d.log_total;
      if (d.state === 'done' || d.state === 'error') {
        stopFlashPoll();
        document.querySelectorAll('.fw-flash-btn')
          .forEach((b) => { b.disabled = false; });
      }
    } catch (_) { /* ignore */ }
  }

  function updateFlashUI(state, progress, msg, log) {
    const stateEl = $('#fw-state');
    const msgEl = $('#fw-message');
    const fillEl = $('#fw-progress-fill');
    const pctEl = $('#fw-progress-pct');
    const logEl = $('#fw-log');

    stateEl.textContent = state.toUpperCase();
    stateEl.className = 'fw-state fw-state-' + state;
    msgEl.textContent = typeof msg === 'string'
      ? msg : JSON.stringify(msg);
    fillEl.style.width = progress + '%';
    pctEl.textContent = progress + '%';

    if (log && log.length) {
      for (const line of log) {
        logEl.textContent += line + '\n';
      }
      logEl.scrollTop = logEl.scrollHeight;
    }
  }

  function initFirmware() {
    const refreshBtn = $('#fw-refresh');
    if (refreshBtn) {
      refreshBtn.addEventListener(
        'click', loadFirmwareList,
      );
    }
  }

  Ultra.initFirmware = initFirmware;
})();

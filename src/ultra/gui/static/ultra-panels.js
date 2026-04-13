/* Ultra RPi -- Camera, egress, and log floating panels */
(function () {
  'use strict';
  const { $ } = window.Ultra;

  const elBtnCamera = $('#btn-camera');
  const elCameraPanel = $('#camera-panel');
  const elCameraFeed = $('#camera-feed');
  const elBtnCameraClose = $('#btn-camera-close');
  const elBtnEgress = $('#btn-egress');
  const elEgressPanel = $('#egress-panel');
  const elEgressTbody = $('#egress-tbody');
  const elBtnEgressClose = $('#btn-egress-close');
  const elBtnEgressClear = $('#btn-egress-clear');
  const elBtnEgressClearUpl = $(
    '#btn-egress-clear-uploaded',
  );
  const elBtnLogs = $('#btn-logs');
  const elLogsPanel = $('#logs-panel');
  const elLogsContent = $('#logs-content');
  const elBtnLogsClear = $('#btn-logs-clear');
  const elBtnLogsClose = $('#btn-logs-close');

  /* ---- Draggable panels ---- */
  function makeDraggable(panel, handle) {
    let ox = 0, oy = 0, sx = 0, sy = 0;
    handle.addEventListener('mousedown', (e) => {
      if (e.target.closest('button')) return;
      e.preventDefault();
      sx = e.clientX;
      sy = e.clientY;
      const onMove = (ev) => {
        ox = ev.clientX - sx;
        oy = ev.clientY - sy;
        sx = ev.clientX;
        sy = ev.clientY;
        const t = panel.offsetTop + oy;
        const l = panel.offsetLeft + ox;
        panel.style.top = t + 'px';
        panel.style.left = l + 'px';
        panel.style.right = 'auto';
      };
      const onUp = () => {
        document.removeEventListener(
          'mousemove', onMove,
        );
        document.removeEventListener(
          'mouseup', onUp,
        );
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  /* ---- Camera ---- */
  function initCamera() {
    function toggleCamera(show) {
      if (show) {
        elCameraPanel.hidden = false;
        elCameraFeed.src = '/api/camera/stream';
        elBtnCamera.classList.add('active');
      } else {
        elCameraPanel.hidden = true;
        elCameraFeed.src = '';
        elBtnCamera.classList.remove('active');
      }
    }
    elBtnCamera.onclick = () => {
      toggleCamera(elCameraPanel.hidden);
    };
    elBtnCameraClose.onclick = () => {
      toggleCamera(false);
    };
    makeDraggable(
      elCameraPanel,
      elCameraPanel.querySelector('.camera-header'),
    );
  }

  /* ---- Egress ---- */
  let egressPanelOpen = false;

  function initEgress() {
    fetchEgressStatus();

    elBtnEgress.onclick = () => {
      egressPanelOpen = !egressPanelOpen;
      if (egressPanelOpen) {
        elEgressPanel.hidden = false;
        fetchEgressRuns();
      } else {
        elEgressPanel.hidden = true;
      }
    };
    elBtnEgressClose.onclick = () => {
      egressPanelOpen = false;
      elEgressPanel.hidden = true;
    };
    elBtnEgressClear.onclick = async () => {
      if (!confirm('Clear ALL egress records?')) return;
      try {
        await fetch('/api/egress/clear', {
          method: 'POST',
        });
        fetchEgressRuns();
        fetchEgressStatus().then(applyEgressSummary);
      } catch (e) {
        console.warn('Failed to clear egress', e);
      }
    };
    elBtnEgressClearUpl.onclick = async () => {
      try {
        await fetch('/api/egress/clear_uploaded', {
          method: 'POST',
        });
        fetchEgressRuns();
        fetchEgressStatus().then(applyEgressSummary);
      } catch (e) {
        console.warn('Failed to clear uploaded', e);
      }
    };
    makeDraggable(
      elEgressPanel,
      elEgressPanel.querySelector(
        '.egress-panel-header',
      ),
    );
  }

  async function fetchEgressStatus() {
    try {
      const res = await fetch('/api/egress/status');
      const s = await res.json();
      applyEgressSummary(s);
    } catch (e) {
      console.warn('Failed to load egress status', e);
    }
  }

  function applyEgressSummary(s) {
    const btn = elBtnEgress;
    btn.classList.remove(
      'egress-idle', 'egress-uploading',
      'egress-done', 'egress-error',
    );
    const pending = s.pending || 0;
    const errored = s.errored || 0;
    const total = s.total || 0;
    const egressed = s.egressed || 0;

    if (total === 0) {
      btn.textContent = 'Egress: --';
      btn.classList.add('egress-idle');
    } else if (errored > 0) {
      btn.textContent = `Egress: ${errored} err`;
      btn.classList.add('egress-error');
    } else if (pending > 0) {
      btn.textContent = `Egress: ${pending} pending`;
      btn.classList.add('egress-uploading');
    } else {
      btn.textContent =
        `Egress: ${egressed}/${total} done`;
      btn.classList.add('egress-done');
    }
  }

  function updateEgressButton(data, evtType) {
    applyEgressSummary(data);
    if (egressPanelOpen) fetchEgressRuns();
  }

  async function fetchEgressRuns() {
    try {
      const res = await fetch('/api/egress/runs');
      const runs = await res.json();
      renderEgressRuns(runs);
    } catch (e) {
      console.warn('Failed to load egress runs', e);
    }
  }

  function renderEgressRuns(runs) {
    elEgressTbody.innerHTML = '';
    if (!runs.length) {
      const tr = document.createElement('tr');
      tr.innerHTML =
        '<td colspan="4" '
        + 'style="text-align:center;color:var(--text-dim)"'
        + '>No runs</td>';
      elEgressTbody.appendChild(tr);
      return;
    }
    for (const r of runs) {
      const tr = document.createElement('tr');
      const dt = r.rundate_ts
        ? r.rundate_ts.slice(0, 19).replace('T', ' ')
        : '--';
      const uuid = r.run_uuid
        ? r.run_uuid.slice(0, 8)
        : '--';
      const dir = r.run_dir_path || '';
      const parts = dir.split('/');
      const chip = parts.length > 1
        ? parts[parts.length - 2] : uuid;

      let stClass, stLabel;
      if (r.is_egressed) {
        stClass = 'egress-status-done';
        stLabel = 'done';
      } else if (r.egress_errors > 0) {
        stClass = 'egress-status-error';
        stLabel = `error (${r.egress_errors})`;
      } else {
        stClass = 'egress-status-pending';
        stLabel = 'pending';
      }

      const runId = r.run_id != null
        ? r.run_id : '--';
      tr.innerHTML =
        `<td title="${r.rundate_ts || ''}">${dt}</td>`
        + `<td>${runId}</td>`
        + `<td title="${dir}">`
        + `${chip}<br>`
        + `<small>${uuid}</small></td>`
        + `<td class="${stClass}">${stLabel}</td>`;
      elEgressTbody.appendChild(tr);
    }
  }

  /* ---- Log Panel ---- */
  let logsPanelOpen = false;
  const LOG_MAX_LINES = 500;
  const logBuffer = [];

  function initLogs() {
    fetchLogs();
    elBtnLogs.onclick = () => {
      logsPanelOpen = !logsPanelOpen;
      elLogsPanel.hidden = !logsPanelOpen;
      if (logsPanelOpen) renderLogBuffer();
    };
    elBtnLogsClose.onclick = () => {
      logsPanelOpen = false;
      elLogsPanel.hidden = true;
    };
    elBtnLogsClear.onclick = () => {
      logBuffer.length = 0;
      elLogsContent.textContent = '';
    };
    makeDraggable(
      elLogsPanel,
      elLogsPanel.querySelector('.logs-panel-header'),
    );
  }

  async function fetchLogs() {
    try {
      const res = await fetch('/api/logs');
      const data = await res.json();
      const lines = data.lines || [];
      logBuffer.length = 0;
      logBuffer.push(...lines);
      if (logsPanelOpen) renderLogBuffer();
    } catch (e) {
      console.warn('Failed to fetch logs', e);
    }
  }

  function appendLogLine(line) {
    logBuffer.push(line);
    if (logBuffer.length > LOG_MAX_LINES) {
      logBuffer.splice(
        0, logBuffer.length - LOG_MAX_LINES,
      );
    }
    if (!logsPanelOpen) return;
    elLogsContent.textContent += line + '\n';
    scrollLogsToBottom();
  }

  function renderLogBuffer() {
    elLogsContent.textContent = (
      logBuffer.join('\n')
    );
    scrollLogsToBottom();
  }

  function scrollLogsToBottom() {
    elLogsContent.scrollTop = (
      elLogsContent.scrollHeight
    );
  }

  Ultra.makeDraggable = makeDraggable;
  Ultra.updateEgressButton = updateEgressButton;
  Ultra.appendLogLine = appendLogLine;
  Ultra.initPanels = function () {
    initCamera();
    initEgress();
    initLogs();
  };
})();

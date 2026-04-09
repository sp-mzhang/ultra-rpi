/* Ultra RPi -- WebSocket client + GUI logic */
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);

  /* ---- State ---- */
  let ws = null;
  let wellDefs = {};
  let isPaused = false;
  let isRunning = false;
  let stepManifest = [];

  /* ---- Elements ---- */
  const elRecipe = $('#recipe-select');
  const elChipId = $('#chip-id');
  const elNote = $('#run-note');
  const elBtnRun = $('#btn-run');
  const elBtnPause = $('#btn-pause');
  const elBtnResume = $('#btn-resume');
  const elBtnAbort = $('#btn-abort');
  const elBtnNewRun = $('#btn-new-run');
  const elBtnSmStart = $('#btn-sm-start');
  const elBtnSmStop = $('#btn-sm-stop');
  const elPhase = $('#step-phase');
  const elLabel = $('#step-label');
  const elTip = $('#tip-badge');
  const elBar = $('#progress-bar');
  const elElapsed = $('#elapsed');
  const elMode = $('#mode-indicator');
  const elMachine = $('#machine-name');
  const elGrid = $('#wells-grid');
  const elStepList = $('#step-list');
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

  /* ---- Init ---- */
  async function init() {
    await loadRecipes();
    await loadQuickRunDefaults();
    await loadStatus();
    connectWS();
    initTabs();
    initSidebar();
    initCharts();
    initCamera();
    initEgress();
    initLogs();
  }

  async function loadRecipes() {
    try {
      const res = await fetch('/api/recipes');
      const list = await res.json();
      elRecipe.innerHTML = '';
      list.forEach((r) => {
        const opt = document.createElement('option');
        opt.value = r.file;
        opt.textContent = r.name;
        elRecipe.appendChild(opt);
      });
    } catch (e) {
      console.warn('Failed to load recipes', e);
    }
  }

  async function loadQuickRunDefaults() {
    try {
      const res = await fetch('/api/quick_run');
      const qr = await res.json();
      if (!qr.enabled) return;
      if (qr.protocol && elRecipe.options.length) {
        for (const opt of elRecipe.options) {
          if (opt.textContent === qr.protocol
              || opt.value === qr.protocol) {
            opt.selected = true;
            break;
          }
        }
      }
      if (qr.chip_id) elChipId.value = qr.chip_id;
    } catch (e) {
      console.warn('Failed to load quick_run', e);
    }
  }

  async function loadStatus() {
    try {
      const res = await fetch('/api/status');
      const s = await res.json();
      completedSteps = s.step_index || 0;
      updateProgress(s);
      if (s.wells && Object.keys(s.wells).length) {
        wellDefs = s.wells;
        renderWells(s.wells);
      }
      if (s.tip) updateTip(s.tip);
      updateMode(s.sm_state || 'inactive');
      updateButtons(s.is_running, s.is_paused);
      if (!s.is_running && s.step_index > 0) {
        showNewRun();
      }
      if (s.machine_name) {
        elMachine.textContent = s.machine_name;
      }
    } catch (e) {
      console.warn('Failed to load status', e);
    }
  }

  /* ---- WebSocket ---- */
  function connectWS() {
    const proto = location.protocol === 'https:'
      ? 'wss' : 'ws';
    ws = new WebSocket(
      `${proto}://${location.host}/ws`
    );
    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        handleEvent(msg.type, msg.data);
      } catch (e) {
        console.warn('WS parse error', e);
      }
    };
    ws.onclose = () => {
      console.log('WS closed, reconnecting in 3s');
      setTimeout(connectWS, 3000);
    };
    ws.onerror = () => ws.close();
  }

  function handleEvent(type, data) {
    switch (type) {
      case 'step_changed':
        updateProgress(data);
        updateStepList(data);
        break;
      case 'well_updated':
        updateWell(data);
        break;
      case 'wells_initialized':
        renderWells(data);
        break;
      case 'tip_changed':
        updateTip(data);
        break;
      case 'peak_data':
        addPeakPoint(data);
        break;
      case 'sweep_data':
        updateSpectrum(data);
        break;
      case 'protocol_paused':
        updateButtons(true, true);
        elLabel.textContent = 'PAUSED: '
          + (data.step_label || '');
        markStepListPaused(true);
        break;
      case 'protocol_resumed':
        updateButtons(true, false);
        markStepListPaused(false);
        break;
      case 'protocol_started':
        completedSteps = 0;
        updateButtons(true, false);
        clearTimingMarkers();
        if (data.steps) buildStepList(data.steps);
        break;
      case 'timing_marker':
        addTimingMarker(data);
        break;
      case 'egress_started':
      case 'egress_done':
      case 'egress_error':
        updateEgressButton(data, type);
        break;
      case 'protocol_done':
      case 'protocol_error':
      case 'protocol_aborted':
        updateButtons(false, false);
        elLabel.textContent = type.replace('_', ' ');
        markStepListPaused(false);
        showNewRun();
        break;
      case 'status_changed':
        updateMode(data.state || 'inactive');
        break;
      case 'log_line':
        appendLogLine(data.line || '');
        break;
    }
  }

  /* ---- Step List ---- */

  function buildStepList(steps) {
    stepManifest = steps;
    elStepList.innerHTML = '';
    steps.forEach((s) => {
      const row = document.createElement('div');
      row.className = 'step-item';
      row.id = 'step-row-' + s.index;
      row.dataset.index = s.index;
      row.dataset.expectedTip = s.expected_tip;
      row.innerHTML =
        '<span class="step-num">' + s.index + '</span>'
        + '<span class="step-phase-tag">'
        + s.phase + '</span>'
        + '<span class="step-lbl">'
        + escHtml(s.label) + '</span>'
        + '<span class="step-status"></span>';
      row.addEventListener('click', onStepClick);
      elStepList.appendChild(row);
    });
  }

  function updateStepList(d) {
    const idx = d.step || d.step_index || 0;
    if (!idx) return;

    if (d.completed) {
      const row = document.getElementById(
        'step-row-' + idx
      );
      if (row) {
        row.classList.remove('active', 'paused');
        row.classList.add(
          d.ok === false ? 'failed' : 'completed'
        );
      }
    } else {
      const prev = elStepList.querySelector(
        '.step-item.active'
      );
      if (prev) prev.classList.remove('active');
      const row = document.getElementById(
        'step-row-' + idx
      );
      if (row) {
        row.classList.add('active');
        row.scrollIntoView({
          block: 'nearest',
          behavior: 'smooth',
        });
      }
    }
  }

  function markStepListPaused(paused) {
    const active = elStepList.querySelector(
      '.step-item.active'
    );
    if (active) {
      active.classList.toggle('paused', paused);
    }
    elStepList.querySelectorAll('.step-item')
      .forEach((el) => {
        el.classList.toggle('clickable', paused);
      });
  }

  async function onStepClick(e) {
    if (!isPaused) return;
    const row = e.currentTarget;
    const idx = parseInt(row.dataset.index, 10);
    const lbl = row.querySelector('.step-lbl')
      .textContent;
    const expTip = row.dataset.expectedTip || '0';

    const msg = 'Restart from step ' + idx
      + ': ' + lbl
      + '?\nExpected tip: ' + expTip;
    if (!confirm(msg)) return;

    try {
      const res = await fetch('/api/restart_from', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ step_index: idx }),
      });
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Restart failed');
      }
    } catch (err) {
      alert('Restart request failed: ' + err.message);
    }
  }

  function escHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  /* ---- UI Updates ---- */
  let completedSteps = 0;

  function updateProgress(d) {
    const step = d.step || d.step_index || 0;
    const total = d.total || d.step_total || 0;

    if (d.completed) {
      completedSteps = step;
    } else {
      elPhase.textContent = d.phase || '--';
      elLabel.textContent = d.label
        || d.step_label || 'Idle';
    }

    const pct = total
      ? (completedSteps / total * 100) : 0;
    elBar.style.width = pct + '%';
    elBar.textContent =
      `${completedSteps} / ${total}`;
    if (d.elapsed_s !== undefined) {
      elElapsed.textContent =
        `Elapsed: ${d.elapsed_s.toFixed(1)}s`;
    }
  }

  function updateWell(d) {
    const el = document.getElementById(
      'well-' + d.name
    );
    if (!el) return;
    const volEl = el.querySelector('.well-vol');
    const fillEl = el.querySelector('.fill-bar-inner');
    if (volEl) {
      volEl.textContent =
        `${d.current_volume_ul.toFixed(0)} \u00b5L`;
    }
    const init = wellDefs[d.name];
    if (init && fillEl) {
      const initVol = init.initial_volume_ul || 1;
      const pct = Math.max(
        0,
        Math.min(
          100, d.current_volume_ul / initVol * 100,
        ),
      );
      fillEl.style.width = pct + '%';
    }
  }

  function updateTip(d) {
    const id = d.current_tip_id || 0;
    elTip.textContent = id
      ? `Tip: ${id}` : 'Tip: none';
  }

  function updateMode(state) {
    if (state === 'inactive') {
      elMode.textContent = 'Manual Mode';
      elMode.className = 'badge badge-blue';
      elBtnSmStart.disabled = false;
      elBtnSmStop.disabled = true;
    } else {
      elMode.textContent = 'SM: ' + state;
      elMode.className = 'badge badge-green';
      elBtnSmStart.disabled = true;
      elBtnSmStop.disabled = false;
    }
  }

  function updateButtons(running, paused) {
    isRunning = running;
    isPaused = paused;
    elBtnRun.disabled = running;
    elBtnPause.disabled = !running || paused;
    elBtnResume.disabled = !running || !paused;
    elBtnAbort.disabled = !running;
    elBtnRun.textContent = 'Run';
    elBtnRun.classList.remove('btn-starting');
    if (running) {
      elBtnNewRun.style.display = 'none';
      elBtnRun.style.display = '';
    }
  }

  function showNewRun() {
    elBtnRun.style.display = 'none';
    elBtnNewRun.style.display = '';
  }

  function renderWells(wells) {
    wellDefs = wells;
    elGrid.innerHTML = '';
    const names = Object.keys(wells).sort();
    names.forEach((name) => {
      const w = wells[name];
      const card = document.createElement('div');
      card.className = 'well-card';
      card.id = 'well-' + name;
      const initVol = w.initial_volume_ul || 0;
      const curVol = w.current_volume_ul || 0;
      const pct = initVol
        ? (curVol / initVol * 100) : 0;
      card.innerHTML = `
        <div class="well-name">${name}</div>
        <div class="well-reagent"
             title="${w.reagent}">${w.reagent}</div>
        <div class="well-vol">
          ${curVol.toFixed(0)} \u00b5L
        </div>
        <div class="fill-bar">
          <div class="fill-bar-inner"
               style="width:${pct}%"></div>
        </div>
      `;
      elGrid.appendChild(card);
    });
  }

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

  /* ---- Timing Markers ---- */
  const sgMarkers = [];

  function addTimingMarker(d) {
    const t = d.elapsed_s;
    const label = d.label || '';
    const evtType = d.event_type || 'start';
    sgMarkers.push({
      x: t, label: label, event_type: evtType,
    });
    if (!sgChart) return;
    const id = 'marker_' + sgMarkers.length;
    if (!sgChart.options.plugins.annotation) {
      sgChart.options.plugins.annotation = {
        annotations: {},
      };
    }
    let color = 'rgba(255,255,255,0.5)';
    if (evtType === 'start') color = 'rgba(255,80,80,0.7)';
    else if (evtType === 'stop') color = 'rgba(80,140,255,0.7)';
    sgChart.options.plugins.annotation
      .annotations[id] = {
      type: 'line',
      scaleID: 'x',
      value: t,
      borderColor: color,
      borderWidth: 1,
      borderDash: [4, 3],
      label: {
        display: !!label,
        content: label + ' (' + evtType + ')',
        rotation: 'auto',
        position: 'start',
        backgroundColor: 'rgba(0,0,0,0.6)',
        color: '#fff',
        font: { size: 9 },
        padding: 2,
      },
    };
    sgDirty = true;
  }

  function clearTimingMarkers() {
    sgMarkers.length = 0;
    if (sgChart && sgChart.options.plugins.annotation) {
      sgChart.options.plugins.annotation.annotations = {};
    }
  }

  /* ========================================================
   * Charts
   * ========================================================
   * Sensorgram: peak wavelength (nm) vs time (s)
   * Spectrum:   power (dB) vs wavelength (nm)
   * Both share a single tabbed container and a shared
   * channel sidebar with 15 pre-built buttons.
   * ====================================================== */

  const NUM_CHANNELS = 15;
  const COLORS = [
    '#1F77B4', '#FF7F0E', '#2CA02C', '#D62728',
    '#9467BD', '#8C564B', '#E377C2', '#7F7F7F',
    '#BCBD22', '#17BECF', '#9EDAE5', '#FFBB78',
    '#98DF8A', '#FF9896', '#C5B0D5',
  ];

  let activeTab = 'spectrum';
  const channelVisible = new Array(NUM_CHANNELS)
    .fill(true);
  const chButtons = [];

  /* ---------- Tabs ---------- */

  function initTabs() {
    document.querySelectorAll('.tab-btn')
      .forEach((btn) => {
        btn.addEventListener('click', () => {
          switchTab(btn.dataset.tab);
        });
      });
  }

  function switchTab(tab) {
    activeTab = tab;
    document.querySelectorAll('.tab-btn')
      .forEach((b) => {
        b.classList.toggle(
          'active', b.dataset.tab === tab,
        );
      });

    const spCanvas = $('#spectrum-canvas');
    const sgCanvas = $('#sensorgram-canvas');
    const tbSp = $('#toolbar-spectrum');
    const tbSg = $('#toolbar-sensorgram');

    if (tab === 'spectrum') {
      spCanvas.style.display = '';
      sgCanvas.style.display = 'none';
      tbSp.hidden = false;
      tbSg.hidden = true;
      if (spChart) spChart.resize();
    } else {
      spCanvas.style.display = 'none';
      sgCanvas.style.display = '';
      tbSp.hidden = true;
      tbSg.hidden = false;
      if (sgChart) sgChart.resize();
    }
  }

  /* ---------- Channel sidebar ---------- */

  function initSidebar() {
    const grid = $('#ch-grid');
    for (let i = 0; i < NUM_CHANNELS; i++) {
      const btn = document.createElement('button');
      btn.className = 'ch-btn';
      btn.textContent = '' + (i + 1);
      btn.style.background =
        COLORS[i % COLORS.length];
      btn.addEventListener('click', () => {
        toggleChannel(i);
      });
      grid.appendChild(btn);
      chButtons.push(btn);
    }

    $('#ch-all').addEventListener('click', () => {
      setAllChannels(true);
    });
    $('#ch-none').addEventListener('click', () => {
      setAllChannels(false);
    });
  }

  function toggleChannel(idx) {
    channelVisible[idx] = !channelVisible[idx];
    syncChannelVisibility(idx);
  }

  function setAllChannels(visible) {
    for (let i = 0; i < NUM_CHANNELS; i++) {
      channelVisible[i] = visible;
    }
    syncAllChannelVisibility();
  }

  function syncChannelVisibility(idx) {
    const show = channelVisible[idx];
    chButtons[idx].classList.toggle('off', !show);
    applyVisibility(sgChart, idx, show);
    applyVisibility(spChart, idx, show);
    sgDirty = true;
    spDirty = true;
  }

  function syncAllChannelVisibility() {
    for (let i = 0; i < NUM_CHANNELS; i++) {
      const show = channelVisible[i];
      chButtons[i].classList.toggle('off', !show);
      applyVisibility(sgChart, i, show);
      applyVisibility(spChart, i, show);
    }
    sgDirty = true;
    spDirty = true;
  }

  function applyVisibility(chart, chIdx, show) {
    if (!chart) return;
    const ds = chart.data.datasets[chIdx];
    if (!ds) return;
    const meta = chart.getDatasetMeta(chIdx);
    meta.hidden = !show;
    ds.hidden = !show;
  }

  /* ---------- Sensorgram (time-series) ---------- */
  let sgChart = null;
  const sgRaw = {};
  const sgBaselines = {};
  let sgFrozen = false;
  let sgAlignY = false;
  let sgStartX = 0;
  let sgDirty = false;

  function initSensorgram() {
    const ctx = $('#sensorgram-canvas')
      .getContext('2d');
    sgChart = new Chart(ctx, {
      type: 'line',
      data: { datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            type: 'linear',
            title: {
              display: true, text: 'Time (s)',
              color: '#8b8fa3',
            },
            grid: { color: '#2a2d3a' },
            ticks: { color: '#8b8fa3' },
          },
          yPeak: {
            position: 'left',
            title: {
              display: true,
              text: 'Wavelength (nm)',
              color: '#1F77B4',
            },
            grid: { color: '#2a2d3a' },
            ticks: {
              color: '#8b8fa3',
              callback: (v) => v.toFixed(4),
            },
          },
        },
        plugins: {
          legend: { display: false },
          annotation: { annotations: {} },
          zoom: {
            pan: { enabled: true, mode: 'xy' },
            zoom: {
              wheel: { enabled: true },
              pinch: { enabled: true },
              mode: 'xy',
            },
          },
        },
      },
    });

    for (let i = 0; i < NUM_CHANNELS; i++) {
      const color = COLORS[i % COLORS.length];
      sgChart.data.datasets.push({
        label: '' + (i + 1),
        data: [],
        borderColor: color,
        borderWidth: 1.5,
        pointRadius: 2,
        pointBackgroundColor: color,
        tension: 0,
        yAxisID: 'yPeak',
        hidden: false,
        _rawKey: 'ch' + (i + 1),
      });
      sgRaw['ch' + (i + 1)] = [];
    }
    bindSgControls();
  }

  function bindSgControls() {
    const elStartX = $('#sg-start-x');
    const elAlignY = $('#sg-align-y');
    const elFreeze = $('#sg-freeze');
    const elReset = $('#sg-reset-zoom');
    const elClear = $('#sg-clear');

    elStartX.addEventListener('change', () => {
      sgStartX = parseFloat(elStartX.value) || 0;
      sgChart.options.scales.x.min =
        sgStartX || undefined;
      if (sgAlignY) sgRecomputeAlign();
      sgDirty = true;
    });

    elAlignY.onclick = () => {
      sgAlignY = !sgAlignY;
      elAlignY.classList.toggle('active', sgAlignY);
      if (sgAlignY) {
        sgRecomputeAlign();
      } else {
        sgRestoreRaw();
      }
      sgDirty = true;
    };

    elFreeze.onclick = () => {
      sgFrozen = !sgFrozen;
      elFreeze.classList.toggle('active', sgFrozen);
      elFreeze.textContent = sgFrozen
        ? 'Frozen' : 'Freeze';
    };

    elReset.onclick = () => sgChart.resetZoom();

    elClear.onclick = () => {
      for (let i = 0; i < NUM_CHANNELS; i++) {
        const key = 'ch' + (i + 1);
        sgRaw[key] = [];
        delete sgBaselines[key];
        sgChart.data.datasets[i].data = [];
      }
      sgChart.update('none');
    };
  }

  function sgRecomputeAlign() {
    sgChart.data.datasets.forEach((ds) => {
      const key = ds._rawKey;
      const raw = sgRaw[key];
      if (!raw || !raw.length) return;
      const ref = nearestY(raw, sgStartX);
      sgBaselines[key] = ref;
      ds.data = raw.map(
        (p) => ({ x: p.x, y: p.y - ref }),
      );
    });
  }

  function sgRestoreRaw() {
    sgChart.data.datasets.forEach((ds) => {
      const key = ds._rawKey;
      const raw = sgRaw[key];
      if (!raw) return;
      ds.data = raw.map(
        (p) => ({ x: p.x, y: p.y }),
      );
      delete sgBaselines[key];
    });
  }

  function nearestY(arr, targetX) {
    let best = arr[0];
    let bestDist = Math.abs(best.x - targetX);
    for (let i = 1; i < arr.length; i++) {
      const d = Math.abs(arr[i].x - targetX);
      if (d < bestDist) {
        best = arr[i]; bestDist = d;
      }
    }
    return best.y;
  }

  function addPeakPoint(d) {
    if (sgFrozen) return;
    const chNum = d.channel || 1;
    const key = 'ch' + chNum;
    const t = d.timestamp_s || 0;
    const wl = d.wavelength_nm;
    if (wl == null) return;

    const pt = { x: t, y: wl };
    if (!sgRaw[key]) sgRaw[key] = [];
    sgRaw[key].push(pt);

    const dsIdx = chNum - 1;
    if (dsIdx < 0 || dsIdx >= NUM_CHANNELS) return;
    const ds = sgChart.data.datasets[dsIdx];
    if (!ds) return;

    if (sgAlignY) {
      const ref = sgBaselines[key] ?? 0;
      ds.data.push({ x: pt.x, y: pt.y - ref });
    } else {
      ds.data.push(pt);
    }
    sgDirty = true;
  }

  /* ---------- Spectrum (live sweep) ---------- */
  let spChart = null;
  let spFrozen = false;
  let spDirty = false;

  function initSpectrum() {
    const ctx = $('#spectrum-canvas')
      .getContext('2d');
    spChart = new Chart(ctx, {
      type: 'line',
      data: { datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            type: 'linear',
            title: {
              display: true,
              text: 'Wavelength (nm)',
              color: '#8b8fa3',
            },
            grid: { color: '#2a2d3a' },
            ticks: {
              color: '#8b8fa3',
              callback: (v) => v.toFixed(2),
            },
          },
          yDb: {
            position: 'left',
            title: {
              display: true, text: 'dB',
              color: '#1F77B4',
            },
            grid: { color: '#2a2d3a' },
            ticks: {
              color: '#8b8fa3',
              callback: (v) => v.toFixed(1),
            },
          },
        },
        plugins: {
          legend: { display: false },
          zoom: {
            pan: { enabled: true, mode: 'xy' },
            zoom: {
              wheel: { enabled: true },
              pinch: { enabled: true },
              mode: 'xy',
            },
          },
        },
      },
    });

    for (let i = 0; i < NUM_CHANNELS; i++) {
      const color = COLORS[i % COLORS.length];
      spChart.data.datasets.push({
        label: '' + (i + 1),
        data: [],
        borderColor: color,
        borderWidth: 1,
        pointRadius: 0,
        tension: 0,
        yAxisID: 'yDb',
        hidden: false,
        _rawKey: 'sp-ch' + (i + 1),
      });
    }
    bindSpControls();
  }

  function bindSpControls() {
    const elFreeze = $('#sp-freeze');
    const elReset = $('#sp-reset-zoom');

    elFreeze.onclick = () => {
      spFrozen = !spFrozen;
      elFreeze.classList.toggle('active', spFrozen);
      elFreeze.textContent = spFrozen
        ? 'Frozen' : 'Freeze';
    };

    elReset.onclick = () => spChart.resetZoom();
  }

  function updateSpectrum(d) {
    if (spFrozen || !spChart) return;
    const wls = d.wavelengths;
    const curves = d.curves;
    if (!wls || !curves) return;

    for (let i = 0; i < NUM_CHANNELS; i++) {
      const ch = i + 1;
      const vals = curves[ch];
      const ds = spChart.data.datasets[i];
      if (!ds) continue;
      if (!vals) {
        ds.data = [];
        continue;
      }
      ds.data = wls.map((w, j) => ({
        x: w,
        y: vals[j] != null ? vals[j] : NaN,
      }));
    }
    spDirty = true;
  }

  /* ---------- Chart init + flush ---------- */

  function initCharts() {
    if (window.chartjsPluginAnnotation) {
      Chart.register(window.chartjsPluginAnnotation);
    }
    const sgCanvas = $('#sensorgram-canvas');
    const spCanvas = $('#spectrum-canvas');
    sgCanvas.style.display = '';
    spCanvas.style.display = '';

    initSensorgram();
    initSpectrum();
    syncAllChannelVisibility();

    switchTab(activeTab);
    setInterval(flushCharts, 500);
  }

  function flushCharts() {
    if (sgDirty && sgChart && !sgFrozen) {
      sgChart.update('none');
      sgDirty = false;
    }
    if (spDirty && spChart && !spFrozen) {
      spChart.update('none');
      spDirty = false;
    }
  }

  /* ---- Button Handlers ---- */
  elBtnRun.onclick = async () => {
    const body = {
      recipe: elRecipe.value,
      chip_id: elChipId.value || 'ULTRA-TEST-001',
      note: elNote.value,
    };

    elBtnRun.disabled = true;
    elBtnRun.textContent = 'Starting\u2026';
    elBtnRun.classList.add('btn-starting');
    elLabel.textContent = 'Initialising hardware\u2026';

    try {
      const res = await fetch('/api/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Failed to start');
        elBtnRun.disabled = false;
        elBtnRun.textContent = 'Run';
        elBtnRun.classList.remove('btn-starting');
        elLabel.textContent = 'Idle';
      }
    } catch (e) {
      alert('Request failed: ' + e.message);
      elBtnRun.disabled = false;
      elBtnRun.textContent = 'Run';
      elBtnRun.classList.remove('btn-starting');
      elLabel.textContent = 'Idle';
    }
  };

  elBtnNewRun.onclick = () => {
    for (let i = 0; i < NUM_CHANNELS; i++) {
      const key = 'ch' + (i + 1);
      sgRaw[key] = [];
      delete sgBaselines[key];
      sgChart.data.datasets[i].data = [];
      spChart.data.datasets[i].data = [];
    }
    sgChart.update('none');
    spChart.update('none');

    completedSteps = 0;
    elPhase.textContent = '--';
    elLabel.textContent = 'Idle';
    elTip.textContent = 'Tip: none';
    elBar.style.width = '0%';
    elBar.textContent = '0 / 0';
    elElapsed.textContent = 'Elapsed: 0.0s';
    elGrid.innerHTML = '';
    elStepList.innerHTML = '';
    stepManifest = [];
    clearTimingMarkers();

    elBtnNewRun.style.display = 'none';
    elBtnRun.style.display = '';
    elBtnRun.disabled = false;
  };

  elBtnPause.onclick = () =>
    fetch('/api/pause', { method: 'POST' });
  elBtnResume.onclick = () =>
    fetch('/api/resume', { method: 'POST' });
  elBtnAbort.onclick = () =>
    fetch('/api/abort', { method: 'POST' });

  elBtnSmStart.onclick = () =>
    fetch('/api/state-machine/start', {
      method: 'POST',
    });
  elBtnSmStop.onclick = () =>
    fetch('/api/state-machine/stop', {
      method: 'POST',
    });

  /* ================================================
   * ENGINEERING TAB
   * ================================================ */

  const LOCATION_NAMES = [
    'Pipette Tip 1','Pipette Tip 2','Pipette Tip 3',
    'Pipette Tip 4','Pipette Tip 5','Pipette Tip 6',
    'Pipette Tip 7','Pipette Tip 8',
    'Pipette Port 1','Pipette Port 2','Pipette Port 3',
    'Pipette Port 4','Pipette Port 5','Pipette Port 6',
    'Pipette Port 7','Pipette Port 8',
    'Locked Collar Height','Unlocked Collar Height',
    'Serum Port',
    'Well-L1','Well-L2',
    'Well-S1','Well-S2','Well-S3','Well-S4',
    'Well-S5','Well-S6','Well-S7','Well-S8',
    'Well-S9','Well-S10','Well-S11','Well-S12',
    'Well-M1','Well-M2','Well-M3','Well-M4',
    'Well-M5','Well-M6','Well-M7','Well-M8',
    'Well-M9','Well-M10','Well-M11','Well-M12',
    'Well-M13','Well-M14','Well-M15','Well-M16',
    'Well-M17','Well-M18',
    'Blister-B1','Blister-B2','Blister-B3',
    'Lid Notch-Closed Position',
    'Lid Notch-Open Position',
    'Tip Removal - Position 1',
    'Tip Removal - Position 2',
  ];

  let engPosTimer = null;

  function initEngineering() {
    initAppTabs();
    initEngTabs();
    populateLocationSelects();
    wireEngMotion();
    wireEngPump();
    wireEngCentrifuge();
    wireEngCartridge();
    wireEngLocations();
    wireEngLeds();
    wireEngEnvironment();
    wireEngDoor();
    wireEngConsole();
    wireSimpleCommandButtons();
  }

  /* -- App-level Run / Engineering tabs -- */
  function initAppTabs() {
    document.querySelectorAll('.app-tab').forEach(
      (btn) => {
        btn.addEventListener('click', () => {
          document.querySelectorAll('.app-tab')
            .forEach(
              (b) => b.classList.remove('active'),
            );
          btn.classList.add('active');
          const v = btn.dataset.view;
          const runV = $('#run-view');
          const engV = $('#eng-view');
          if (v === 'engineering') {
            runV.hidden = true;
            engV.hidden = false;
            startEngPolling();
          } else {
            runV.hidden = false;
            engV.hidden = true;
            stopEngPolling();
          }
        });
      },
    );
  }

  /* -- Engineering sub-tabs -- */
  function initEngTabs() {
    document.querySelectorAll('.eng-tab').forEach(
      (btn) => {
        btn.addEventListener('click', () => {
          document.querySelectorAll('.eng-tab')
            .forEach(
              (b) => b.classList.remove('active'),
            );
          btn.classList.add('active');
          const id = btn.dataset.eng;
          document.querySelectorAll('.eng-pane')
            .forEach((p) => {
              p.hidden = (
                p.id !== `eng-${id}`
              );
            });
        });
      },
    );
  }

  function populateLocationSelects() {
    const opts = LOCATION_NAMES.map(
      (n, i) => `<option value="${i}">${n}</option>`,
    ).join('');
    document.querySelectorAll('.eng-loc-sel')
      .forEach((sel) => { sel.innerHTML = opts; });
  }

  /* -- STM32 command helper -- */
  async function engCmd(
    cmd, params = {}, waitDone = true,
    timeout = 30,
  ) {
    try {
      const res = await fetch(
        '/api/stm32/command', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            cmd, params,
            wait_done: waitDone,
            timeout_s: timeout,
          }),
        },
      );
      const j = await res.json();
      if (!res.ok) {
        engLog(`ERR ${cmd}: ${j.detail || res.status}`);
        return null;
      }
      engLog(
        `${cmd}: ${JSON.stringify(j.response)}`,
      );
      return j.response;
    } catch (e) {
      engLog(`ERR ${cmd}: ${e}`);
      return null;
    }
  }

  function engLog(msg) {
    const el = $('#eng-con-log');
    if (!el) return;
    const ts = new Date().toLocaleTimeString();
    el.textContent += `[${ts}] ${msg}\n`;
    el.scrollTop = el.scrollHeight;
  }

  /* -- Position polling -- */
  function startEngPolling() {
    if (engPosTimer) return;
    pollEngPosition();
    engPosTimer = setInterval(
      pollEngPosition, 1000,
    );
  }
  function stopEngPolling() {
    if (engPosTimer) {
      clearInterval(engPosTimer);
      engPosTimer = null;
    }
  }
  async function pollEngPosition() {
    try {
      const r = await fetch('/api/stm32/status');
      if (!r.ok) return;
      const d = await r.json();
      const g = d.gantry || {};
      const setV = (id, v) => {
        const el = $(`#eng-pos-${id}`);
        if (el) {
          el.textContent = (
            v != null ? Number(v).toFixed(2) : '--'
          );
        }
      };
      setV('x', g.x_mm);
      setV('y', g.y_mm);
      setV('z', g.z_mm);
      const lift = d.lift || {};
      setV('lift', lift.position_mm);
    } catch (_) { /* ignore */ }
  }

  /* ---- MOTION wiring ---- */
  function wireEngMotion() {
    document.querySelectorAll('.eng-jog-btn')
      .forEach((btn) => {
        btn.addEventListener('click', () => {
          const axis = btn.dataset.axis;
          const dir = parseInt(btn.dataset.dir);
          const step = parseFloat(
            $('#eng-jog-step').value,
          );
          const speed = parseFloat(
            $('#eng-jog-speed').value,
          );
          const dist = step * dir;
          const um = Math.round(dist * 1000);
          const sp = Math.round(speed * 10);
          const p = {};
          p[`${axis}_um`] = um;
          p[`${axis}_speed_01mms`] = sp;
          p.relative = 1;
          engCmd('move_gantry', p);
        });
      });

    $('#eng-goto-btn').addEventListener(
      'click', () => {
        const x = parseFloat(
          $('#eng-goto-x').value,
        );
        const y = parseFloat(
          $('#eng-goto-y').value,
        );
        const z = parseFloat(
          $('#eng-goto-z').value,
        );
        engCmd('move_gantry', {
          x_um: Math.round(x * 1000),
          y_um: Math.round(y * 1000),
          z_um: Math.round(z * 1000),
        });
      },
    );

    $('#eng-estop').addEventListener(
      'click', () => engCmd('abort', {}, false, 5),
    );
  }

  /* ---- PUMP wiring ---- */
  function wireEngPump() {
    $('#eng-tip-pickup').onclick = () => {
      const id = parseInt($('#eng-tip-id').value);
      engCmd('move_to_location', {
        location_id: id, speed_01mms: 200,
      });
    };
    $('#eng-tip-return').onclick = () => {
      const id = parseInt($('#eng-tip-id').value);
      engCmd('move_to_location', {
        location_id: id, speed_01mms: 200,
      });
    };
    $('#eng-tip-swap').onclick = () => {
      const id = parseInt($('#eng-tip-id').value);
      engCmd('gantry_tip_swap', { tip_id: id });
    };
    $('#eng-pump-aspirate').onclick = () => {
      const vol = parseInt($('#eng-pump-vol').value);
      const spd = parseInt($('#eng-pump-speed').value);
      engCmd('pump_aspirate', {
        volume_ul: vol, speed: spd,
      });
    };
    $('#eng-pump-dispense').onclick = () => {
      const vol = parseInt($('#eng-pump-vol').value);
      const spd = parseInt($('#eng-pump-speed').value);
      engCmd('pump_dispense', {
        volume_ul: vol, speed: spd,
      });
    };
    $('#eng-pump-blowout').onclick = () => {
      engCmd('pump_blowout');
    };
    $('#eng-sa-go').onclick = () => {
      const loc = parseInt($('#eng-sa-loc').value);
      const vol = parseInt($('#eng-sa-vol').value);
      engCmd('smart_aspirate', {
        location_id: loc, volume_ul: vol,
      });
    };
    $('#eng-well-dispense').onclick = () => {
      const src = parseInt($('#eng-wc-src').value);
      const dst = parseInt($('#eng-wc-dst').value);
      const vol = parseInt($('#eng-wc-vol').value);
      const spd = parseInt($('#eng-wc-speed').value);
      engCmd('well_dispense', {
        src_location: src, dst_location: dst,
        volume_ul: vol, speed: spd,
      });
    };
    $('#eng-cart-dispense').onclick = () => {
      const src = parseInt($('#eng-wc-src').value);
      const dst = parseInt($('#eng-wc-dst').value);
      const vol = parseInt($('#eng-wc-vol').value);
      const spd = parseInt($('#eng-wc-speed').value);
      engCmd('cart_dispense', {
        src_location: src, dst_location: dst,
        volume_ul: vol, speed: spd,
      });
    };
    $('#eng-cart-bf').onclick = () => {
      const src = parseInt($('#eng-wc-src').value);
      const dst = parseInt($('#eng-wc-dst').value);
      const vol = parseInt($('#eng-wc-vol').value);
      const spd = parseInt($('#eng-wc-speed').value);
      const cyc = parseInt($('#eng-wc-cycles').value);
      engCmd('cart_dispense_bf', {
        src_location: src, dst_location: dst,
        volume_ul: vol, speed: spd, cycles: cyc,
      });
    };
    $('#eng-tip-mix').onclick = () => {
      const loc = parseInt($('#eng-wc-src').value);
      const vol = parseInt($('#eng-wc-vol').value);
      const spd = parseInt($('#eng-wc-speed').value);
      const cyc = parseInt($('#eng-wc-cycles').value);
      engCmd('tip_mix', {
        location_id: loc,
        volume_ul: vol, speed: spd, cycles: cyc,
      });
    };
  }

  /* ---- CENTRIFUGE wiring ---- */
  function wireEngCentrifuge() {
    $('#eng-cfuge-start').onclick = () => {
      const rpm = parseInt(
        $('#eng-cfuge-rpm').value,
      );
      const dur = parseInt(
        $('#eng-cfuge-dur').value,
      );
      engCmd('centrifuge_start', {
        rpm, duration_s: dur,
      });
    };
    $('#eng-cfuge-angle-go').onclick = () => {
      const ang = parseInt(
        $('#eng-cfuge-angle').value,
      );
      engCmd('centrifuge_move_angle', {
        angle_deg: ang,
      });
    };
    $('#eng-cfuge-refresh').onclick = async () => {
      const r = await engCmd(
        'centrifuge_status', {}, false, 3,
      );
      $('#eng-cfuge-status').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
  }

  /* ---- CARTRIDGE wiring ---- */
  function wireEngCartridge() {
    /* uses simple data-cmd buttons */
  }

  /* ---- LOCATIONS wiring ---- */
  function wireEngLocations() {
    $('#eng-loc-go').onclick = () => {
      const locId = parseInt(
        $('#eng-loc-select').value,
      );
      const speed = parseFloat(
        $('#eng-loc-speed').value,
      );
      const sp = Math.max(
        0, Math.round(speed * 10),
      );
      engCmd('move_to_location', {
        location_id: locId, speed_01mms: sp,
      });
    };
    $('#eng-ctr-go').onclick = () => {
      const x = parseFloat($('#eng-ctr-x').value);
      const y = parseFloat($('#eng-ctr-y').value);
      const z = parseFloat($('#eng-ctr-z').value);
      engCmd('set_loc_centre', {
        x_um: Math.round(x * 1000),
        y_um: Math.round(y * 1000),
        z_um: Math.round(z * 1000),
      });
    };
    $('#eng-off-go').onclick = () => {
      const x = parseFloat($('#eng-off-x').value);
      const y = parseFloat($('#eng-off-y').value);
      const z = parseFloat($('#eng-off-z').value);
      engCmd('set_loc_offset', {
        dx_um: Math.round(x * 1000),
        dy_um: Math.round(y * 1000),
        dz_um: Math.round(z * 1000),
      });
    };
  }

  /* ---- LEDs wiring ---- */
  function wireEngLeds() {
    ['r', 'g', 'b', 'w'].forEach((c) => {
      const sl = $(`#eng-led-${c}`);
      const sp = $(`#eng-led-${c}-val`);
      sl.oninput = () => { sp.textContent = sl.value; };
    });
    const ledVals = () => ({
      r: parseInt($('#eng-led-r').value),
      g: parseInt($('#eng-led-g').value),
      b: parseInt($('#eng-led-b').value),
      w: parseInt($('#eng-led-w').value),
    });
    $('#eng-led-set').onclick = () => {
      const idx = parseInt($('#eng-led-idx').value);
      const c = ledVals();
      engCmd('led_set_pixel', {
        index: idx, ...c,
      }, false);
    };
    $('#eng-led-off').onclick = () => {
      const idx = parseInt($('#eng-led-idx').value);
      engCmd('led_set_pixel_off', {
        index: idx,
      }, false);
    };
    $('#eng-led-all-same').onclick = () => {
      const c = ledVals();
      for (let i = 0; i < 5; i++) {
        engCmd('led_set_pixel', {
          index: i, ...c,
        }, false);
      }
    };
    $('#eng-led-pat-start').onclick = () => {
      const pat = parseInt(
        $('#eng-led-pattern').value,
      );
      const dur = parseInt($('#eng-led-dur').value);
      const stg = parseInt(
        $('#eng-led-stage').value,
      );
      engCmd('led_set_pattern', {
        pattern: pat,
        duration_s: dur,
        stage: stg,
      }, false);
    };
    $('#eng-led-pat-stop').onclick = () => {
      engCmd('led_set_pattern', {
        pattern: 0, duration_s: 0, stage: 0,
      }, false);
    };
  }

  /* ---- ENVIRONMENT wiring ---- */
  function wireEngEnvironment() {
    $('#eng-heat-duty-set').onclick = () => {
      const d = parseInt(
        $('#eng-heat-duty').value,
      );
      engCmd('air_heater_set_duty', {
        duty_pct: d,
      }, false);
    };
    $('#eng-heat-status').onclick = async () => {
      const r = await engCmd(
        'air_heater_get_status', {}, false, 3,
      );
      $('#eng-heat-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
    $('#eng-fan-duty-set').onclick = () => {
      const d = parseInt($('#eng-fan-duty').value);
      engCmd('fan_set_duty', {
        duty_pct: d,
      }, false);
    };
    $('#eng-fan-status').onclick = async () => {
      const r = await engCmd(
        'fan_get_status', {}, false, 3,
      );
      $('#eng-fan-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
    $('#eng-temp-status').onclick = async () => {
      const r = await engCmd(
        'temp_get_status', {}, false, 3,
      );
      $('#eng-temp-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
    $('#eng-accel-status').onclick = async () => {
      const r = await engCmd(
        'accel_get_status', {}, false, 3,
      );
      $('#eng-accel-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
  }

  /* ---- DOOR / LID wiring ---- */
  function wireEngDoor() {
    $('#eng-door-status').onclick = async () => {
      const r = await engCmd(
        'door_status', {}, false, 3,
      );
      $('#eng-door-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
    $('#eng-lid-open').onclick = () => {
      const z = parseFloat($('#eng-lid-z').value);
      const extra = parseFloat(
        $('#eng-lid-extra').value,
      );
      engCmd('lid_move', {
        open: 1,
        z_engage_um: Math.round(z * 1000),
        extra_x_um: Math.round(extra * 1000),
      });
    };
    $('#eng-lid-close').onclick = () => {
      const z = parseFloat($('#eng-lid-z').value);
      const extra = parseFloat(
        $('#eng-lid-extra').value,
      );
      engCmd('lid_move', {
        open: 0,
        z_engage_um: Math.round(z * 1000),
        extra_x_um: Math.round(extra * 1000),
      });
    };
  }

  /* ---- CONSOLE wiring ---- */
  function wireEngConsole() {
    fetch('/api/stm32/commands').then(
      (r) => r.json(),
    ).then((cmds) => {
      const dl = $('#eng-cmd-list');
      cmds.forEach((c) => {
        const o = document.createElement('option');
        o.value = c;
        dl.appendChild(o);
      });
    }).catch(() => {});

    const send = () => {
      const cmd = $('#eng-con-cmd').value.trim();
      if (!cmd) return;
      let params = {};
      const pStr = $('#eng-con-params').value.trim();
      if (pStr) {
        try { params = JSON.parse(pStr); }
        catch (e) {
          engLog(`Invalid JSON: ${e}`);
          return;
        }
      }
      const wait = $('#eng-con-wait').checked;
      engCmd(cmd, params, wait);
    };
    $('#eng-con-send').onclick = send;
    $('#eng-con-cmd').addEventListener(
      'keydown', (e) => {
        if (e.key === 'Enter') send();
      },
    );
  }

  /* -- Wire all simple data-cmd buttons -- */
  function wireSimpleCommandButtons() {
    document.querySelectorAll(
      '.eng-cmd-btn[data-cmd]',
    ).forEach((btn) => {
      btn.addEventListener('click', () => {
        const cmd = btn.dataset.cmd;
        let params = {};
        if (btn.dataset.params) {
          try {
            params = JSON.parse(btn.dataset.params);
          } catch (_) { /* ignore */ }
        }
        engCmd(cmd, params);
      });
    });
  }

  /* ---- Boot ---- */
  init();
  initEngineering();
})();

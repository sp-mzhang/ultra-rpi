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
    'Pipette Port 1','Pipette Port 2',
    'Pipette Port 3','Pipette Port 4',
    'Pipette Port 5','Pipette Port 6',
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
  let engConnected = false;
  let engTempTimer = null;

  function setEngControls(en) {
    ['eng-pos-fieldset', 'eng-motor-fieldset',
     'eng-tabs-fieldset', 'eng-devcmd-fieldset',
     'eng-log-fieldset',
    ].forEach((id) => {
      const el = $(`#${id}`);
      if (el) el.disabled = !en;
    });
  }

  function initEngineering() {
    initAppTabs();
    initEngTabs();
    populateLocationSelects();
    wireEngConnect();
    wireEngMotion();
    wireEngPump();
    wireEngCentrifuge();
    wireEngCartridge();
    wireEngLocations();
    wireEngLeds();
    wireEngEnvironment();
    wireEngCamera();
    wireEngFans();
    wireEngAccel();
    wireEngTemp();
    wireEngDevCmd();
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
          } else {
            runV.hidden = false;
            engV.hidden = true;
            stopEngPolling();
            if (engConnected) doEngDisconnect();
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

  /* -- Connect / Disconnect -- */
  function wireEngConnect() {
    const btn = $('#eng-connect');
    btn.onclick = () => {
      if (engConnected) doEngDisconnect();
      else doEngConnect();
    };
  }

  async function doEngConnect() {
    const btn = $('#eng-connect');
    btn.disabled = true;
    btn.textContent = 'Connecting...';
    try {
      const r = await fetch(
        '/api/stm32/connect', { method: 'POST' },
      );
      const j = await r.json();
      if (!r.ok) {
        alert(j.detail || 'Connect failed');
        btn.disabled = false;
        btn.textContent = 'Connect';
        return;
      }
      engConnected = true;
      btn.textContent = 'Disconnect';
      btn.classList.add('connected');
      btn.disabled = false;
      $('#eng-conn-status').textContent = 'Connected';
      $('#eng-conn-status').className = 'eng-connected';
      const fw = j.firmware || '--';
      $('#eng-fw-version').textContent =
        `Firmware: ${fw}`;
      setEngControls(true);
      const autoCb = $('#eng-auto-update');
      if (autoCb && autoCb.checked) {
        startEngPolling();
      }
      engLog('Connected to STM32');
    } catch (e) {
      alert(`Connect error: ${e}`);
      btn.disabled = false;
      btn.textContent = 'Connect';
    }
  }

  async function doEngDisconnect() {
    stopEngPolling();
    try {
      await fetch(
        '/api/stm32/disconnect', { method: 'POST' },
      );
    } catch (_) { /* ignore */ }
    engConnected = false;
    const btn = $('#eng-connect');
    btn.textContent = 'Connect';
    btn.classList.remove('connected');
    btn.disabled = false;
    $('#eng-conn-status').textContent = 'Disconnected';
    $('#eng-conn-status').className =
      'eng-disconnected';
    $('#eng-fw-version').textContent = 'Firmware: --';
    setEngControls(false);
    engLog('Disconnected from STM32');
  }

  /* Commands that produce async DONE messages and
     therefore need send_command_wait_done on the backend.
     Everything else uses send_command (ACK only). */
  const WAIT_DONE_CMDS = new Set([
    'home_all', 'home_gantry',
    'home_x_axis', 'home_y_axis', 'home_z_axis',
    'move_gantry', 'move_z_axis', 'move_to_location',
    'move_to_well',
    'lift_home', 'lift_move', 'lift_move_top',
    'lift_stop',
    'pump_init', 'pump_aspirate', 'pump_dispense',
    'pump_prime', 'pump_blowout',
    'pump_piston_reset', 'pump_lld_start',
    'pump_move_absolute', 'pump_wait_idle',
    'smart_aspirate', 'well_dispense',
    'cart_dispense', 'cart_dispense_bf',
    'tip_mix', 'lld_perform',
    'gantry_tip_swap', 'lid_move',
    'centrifuge_start', 'centrifuge_move_angle',
    'centrifuge_home',
    'centrifuge_unlock', 'centrifuge_lock',
    'centrifuge_reverse',
    'centrifuge_goto_serum', 'centrifuge_goto_pipette',
    'centrifuge_goto_blister',
  ]);

  /* -- STM32 command helper -- */
  async function engCmd(
    cmd, params = {},
    waitDone = undefined,
    timeout = 30,
  ) {
    if (waitDone === undefined) {
      waitDone = WAIT_DONE_CMDS.has(cmd);
    }
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
        engLog(
          `ERR ${cmd}: ${j.detail || res.status}`,
        );
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
    const asc = $('#eng-con-autoscroll');
    if (!asc || asc.checked) {
      el.scrollTop = el.scrollHeight;
    }
  }

  /* -- Position polling -- */
  function startEngPolling() {
    if (engPosTimer) return;
    pollEngPosition();
    const ms = parseInt(
      $('#eng-update-ms').value,
    ) || 3000;
    engPosTimer = setInterval(pollEngPosition, ms);
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
            v != null
              ? Number(v).toFixed(2) : '--'
          );
        }
      };
      setV('x', g.x_mm);
      setV('y', g.y_mm);
      setV('z', g.z_mm);
      const lift = d.lift || {};
      setV('lift', lift.position_mm);

      const setLed = (id, homed) => {
        const el = $(`#eng-led-${id}`);
        if (!el) return;
        el.classList.toggle('green', !!homed);
        el.classList.toggle('red', !homed);
      };
      setLed('x', g.x_homed);
      setLed('y', g.y_homed);
      setLed('z', g.z_homed);
      setLed('lift', lift.homed);
    } catch (_) { /* ignore */ }
  }

  /* ---- MOTION wiring ---- */
  function wireEngMotion() {
    document.querySelectorAll('.eng-jog-btn')
      .forEach((btn) => {
        btn.addEventListener('click', () => {
          const axis = btn.dataset.axis;
          const dir = parseInt(btn.dataset.dir);
          const stepEl = $(`#eng-step-${axis}`);
          const velEl = $(`#eng-vel-${axis}`);
          const step = parseFloat(
            stepEl ? stepEl.value : 1,
          );
          const speed = parseFloat(
            velEl ? velEl.value : 20,
          );
          if (axis === 'lift') {
            engCmd('lift_move', {
              target_mm: step * dir,
              speed: speed,
              relative: true,
            });
          } else {
            const p = {};
            p[`${axis}_mm`] = step * dir;
            p.speed = speed;
            p.relative = true;
            engCmd('move_gantry', p);
          }
        });
      });

    $('#eng-x-end').onclick = () => {
      const spd = parseFloat($('#eng-vel-x').value);
      engCmd('move_gantry', {
        x_mm: 72.0, speed: spd,
      });
    };
    $('#eng-y-front').onclick = () => {
      const spd = parseFloat($('#eng-vel-y').value);
      engCmd('move_gantry', {
        y_mm: 9999, speed: spd,
      });
    };
    $('#eng-z-bottom').onclick = () => {
      const spd = parseFloat($('#eng-vel-z').value);
      engCmd('move_gantry', {
        z_mm: -23.81, speed: spd,
      });
    };

    $('#eng-goto-btn').onclick = () => {
      const x = parseFloat($('#eng-goto-x').value);
      const y = parseFloat($('#eng-goto-y').value);
      const z = parseFloat($('#eng-goto-z').value);
      const v = parseFloat($('#eng-goto-vel').value);
      engCmd('home_z_axis', {}, true, 30).then(
        () => engCmd('move_gantry', {
          x_mm: x, y_mm: y, z_mm: z, speed: v,
        }),
      );
    };

    $('#eng-lift-move-btn').onclick = () => {
      const mm = parseFloat(
        $('#eng-goto-lift').value,
      );
      const spd = parseFloat(
        $('#eng-vel-lift').value,
      );
      engCmd('lift_move', {
        target_mm: mm, speed: spd,
      });
    };

    $('#eng-estop').onclick = () =>
      engCmd('abort', {}, false, 5);

    $('#eng-update-all').onclick = () =>
      pollEngPosition();

    const autoCb = $('#eng-auto-update');
    if (autoCb) {
      autoCb.onchange = () => {
        if (autoCb.checked && engConnected) {
          startEngPolling();
        } else {
          stopEngPolling();
        }
      };
    }
  }

  /* ---- PUMP wiring ---- */
  function wireEngPump() {
    const pumpSpd = () =>
      parseFloat($('#eng-pump-speed').value) || 100;

    $('#eng-pump-init').onclick = async () => {
      const lbl = $('#eng-pump-status-lbl');
      lbl.textContent = 'Initializing...';
      lbl.className = '';
      await engCmd('pump_init', {}, true, 30);
      await engCmd('pump_piston_reset', {}, true, 10);
      lbl.textContent = 'Initialized';
      lbl.className = '';
      lbl.style.color = 'var(--green)';
    };

    $('#eng-tip-swap').onclick = () => {
      const from = parseInt(
        $('#eng-tip-return-id').value,
      );
      const to = parseInt(
        $('#eng-tip-pickup-id').value,
      );
      const p = {
        from_id: from, to_id: to,
        x_eject_um: parseInt(
          $('#eng-ts-x-ej').value,
        ),
        pick_depth_um: Math.round(
          parseFloat(
            $('#eng-ts-pick-z').value,
          ) * 1000,
        ),
        retract_um: parseInt(
          $('#eng-ts-retract').value,
        ),
        xy_speed_01mms: Math.round(
          parseFloat(
            $('#eng-ts-xy-spd').value,
          ) * 10,
        ),
        z_speed_01mms: Math.round(
          parseFloat(
            $('#eng-ts-z-spd').value,
          ) * 10,
        ),
      };
      engCmd('gantry_tip_swap', p);
    };

    $('#eng-lld-perform').onclick = () =>
      engCmd('lld_perform', {});

    $('#eng-aspirate').onclick = () => {
      engCmd('pump_aspirate', {
        volume_ul: parseFloat(
          $('#eng-asp-vol').value,
        ),
        speed_ul_s: pumpSpd(),
      });
    };
    $('#eng-llf-aspirate').onclick = () => {
      const wt = $('#eng-well-type').value;
      engCmd('smart_aspirate', {
        volume_ul: parseFloat(
          $('#eng-asp-vol').value,
        ),
        pump_speed_ul_s: pumpSpd(),
        well_id: wt === 'large' ? 1 : 0,
        air_slug_ul: parseFloat(
          $('#eng-air-slug').value,
        ),
      });
    };
    $('#eng-dispense').onclick = () => {
      engCmd('pump_dispense', {
        volume_ul: parseFloat(
          $('#eng-disp-vol').value,
        ),
        speed_ul_s: pumpSpd(),
      });
    };

    $('#eng-wd-go').onclick = () => {
      engCmd('well_dispense', {
        z_depth_mm: parseFloat(
          $('#eng-wd-depth').value,
        ),
        volume_ul: parseFloat(
          $('#eng-wd-vol').value,
        ),
        speed_ul_s: parseFloat(
          $('#eng-wd-speed').value,
        ),
        z_retract_mm: parseFloat(
          $('#eng-wd-retract').value,
        ),
        blowout: $('#eng-wd-blowout').checked
          ? 1 : 0,
      });
    };
    $('#eng-cd-go').onclick = () => {
      engCmd('cart_dispense', {
        volume_ul: parseFloat(
          $('#eng-cd-vol').value,
        ),
        vel_ul_s: parseFloat(
          $('#eng-cd-vel').value,
        ),
        reasp_ul: parseFloat(
          $('#eng-cd-reasp').value,
        ),
        sleep_s: parseFloat(
          $('#eng-cd-sleep').value,
        ),
        z_retract_mm: parseFloat(
          $('#eng-cd-retract').value,
        ),
      });
    };
    $('#eng-cb-go').onclick = () => {
      engCmd('cart_dispense_bf', {
        duration_s: parseFloat(
          $('#eng-cb-dur').value,
        ),
        vel_ul_s: parseFloat(
          $('#eng-cb-vel').value,
        ),
        for_vol_ul: parseFloat(
          $('#eng-cb-fwd').value,
        ),
        back_vol_ul: parseFloat(
          $('#eng-cb-bak').value,
        ),
        reasp_ul: parseFloat(
          $('#eng-cb-reasp').value,
        ),
        sleep_s: parseFloat(
          $('#eng-cb-sleep').value,
        ),
        z_retract_mm: parseFloat(
          $('#eng-cb-retract').value,
        ),
      });
    };
    $('#eng-tm-go').onclick = () => {
      engCmd('tip_mix', {
        mix_vol_ul: parseFloat(
          $('#eng-tm-vol').value,
        ),
        speed_ul_s: parseFloat(
          $('#eng-tm-speed').value,
        ),
        cycles: parseInt(
          $('#eng-tm-cycles').value,
        ),
        pull_vol_ul: parseFloat(
          $('#eng-tm-pull').value,
        ),
      });
    };
  }

  /* ---- CENTRIFUGE wiring ---- */
  function wireEngCentrifuge() {
    $('#eng-cfuge-start').onclick = () => {
      const dur = parseInt(
        $('#eng-cfuge-dur').value,
      );
      engCmd('centrifuge_start', {
        rpm: parseInt($('#eng-cfuge-rpm').value),
        duration: dur,
      }, true, dur + 30);
    };
    $('#eng-cfuge-angle-go').onclick = () => {
      engCmd('centrifuge_move_angle', {
        angle_001deg: Math.round(
          parseFloat(
            $('#eng-cfuge-angle').value,
          ) * 100,
        ),
        move_rpm: parseInt(
          $('#eng-cfuge-move-rpm').value,
        ),
      });
    };
    $('#eng-cfuge-refresh').onclick = async () => {
      const r = await engCmd(
        'centrifuge_status', {}, false, 3,
      );
      if (r) {
        $('#eng-cfuge-status').textContent =
          JSON.stringify(r, null, 2);
        if (r.angle_deg != null) {
          $('#eng-cfuge-actual').textContent =
            Number(r.angle_deg).toFixed(1);
        }
      }
    };
    $('#eng-cfuge-enc-align').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0013,
      }, false, 10);
    };
    $('#eng-cfuge-clear-err').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0006,
      }, false, 5);
    };

    $('#eng-trig-enable').onclick = () => {
      const pos = parseInt(
        $('#eng-trig-pos').value,
      );
      const width = parseInt(
        $('#eng-trig-width').value,
      );
      const pol = parseInt(
        $('#eng-trig-pol').value,
      );
      const posRaw = Math.round(
        pos * 16384 / 360,
      );
      [
        [0x0034, posRaw],
        [0x0033, width],
        [0x0032, pol],
        [0x0031, 1],
      ].forEach(([cmd, val]) => {
        engCmd('centrifuge_bldc_cmd', {
          bldc_cmd: cmd, data_u16: val,
        }, false, 5);
      });
    };
    $('#eng-trig-disable').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0031, data_u16: 0,
      }, false, 5);
    };
    $('#eng-trig-info').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x0030,
        }, false, 5,
      );
      engLog(`Trigger info: ${JSON.stringify(r)}`);
    };

    $('#eng-pid-get').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x0022,
        }, false, 5,
      );
      if (r) {
        engLog(`PID: ${JSON.stringify(r)}`);
      }
    };
    $('#eng-pid-set').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0023,
        p_gain: parseInt(
          $('#eng-pid-p-gain').value,
        ),
        p_shift: parseInt(
          $('#eng-pid-p-shift').value,
        ),
        i_gain: parseInt(
          $('#eng-pid-i-gain').value,
        ),
        i_shift: parseInt(
          $('#eng-pid-i-shift').value,
        ),
      }, false, 5);
    };

    const thGetCmds = [
      0x0016, 0x0018, 0x001C, 0x001E,
    ];
    const thSetCmds = [
      0x0017, 0x0019, 0x001D, 0x001F,
    ];
    const thFields = [
      '#eng-th-dev', '#eng-th-ctrl',
      '#eng-th-stop', '#eng-th-detect',
    ];
    $('#eng-th-get').onclick = async () => {
      for (let i = 0; i < thGetCmds.length; i++) {
        const r = await engCmd(
          'centrifuge_bldc_cmd', {
            bldc_cmd: thGetCmds[i],
          }, false, 5,
        );
        engLog(
          `Thresh[${i}]: ${JSON.stringify(r)}`,
        );
      }
    };
    $('#eng-th-set').onclick = () => {
      for (let i = 0; i < thSetCmds.length; i++) {
        engCmd('centrifuge_bldc_cmd', {
          bldc_cmd: thSetCmds[i],
          data_u32: parseInt(
            $(thFields[i]).value,
          ),
        }, false, 5);
      }
    };

    $('#eng-curr-soft-get').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x001A,
      }, false, 5);
    };
    $('#eng-curr-soft-set').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x001B,
        data_u16: parseInt(
          $('#eng-curr-soft').value,
        ),
      }, false, 5);
    };
    $('#eng-curr-max-get').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0050,
      }, false, 5);
    };
    $('#eng-curr-max-set').onclick = () => {
      engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0051,
        data_u16: parseInt(
          $('#eng-curr-max').value,
        ),
      }, false, 5);
    };
  }

  /* ---- CARTRIDGE wiring ---- */
  function wireEngCartridge() {
    const seqP = () => ({
      angle_open_initial_deg: parseInt(
        $('#eng-cart-open-init').value,
      ),
      angle_extra_deg: parseInt(
        $('#eng-cart-extra-deg').value,
      ),
      lift_high_01mm: parseInt(
        $('#eng-cart-lift-hi').value,
      ),
      lift_mid_01mm: parseInt(
        $('#eng-cart-lift-mid').value,
      ),
      move_rpm: parseInt(
        $('#eng-cart-rpm').value,
      ),
    });
    $('#eng-cart-unlock').onclick = () =>
      engCmd('centrifuge_unlock', seqP());
    $('#eng-cart-lock').onclick = () =>
      engCmd('centrifuge_lock', seqP());
    $('#eng-cart-reverse').onclick = () =>
      engCmd('centrifuge_reverse', seqP());
    $('#eng-cart-goto-serum').onclick = () =>
      engCmd('centrifuge_goto_serum', {
        angle_open_initial_deg: parseInt(
          $('#eng-cart-open-init').value,
        ),
        move_rpm: parseInt(
          $('#eng-cart-rpm').value,
        ),
      });
    $('#eng-cart-goto-pipette').onclick = () =>
      engCmd('centrifuge_goto_pipette', {
        angle_open_initial_deg: parseInt(
          $('#eng-cart-open-init').value,
        ),
        move_rpm: parseInt(
          $('#eng-cart-rpm').value,
        ),
      });
    $('#eng-cart-goto-blister').onclick = () =>
      engCmd('centrifuge_goto_blister', {
        angle_open_initial_deg: parseInt(
          $('#eng-cart-open-init').value,
        ),
        move_rpm: parseInt(
          $('#eng-cart-rpm').value,
        ),
      });
  }

  /* ---- CAMERA wiring ---- */
  function wireEngCamera() {
    const preview = $('#eng-cam-preview');
    $('#eng-cam-start').onclick = () => {
      preview.innerHTML =
        '<img src="/api/camera/stream" alt="Live">';
      $('#eng-cam-status-lbl').textContent =
        'Streaming';
    };
    $('#eng-cam-stop').onclick = () => {
      preview.innerHTML =
        '<span class="eng-dim">' +
        'Preview (Start stream to see live)' +
        '</span>';
      $('#eng-cam-status-lbl').textContent =
        'Stopped';
    };
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
      if (sl && sp) {
        sl.oninput = () => {
          sp.textContent = sl.value;
        };
      }
    });
    const ledVals = () => ({
      r: parseInt($('#eng-led-r').value),
      g: parseInt($('#eng-led-g').value),
      b: parseInt($('#eng-led-b').value),
      w: parseInt($('#eng-led-w').value),
    });
    $('#eng-led-set').onclick = () => {
      const idx = parseInt(
        $('#eng-led-idx').value,
      );
      engCmd('led_set_pixel', {
        idx, ...ledVals(),
      });
    };
    $('#eng-led-off').onclick = () => {
      const idx = parseInt(
        $('#eng-led-idx').value,
      );
      engCmd('led_set_pixel_off', { idx });
    };
    $('#eng-led-all-same').onclick = () => {
      const c = ledVals();
      for (let i = 0; i < 5; i++) {
        engCmd('led_set_pixel', {
          idx: i, ...c,
        });
      }
    };

    document.querySelectorAll('.eng-led-pat-btn')
      .forEach((btn) => {
        btn.onclick = () => {
          const pat = parseInt(btn.dataset.pat);
          const dur = parseInt(
            $('#eng-led-dur').value,
          );
          engCmd('led_set_pattern', {
            pattern: pat, duration_s: dur, stage: 0,
          });
        };
      });
    $('#eng-led-progress').onclick = () => {
      const dur = parseInt(
        $('#eng-led-dur').value,
      );
      const stg = parseInt(
        $('#eng-led-stage').value,
      );
      engCmd('led_set_pattern', {
        pattern: 4, duration_s: dur, stage: stg,
      });
    };
    $('#eng-led-pat-stop').onclick = () => {
      engCmd('led_set_pattern', {
        pattern: 0, duration_s: 0, stage: 0,
      });
    };

    const btnLed = $('#eng-btn-led');
    if (btnLed) {
      btnLed.onchange = () => {
        engCmd('led_set_button', {
          on: btnLed.checked ? 1 : 0,
        });
      };
    }
  }

  /* ---- ENVIRONMENT / AIR HEATER wiring ---- */
  function wireEngEnvironment() {
    const heatSl = $('#eng-ah-heat-duty');
    const heatLbl = $('#eng-ah-heat-duty-val');
    if (heatSl && heatLbl) {
      heatSl.oninput = () => {
        heatLbl.textContent = heatSl.value;
      };
    }
    const fanSl = $('#eng-ah-fan-duty');
    const fanLbl = $('#eng-ah-fan-duty-val');
    if (fanSl && fanLbl) {
      fanSl.oninput = () => {
        fanLbl.textContent = fanSl.value;
      };
    }

    $('#eng-ah-enable').onchange = () => {
      engCmd('air_heater_set_en', {
        enable: $('#eng-ah-enable').checked ? 1 : 0,
      });
    };
    $('#eng-ah-heat-set').onclick = () => {
      engCmd('air_heater_set_duty', {
        duty_pct: parseInt(heatSl.value),
      });
    };
    $('#eng-ah-fan-set').onclick = () => {
      engCmd('air_heater_set_fan', {
        duty_pct: parseInt(fanSl.value),
      });
    };
    $('#eng-ah-ctrl-start').onclick = () => {
      engCmd('air_heater_set_ctrl', {
        enable: true,
        setpoint_c: parseFloat(
          $('#eng-ah-setpoint').value,
        ),
        hysteresis_c: parseFloat(
          $('#eng-ah-hyst').value,
        ),
        heater_duty: parseInt(
          $('#eng-ah-ctrl-heat').value,
        ),
        fan_duty: parseInt(
          $('#eng-ah-ctrl-fan').value,
        ),
      });
    };
    $('#eng-ah-ctrl-stop').onclick = () => {
      engCmd('air_heater_set_ctrl', {
        enable: false,
        setpoint_c: 0, hysteresis_c: 0,
        heater_duty: 0, fan_duty: 0,
      });
    };
    $('#eng-ah-get-status').onclick = async () => {
      const r = await engCmd(
        'air_heater_get_status', {}, false, 3,
      );
      if (r) {
        const s = (k) => r[k] != null ? r[k] : '--';
        $('#eng-ah-ntc1').textContent = s('ntc1_c');
        $('#eng-ah-ntc2').textContent = s('ntc2_c');
        $('#eng-ah-st-heat').textContent =
          s('heater_duty');
        $('#eng-ah-st-fan').textContent =
          s('fan_duty');
        $('#eng-ah-st-en').textContent =
          s('heater_en');
        $('#eng-ah-st-ctrl').textContent =
          s('ctrl_state');
      }
    };
  }

  /* ---- FANS wiring ---- */
  function wireEngFans() {
    $('#eng-fan-duty-set').onclick = () => {
      engCmd('fan_set_duty', {
        pct: parseInt(
          $('#eng-fan-duty').value,
        ),
      });
    };
    $('#eng-fan-status').onclick = async () => {
      const r = await engCmd(
        'fan_get_status', {}, false, 3,
      );
      $('#eng-fan-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
  }

  /* ---- ACCELEROMETER wiring ---- */
  function wireEngAccel() {
    $('#eng-accel-status').onclick = async () => {
      const r = await engCmd(
        'accel_get_status', {}, false, 3,
      );
      $('#eng-accel-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
  }

  /* ---- TEMPERATURE wiring ---- */
  function wireEngTemp() {
    $('#eng-temp-get').onclick = async () => {
      const r = await engCmd(
        'temp_get_status', {}, false, 3,
      );
      $('#eng-temp-status-box').textContent = (
        r ? JSON.stringify(r, null, 2) : 'Error'
      );
    };
    const autoCb = $('#eng-temp-auto');
    if (autoCb) {
      autoCb.onchange = () => {
        if (engTempTimer) {
          clearInterval(engTempTimer);
          engTempTimer = null;
        }
        if (autoCb.checked && engConnected) {
          engTempTimer = setInterval(async () => {
            const r = await engCmd(
              'temp_get_status', {}, false, 3,
            );
            $('#eng-temp-status-box').textContent =
              r ? JSON.stringify(r, null, 2)
                : 'Error';
          }, 500);
        }
      };
    }
  }

  /* ---- DEVICE COMMANDS (right panel) ---- */
  function wireEngDevCmd() {
    const doorLed = $('#eng-door-led');
    if (doorLed) {
      doorLed.onchange = () => {
        engCmd('led_set_button', {
          on: doorLed.checked ? 1 : 0,
        });
      };
    }
    $('#eng-lid-open').onclick = () => {
      const z = parseFloat($('#eng-lid-z').value);
      const extra = parseFloat(
        $('#eng-lid-extra').value,
      );
      engCmd('lid_move', {
        open: 1,
        z_engage_um: Math.round(z * 1000),
        xy_speed_01mms: 250,
        z_speed_01mms: 60,
        x_open_extra_um: Math.round(extra * 1000),
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
        xy_speed_01mms: 250,
        z_speed_01mms: 60,
        x_open_extra_um: Math.round(extra * 1000),
      });
    };
    $('#eng-dev-status').onclick = async () => {
      const r = await engCmd(
        'get_status', {}, false, 3,
      );
      if (r) engLog(
        `Status: ${JSON.stringify(r)}`,
      );
    };
    $('#eng-con-clear').onclick = () => {
      const el = $('#eng-con-log');
      if (el) el.textContent = '';
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
      const pStr = (
        $('#eng-con-params').value.trim()
      );
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
            params = JSON.parse(
              btn.dataset.params,
            );
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

/* Ultra RPi -- WebSocket client + GUI logic */
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  /* ---- State ---- */
  let ws = null;
  let peakChart = null;
  let wellDefs = {};

  /* ---- Elements ---- */
  const elRecipe = $('#recipe-select');
  const elChipId = $('#chip-id');
  const elBtnRun = $('#btn-run');
  const elBtnPause = $('#btn-pause');
  const elBtnResume = $('#btn-resume');
  const elBtnAbort = $('#btn-abort');
  const elBtnSmStart = $('#btn-sm-start');
  const elBtnSmStop = $('#btn-sm-stop');
  const elPhase = $('#step-phase');
  const elLabel = $('#step-label');
  const elTip = $('#tip-badge');
  const elBar = $('#progress-bar');
  const elElapsed = $('#elapsed');
  const elMode = $('#mode-indicator');
  const elGrid = $('#wells-grid');

  /* ---- Init ---- */
  async function init() {
    await loadRecipes();
    await loadStatus();
    connectWS();
    initChart();
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

  async function loadStatus() {
    try {
      const res = await fetch('/api/status');
      const s = await res.json();
      updateProgress(s);
      if (s.wells && Object.keys(s.wells).length) {
        wellDefs = s.wells;
        renderWells(s.wells);
      }
      updateMode(s.sm_state || 'inactive');
      updateButtons(s.is_running, s.is_paused);
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
        break;
      case 'well_updated':
        updateWell(data);
        break;
      case 'tip_changed':
        updateTip(data);
        break;
      case 'peak_data':
        addPeakPoint(data);
        break;
      case 'pressure_update':
        addPressurePoints(data);
        break;
      case 'protocol_paused':
        updateButtons(true, true);
        elLabel.textContent = 'PAUSED: '
          + (data.step_label || '');
        break;
      case 'protocol_resumed':
        updateButtons(true, false);
        break;
      case 'protocol_started':
        updateButtons(true, false);
        break;
      case 'protocol_done':
      case 'protocol_error':
      case 'protocol_aborted':
        updateButtons(false, false);
        elLabel.textContent = type.replace('_', ' ');
        break;
      case 'status_changed':
        updateMode(data.state || 'inactive');
        break;
    }
  }

  /* ---- UI Updates ---- */
  function updateProgress(d) {
    const step = d.step || d.step_index || 0;
    const total = d.total || d.step_total || 0;
    const pct = total ? (step / total * 100) : 0;
    elPhase.textContent = d.phase || '--';
    elLabel.textContent = d.label
      || d.step_label || 'Idle';
    elBar.style.width = pct + '%';
    elBar.textContent = `${step} / ${total}`;
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
        `${d.current_volume_ul.toFixed(0)} µL`;
    }
    const init = wellDefs[d.name];
    if (init && fillEl) {
      const initVol = init.initial_volume_ul || 1;
      const pct = Math.max(
        0,
        Math.min(100, d.current_volume_ul / initVol * 100)
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
    elBtnRun.disabled = running;
    elBtnPause.disabled = !running || paused;
    elBtnResume.disabled = !running || !paused;
    elBtnAbort.disabled = !running;
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
        <div class="well-vol">${curVol.toFixed(0)} µL</div>
        <div class="fill-bar">
          <div class="fill-bar-inner"
               style="width:${pct}%"></div>
        </div>
      `;
      elGrid.appendChild(card);
    });
  }

  /* ---- Chart ---- */
  const COLORS = [
    '#4f7df9', '#2ea44f', '#d29922', '#da3633',
    '#8b5cf6', '#06b6d4', '#f97316', '#ec4899',
    '#14b8a6', '#eab308', '#6366f1', '#84cc16',
    '#f43f5e', '#22d3ee', '#a855f7',
  ];

  const rawData = {};
  let chartDirty = false;
  let frozen = false;
  let alignY = false;
  let startX = 0;
  const baselines = {};
  const elStartX = $('#start-x');
  const elBtnAlignY = $('#btn-align-y');
  const elBtnFreeze = $('#btn-freeze');
  const elBtnResetZoom = $('#btn-reset-zoom');
  const elBtnClear = $('#btn-clear');
  const elChToggles = $('#channel-toggles');

  function initChart() {
    const ctx = $('#peak-canvas').getContext('2d');
    peakChart = new Chart(ctx, {
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
              display: true, text: 'Shift (pm)',
              color: '#4f7df9',
            },
            grid: { color: '#2a2d3a' },
            ticks: { color: '#8b8fa3' },
          },
          yPressure: {
            position: 'right',
            title: {
              display: true, text: 'Pressure',
              color: '#2ea44f',
            },
            grid: { drawOnChartArea: false },
            ticks: { color: '#8b8fa3' },
          },
        },
        plugins: {
          legend: { display: false },
          zoom: {
            pan: {
              enabled: true,
              mode: 'xy',
            },
            zoom: {
              wheel: { enabled: true },
              pinch: { enabled: true },
              mode: 'xy',
            },
          },
        },
      },
    });
    setInterval(flushChart, 500);
    bindChartControls();
  }

  function bindChartControls() {
    elStartX.addEventListener('change', () => {
      startX = parseFloat(elStartX.value) || 0;
      applyStartX();
      if (alignY) recomputeAlign();
      chartDirty = true;
    });

    elBtnAlignY.onclick = () => {
      alignY = !alignY;
      elBtnAlignY.classList.toggle('active', alignY);
      if (alignY) {
        recomputeAlign();
      } else {
        restoreRawY();
      }
      chartDirty = true;
    };

    elBtnFreeze.onclick = () => {
      frozen = !frozen;
      elBtnFreeze.classList.toggle('active', frozen);
      elBtnFreeze.textContent = frozen
        ? 'Frozen' : 'Freeze';
    };

    elBtnResetZoom.onclick = () => {
      if (peakChart) peakChart.resetZoom();
    };

    elBtnClear.onclick = () => {
      Object.keys(rawData).forEach((k) => delete rawData[k]);
      Object.keys(baselines).forEach(
        (k) => delete baselines[k],
      );
      if (peakChart) {
        peakChart.data.datasets = [];
        peakChart.update('none');
      }
      elChToggles.innerHTML = '';
    };
  }

  function applyStartX() {
    if (!peakChart) return;
    peakChart.options.scales.x.min = startX || undefined;
    chartDirty = true;
  }

  function recomputeAlign() {
    if (!peakChart) return;
    peakChart.data.datasets.forEach((ds) => {
      const key = ds._rawKey;
      const raw = rawData[key];
      if (!raw || !raw.length) return;
      const ref = nearestY(raw, startX);
      baselines[key] = ref;
      ds.data = raw.map(
        (p) => ({ x: p.x, y: p.y - ref }),
      );
    });
  }

  function restoreRawY() {
    if (!peakChart) return;
    peakChart.data.datasets.forEach((ds) => {
      const key = ds._rawKey;
      const raw = rawData[key];
      if (!raw) return;
      ds.data = raw.map((p) => ({ x: p.x, y: p.y }));
      delete baselines[key];
    });
  }

  function nearestY(arr, targetX) {
    let best = arr[0];
    let bestDist = Math.abs(best.x - targetX);
    for (let i = 1; i < arr.length; i++) {
      const d = Math.abs(arr[i].x - targetX);
      if (d < bestDist) { best = arr[i]; bestDist = d; }
    }
    return best.y;
  }

  function flushChart() {
    if (chartDirty && peakChart && !frozen) {
      peakChart.update('none');
      chartDirty = false;
    }
  }

  function ensureDataset(key, label, axisID) {
    if (!rawData[key]) {
      rawData[key] = [];
      const idx = peakChart.data.datasets.length;
      const color = COLORS[idx % COLORS.length];
      peakChart.data.datasets.push({
        label: label,
        data: rawData[key],
        borderColor: color,
        borderWidth: axisID === 'yPeak' ? 1.5 : 1,
        pointRadius: 0,
        tension: axisID === 'yPeak' ? 0.2 : 0.1,
        yAxisID: axisID,
        _rawKey: key,
      });
      addChannelChip(key, label, color,
        peakChart.data.datasets.length - 1);
    }
  }

  function addChannelChip(key, label, color, dsIdx) {
    const chip = document.createElement('span');
    chip.className = 'ch-chip';
    chip.textContent = label;
    chip.style.background = color;
    chip.style.color = '#fff';
    chip.dataset.key = key;
    chip.dataset.idx = dsIdx;
    chip.onclick = () => {
      const meta = peakChart.getDatasetMeta(
        parseInt(chip.dataset.idx),
      );
      meta.hidden = !meta.hidden;
      chip.classList.toggle('hidden', meta.hidden);
      peakChart.update('none');
    };
    elChToggles.appendChild(chip);
  }

  function addPeakPoint(d) {
    if (frozen) return;
    const ch = 'Ch ' + (d.channel || 0);
    ensureDataset(ch, ch, 'yPeak');
    const pt = {
      x: d.timestamp_s || 0,
      y: d.shift_pm || 0,
    };
    rawData[ch].push(pt);
    const ds = peakChart.data.datasets.find(
      (s) => s._rawKey === ch,
    );
    if (ds) {
      if (alignY) {
        const ref = baselines[ch] ?? 0;
        ds.data.push({ x: pt.x, y: pt.y - ref });
      } else {
        ds.data.push({ x: pt.x, y: pt.y });
      }
    }
    chartDirty = true;
  }

  function addPressurePoints(d) {
    if (frozen) return;
    const label = d.label || 'Pressure';
    const key = 'P:' + label;
    ensureDataset(key, label, 'yPressure');
    const ts = d.timestamp_s || 0;
    const samples = d.samples || [];
    const count = samples.length;
    const ds = peakChart.data.datasets.find(
      (s) => s._rawKey === key,
    );
    samples.forEach((s, i) => {
      const pt = {
        x: ts + (count > 1 ? i / count : 0),
        y: s.pressure || 0,
      };
      rawData[key].push(pt);
      if (ds) {
        if (alignY) {
          const ref = baselines[key] ?? 0;
          ds.data.push({ x: pt.x, y: pt.y - ref });
        } else {
          ds.data.push({ x: pt.x, y: pt.y });
        }
      }
    });
    chartDirty = true;
  }

  /* ---- Button Handlers ---- */
  elBtnRun.onclick = async () => {
    const body = {
      recipe: elRecipe.value,
      chip_id: elChipId.value || 'ULTRA-TEST-001',
    };
    try {
      const res = await fetch('/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Failed to start');
      }
    } catch (e) {
      alert('Request failed: ' + e.message);
    }
  };

  elBtnPause.onclick = () =>
    fetch('/api/pause', { method: 'POST' });
  elBtnResume.onclick = () =>
    fetch('/api/resume', { method: 'POST' });
  elBtnAbort.onclick = () =>
    fetch('/api/abort', { method: 'POST' });

  elBtnSmStart.onclick = () =>
    fetch('/api/state-machine/start', { method: 'POST' });
  elBtnSmStop.onclick = () =>
    fetch('/api/state-machine/stop', { method: 'POST' });

  /* ---- Boot ---- */
  init();
})();

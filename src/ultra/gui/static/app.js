/* Ultra RPi -- WebSocket client + GUI logic */
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);

  /* ---- State ---- */
  let ws = null;
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
    await loadQuickRunDefaults();
    await loadStatus();
    connectWS();
    initCharts();
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
      case 'sweep_data':
        updateSpectrum(data);
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
        <div class="well-vol">
          ${curVol.toFixed(0)} µL
        </div>
        <div class="fill-bar">
          <div class="fill-bar-inner"
               style="width:${pct}%"></div>
        </div>
      `;
      elGrid.appendChild(card);
    });
  }

  /* ==========================================================
   * Charts
   * ==========================================================
   * Sensorgram: peak wavelength (nm) vs time (s)
   *   -- accumulates points over time
   * Spectrum: power (dB) vs wavelength (nm)
   *   -- replaces data each sweep (latest snapshot)
   * ======================================================== */

  const COLORS = [
    '#1F77B4', '#FF7F0E', '#2CA02C', '#D62728',
    '#9467BD', '#8C564B', '#E377C2', '#7F7F7F',
    '#BCBD22', '#17BECF', '#9EDAE5', '#FFBB78',
    '#98DF8A', '#FF9896', '#C5B0D5',
  ];

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
      Object.keys(sgRaw).forEach(
        (k) => delete sgRaw[k],
      );
      Object.keys(sgBaselines).forEach(
        (k) => delete sgBaselines[k],
      );
      sgChart.data.datasets = [];
      sgChart.update('none');
      $('#sg-channel-toggles').innerHTML = '';
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

  function sgEnsureDataset(key, label, colorIdx) {
    if (sgRaw[key]) return;
    sgRaw[key] = [];
    const color = COLORS[colorIdx % COLORS.length];
    sgChart.data.datasets.push({
      label: label,
      data: sgRaw[key],
      borderColor: color,
      borderWidth: 1.5,
      pointRadius: 2,
      pointBackgroundColor: color,
      tension: 0,
      yAxisID: 'yPeak',
      hidden: false,
      _rawKey: key,
    });
    const dsIdx = sgChart.data.datasets.length - 1;
    addChip(
      '#sg-channel-toggles', sgChart,
      key, label, color, dsIdx,
    );
  }

  function addPeakPoint(d) {
    if (sgFrozen) return;
    const chNum = d.channel || 1;
    const key = 'ch' + chNum;
    const label = '' + chNum;
    const colorIdx = (chNum - 1) % COLORS.length;
    const t = d.timestamp_s || 0;
    const wl = d.wavelength_nm;
    if (wl == null) return;

    sgEnsureDataset(key, label, colorIdx);
    const pt = { x: t, y: wl };
    sgRaw[key].push(pt);
    const ds = sgChart.data.datasets.find(
      (s) => s._rawKey === key,
    );
    if (ds) {
      if (sgAlignY) {
        const ref = sgBaselines[key] ?? 0;
        ds.data.push({ x: pt.x, y: pt.y - ref });
      } else {
        ds.data.push(pt);
      }
    }
    sgDirty = true;
  }

  /* ---------- Spectrum (live sweep snapshot) ---------- */
  let spChart = null;
  let spFrozen = false;
  let spDirty = false;
  const spTogglesEl = () => $('#sp-channel-toggles');

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

  let spChipsBuilt = false;

  function updateSpectrum(d) {
    if (spFrozen || !spChart) return;
    const wls = d.wavelengths;
    const curves = d.curves;
    if (!wls || !curves) return;

    const chNums = Object.keys(curves)
      .map(Number).sort((a, b) => a - b);

    if (!spChipsBuilt && chNums.length) {
      spChipsBuilt = true;
      chNums.forEach((ch, idx) => {
        const color =
          COLORS[(ch - 1) % COLORS.length];
        addChip(
          '#sp-channel-toggles', spChart,
          'sp-ch' + ch, '' + ch, color, idx,
        );
      });
    }

    while (
      spChart.data.datasets.length < chNums.length
    ) {
      const idx = spChart.data.datasets.length;
      const ch = chNums[idx];
      const color =
        COLORS[(ch - 1) % COLORS.length];
      spChart.data.datasets.push({
        label: '' + ch,
        data: [],
        borderColor: color,
        borderWidth: 1,
        pointRadius: 0,
        tension: 0,
        yAxisID: 'yDb',
        hidden: false,
        _rawKey: 'sp-ch' + ch,
      });
    }

    chNums.forEach((ch, idx) => {
      const ds = spChart.data.datasets[idx];
      const vals = curves[ch];
      if (!vals) return;
      ds.data = wls.map((w, j) => ({
        x: w,
        y: vals[j] != null ? vals[j] : NaN,
      }));
    });
    spDirty = true;
  }

  /* ---------- Shared helpers ---------- */

  function addChip(
      containerSel, chart,
      key, label, color, dsIdx,
  ) {
    const container = $(containerSel);
    if (!container) return;
    const chip = document.createElement('span');
    chip.className = 'ch-chip';
    chip.textContent = label;
    chip.style.background = color;
    chip.style.color = '#fff';
    chip.dataset.key = key;
    chip.dataset.idx = dsIdx;
    chip.onclick = () => {
      const idx = parseInt(chip.dataset.idx);
      const meta = chart.getDatasetMeta(idx);
      const ds = chart.data.datasets[idx];
      const nowHidden = !meta.hidden;
      meta.hidden = nowHidden;
      ds.hidden = nowHidden;
      chip.classList.toggle('hidden', nowHidden);
      chart.update('none');
    };
    container.appendChild(chip);
  }

  /* ---------- Chart init + flush ---------- */

  function initCharts() {
    initSensorgram();
    initSpectrum();
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
    };
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
    fetch('/api/state-machine/start', {
      method: 'POST',
    });
  elBtnSmStop.onclick = () =>
    fetch('/api/state-machine/stop', {
      method: 'POST',
    });

  /* ---- Boot ---- */
  init();
})();

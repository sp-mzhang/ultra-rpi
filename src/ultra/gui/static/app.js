/* Ultra RPi -- WebSocket client + GUI logic */
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

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
      console.warn('Failed to load quick_run defaults', e);
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

  /* ============================================================
   * Charts -- Sensorgram (wavelength nm) + Peak Shift (pm)
   * ============================================================
   * Each chart is an independent ChartManager with its own
   * rawData store, baselines, freeze/align state, and controls.
   * ========================================================== */

  const COLORS = [
    '#1F77B4', '#FF7F0E', '#2CA02C', '#D62728',
    '#9467BD', '#8C564B', '#E377C2', '#7F7F7F',
    '#BCBD22', '#17BECF', '#9EDAE5', '#FFBB78',
    '#98DF8A', '#FF9896', '#C5B0D5',
  ];

  class ChartManager {
    constructor(canvasId, yLabel, yAxisId, opts) {
      this.rawData = {};
      this.baselines = {};
      this.frozen = false;
      this.alignY = false;
      this.startX = 0;
      this.dirty = false;
      this.yField = opts.yField;
      this.yDecimals = opts.yDecimals || 4;

      const ctx = $(canvasId).getContext('2d');
      this.chart = new Chart(ctx, {
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
            [yAxisId]: {
              position: 'left',
              title: {
                display: true, text: yLabel,
                color: '#1F77B4',
              },
              grid: { color: '#2a2d3a' },
              ticks: {
                color: '#8b8fa3',
                callback: (v) =>
                  v.toFixed(this.yDecimals),
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
      this._yAxisId = yAxisId;
    }

    bindControls(ids) {
      const elStartX = $(ids.startX);
      const elAlignY = ids.alignY ? $(ids.alignY) : null;
      const elFreeze = $(ids.freeze);
      const elReset = $(ids.resetZoom);
      const elClear = $(ids.clear);
      this._togglesEl = $(ids.toggles);

      if (elStartX) {
        elStartX.addEventListener('change', () => {
          this.startX =
            parseFloat(elStartX.value) || 0;
          this.applyStartX();
          if (this.alignY) this.recomputeAlign();
          this.dirty = true;
        });
      }

      if (elAlignY) {
        elAlignY.onclick = () => {
          this.alignY = !this.alignY;
          elAlignY.classList.toggle(
            'active', this.alignY,
          );
          if (this.alignY) {
            this.recomputeAlign();
          } else {
            this.restoreRawY();
          }
          this.dirty = true;
        };
      }

      if (elFreeze) {
        elFreeze.onclick = () => {
          this.frozen = !this.frozen;
          elFreeze.classList.toggle(
            'active', this.frozen,
          );
          elFreeze.textContent = this.frozen
            ? 'Frozen' : 'Freeze';
        };
      }

      if (elReset) {
        elReset.onclick = () => this.chart.resetZoom();
      }

      if (elClear) {
        elClear.onclick = () => this.clearAll();
      }
    }

    clearAll() {
      Object.keys(this.rawData).forEach(
        (k) => delete this.rawData[k],
      );
      Object.keys(this.baselines).forEach(
        (k) => delete this.baselines[k],
      );
      this.chart.data.datasets = [];
      this.chart.update('none');
      if (this._togglesEl) {
        this._togglesEl.innerHTML = '';
      }
    }

    applyStartX() {
      this.chart.options.scales.x.min =
        this.startX || undefined;
      this.dirty = true;
    }

    recomputeAlign() {
      this.chart.data.datasets.forEach((ds) => {
        const key = ds._rawKey;
        const raw = this.rawData[key];
        if (!raw || !raw.length) return;
        const ref = nearestY(raw, this.startX);
        this.baselines[key] = ref;
        ds.data = raw.map(
          (p) => ({ x: p.x, y: p.y - ref }),
        );
      });
    }

    restoreRawY() {
      this.chart.data.datasets.forEach((ds) => {
        const key = ds._rawKey;
        const raw = this.rawData[key];
        if (!raw) return;
        ds.data = raw.map(
          (p) => ({ x: p.x, y: p.y }),
        );
        delete this.baselines[key];
      });
    }

    flush() {
      if (this.dirty && !this.frozen) {
        this.chart.update('none');
        this.dirty = false;
      }
    }

    ensureDataset(key, label, colorIdx) {
      if (this.rawData[key]) return;
      this.rawData[key] = [];
      const color = COLORS[colorIdx % COLORS.length];
      this.chart.data.datasets.push({
        label: label,
        data: this.rawData[key],
        borderColor: color,
        borderWidth: 1.5,
        pointRadius: 2,
        pointBackgroundColor: color,
        tension: 0,
        yAxisID: this._yAxisId,
        hidden: false,
        _rawKey: key,
      });
      const dsIdx =
        this.chart.data.datasets.length - 1;
      this._addChip(key, label, color, dsIdx);
    }

    _addChip(key, label, color, dsIdx) {
      if (!this._togglesEl) return;
      const chip = document.createElement('span');
      chip.className = 'ch-chip';
      chip.textContent = label;
      chip.style.background = color;
      chip.style.color = '#fff';
      chip.dataset.key = key;
      chip.dataset.idx = dsIdx;
      const chart = this.chart;
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
      this._togglesEl.appendChild(chip);
    }

    addPoint(key, label, colorIdx, x, y) {
      if (this.frozen) return;
      this.ensureDataset(key, label, colorIdx);
      const pt = { x: x, y: y };
      this.rawData[key].push(pt);
      const ds = this.chart.data.datasets.find(
        (s) => s._rawKey === key,
      );
      if (ds) {
        if (this.alignY) {
          const ref = this.baselines[key] ?? 0;
          ds.data.push({ x: pt.x, y: pt.y - ref });
        } else {
          ds.data.push(pt);
        }
      }
      this.dirty = true;
    }
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

  /* ---- Chart instances ---- */
  let sgChart = null;
  let psChart = null;

  function initCharts() {
    sgChart = new ChartManager(
      '#sensorgram-canvas', 'Wavelength (nm)',
      'yPeak', { yField: 'wavelength_nm', yDecimals: 4 },
    );
    sgChart.bindControls({
      startX: '#sg-start-x',
      alignY: '#sg-align-y',
      freeze: '#sg-freeze',
      resetZoom: '#sg-reset-zoom',
      clear: '#sg-clear',
      toggles: '#sg-channel-toggles',
    });

    psChart = new ChartManager(
      '#shift-canvas', 'Shift (pm)',
      'yShift', { yField: 'shift_pm', yDecimals: 1 },
    );
    psChart.bindControls({
      startX: '#ps-start-x',
      alignY: null,
      freeze: '#ps-freeze',
      resetZoom: '#ps-reset-zoom',
      clear: '#ps-clear',
      toggles: '#ps-channel-toggles',
    });

    setInterval(() => {
      sgChart.flush();
      psChart.flush();
    }, 500);
  }

  function addPeakPoint(d) {
    const chNum = d.channel || 1;
    const key = 'ch' + chNum;
    const label = '' + chNum;
    const colorIdx = (chNum - 1) % COLORS.length;
    const t = d.timestamp_s || 0;

    if (sgChart && d.wavelength_nm != null) {
      sgChart.addPoint(
        key, label, colorIdx, t, d.wavelength_nm,
      );
    }
    if (psChart && d.shift_pm != null) {
      psChart.addPoint(
        key, label, colorIdx, t, d.shift_pm,
      );
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

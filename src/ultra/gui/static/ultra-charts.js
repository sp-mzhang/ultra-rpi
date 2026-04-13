/* Ultra RPi -- Sensorgram + spectrum charts */
(function () {
  'use strict';
  const { $ } = window.Ultra;

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

  Ultra.addTimingMarker = addTimingMarker;
  Ultra.clearTimingMarkers = clearTimingMarkers;
  Ultra.addPeakPoint = addPeakPoint;
  Ultra.updateSpectrum = updateSpectrum;
  Ultra.initCharts = initCharts;
  Ultra.initTabs = initTabs;
  Ultra.initSidebar = initSidebar;
  Ultra.resetCharts = function() {
    for (let i = 0; i < NUM_CHANNELS; i++) {
      const key = 'ch' + (i + 1);
      sgRaw[key] = [];
      delete sgBaselines[key];
      sgChart.data.datasets[i].data = [];
      spChart.data.datasets[i].data = [];
    }
    sgChart.update('none');
    spChart.update('none');
  };
})();

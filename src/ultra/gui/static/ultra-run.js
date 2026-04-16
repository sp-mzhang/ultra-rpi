/* Ultra RPi -- Protocol run control, step list, wells, buttons */
(function () {
  'use strict';
  const { $, state } = window.Ultra;

  /* ---- Elements ---- */
  const elRecipe = $('#recipe-select');
  const elCalib = $('#calib-version');
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

  async function fetchRecipeList() {
    const res = await fetch('/api/recipes');
    return res.json();
  }

  function _fillSelect(selectEl, list) {
    const prev = selectEl.value;
    selectEl.innerHTML = '';
    (list || []).forEach((r) => {
      const opt = document.createElement('option');
      opt.value = r.file;
      const src = r.source ? ` (${r.source})` : '';
      opt.textContent = r.name + src;
      selectEl.appendChild(opt);
    });
    if (prev && [...selectEl.options].some(
      (o) => o.value === prev,
    )) {
      selectEl.value = prev;
    }
  }

  async function loadRecipes() {
    try {
      const list = await fetchRecipeList();
      _fillSelect(elRecipe, list);
    } catch (e) {
      console.error('loadRecipes failed:', e);
    }
  }

  async function loadCalibVersions() {
    try {
      const res = await fetch('/api/calibration');
      const data = await res.json();
      const assays = data.assays || {};
      const defAssay = data.default_assay || '';
      const defVer = data.default_version || '';
      elCalib.innerHTML = '';
      for (const [assay, versions] of Object.entries(assays)) {
        for (const ver of versions) {
          const opt = document.createElement('option');
          opt.value = assay + '/' + ver;
          opt.textContent = assay + ' ' + ver;
          elCalib.appendChild(opt);
        }
      }
      if (defAssay && defVer) {
        elCalib.value = defAssay + '/' + defVer;
      }
    } catch (e) {
      console.error('loadCalibVersions failed:', e);
    }
  }

  async function loadQuickRunDefaults() {
    try {
      const res = await fetch('/api/quick_run');
      const qr = await res.json();
      if (!qr.enabled) return;
      if (qr.protocol) elRecipe.value = qr.protocol;
      if (qr.chip_id) elChipId.value = qr.chip_id;
    } catch (_) { /* ignore */ }
  }

  async function loadStatus() {
    try {
      const res = await fetch('/api/status');
      const s = await res.json();
      updateProgress(s);
      if (s.wells && Object.keys(s.wells).length) {
        renderWells(s.wells);
      }
      updateTip(s);
      updateMode(s.sm_state || 'inactive');
      updateButtons(s.is_running, s.is_paused);
      if (s.recipe) {
        elRecipe.value = s.recipe;
        if (s.is_running) {
          elLabel.textContent = s.recipe_display || s.recipe;
        }
      }
      if (s.calibration_version) {
        elCalib.value = s.calibration_version;
      }
      if (elMachine && s.machine_name) {
        elMachine.textContent = s.machine_name;
      }
    } catch (_) { /* ignore */ }
  }

  /* ---- WebSocket ---- */
  function connectWS() {
    const proto = location.protocol === 'https:'
      ? 'wss' : 'ws';
    state.ws = new WebSocket(
      `${proto}://${location.host}/ws`
    );
    state.ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        handleEvent(msg.type, msg.data);
      } catch (e) {
        console.warn('WS parse error', e);
      }
    };
    state.ws.onclose = () => {
      console.log('WS closed, reconnecting in 3s');
      setTimeout(connectWS, 3000);
    };
    state.ws.onerror = () => state.ws.close();
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
        if (Ultra.addPeakPoint) Ultra.addPeakPoint(data);
        break;
      case 'sweep_data':
        if (Ultra.updateSpectrum) Ultra.updateSpectrum(data);
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
      case 'pressure_update':
        if (Ultra.addPressureSamples) {
          Ultra.addPressureSamples(
            data.label, data.timestamp_s, data.samples,
          );
        }
        break;
      case 'accel_stream':
        if (Ultra.onAccelStream) Ultra.onAccelStream(data);
        break;
      case 'protocol_started':
        completedSteps = 0;
        updateButtons(true, false);
        if (Ultra.clearTimingMarkers) Ultra.clearTimingMarkers();
        if (Ultra.clearPressureChart) Ultra.clearPressureChart();
        if (data.steps) buildStepList(data.steps);
        if (data.recipe) {
          elRecipe.value = data.recipe;
          elLabel.textContent = data.recipe_display || data.recipe;
        }
        break;
      case 'timing_marker':
        if (Ultra.addTimingMarker) Ultra.addTimingMarker(data);
        break;
      case 'egress_started':
      case 'egress_done':
      case 'egress_error':
        if (Ultra.updateEgressButton) {
          Ultra.updateEgressButton(data, type);
        }
        break;
      case 'analysis_complete':
        renderAnalysisResults(data);
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
        if (Ultra.appendLogLine) {
          Ultra.appendLogLine(data.line || '');
        }
        break;
    }
  }

  /* ---- Step List ---- */
  function buildStepList(steps) {
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
    if (!state.isPaused) return;
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
    const init = state.wellDefs[d.name];
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

  function updateMode(modeState) {
    if (modeState === 'inactive') {
      elMode.textContent = 'Manual Mode';
      elMode.className = 'badge badge-blue';
      elBtnSmStart.disabled = false;
      elBtnSmStop.disabled = true;
    } else {
      elMode.textContent = 'SM: ' + modeState;
      elMode.className = 'badge badge-green';
      elBtnSmStart.disabled = true;
      elBtnSmStop.disabled = false;
    }
  }

  function updateButtons(running, paused) {
    state.isRunning = running;
    state.isPaused = paused;
    elBtnRun.disabled = running;
    elBtnPause.disabled = !running || paused;
    elBtnResume.disabled = !running || !paused;
    elBtnAbort.disabled = !running;
    elBtnAbort.textContent = 'Abort';
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
    state.wellDefs = wells;
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

  /* ---- Analysis Results Modal ---- */
  const elAnalysisModal = $('#analysis-modal');
  const elAnalysisBody = elAnalysisModal
    ? elAnalysisModal.querySelector('tbody') : null;
  const elModalClose = $('#analysis-modal-close');
  const elModalOk = $('#analysis-modal-ok');

  function closeAnalysisModal() {
    if (elAnalysisModal) elAnalysisModal.hidden = true;
  }
  if (elModalClose) elModalClose.onclick = closeAnalysisModal;
  if (elModalOk) elModalOk.onclick = closeAnalysisModal;
  if (elAnalysisModal) {
    elAnalysisModal.addEventListener('click', (e) => {
      if (e.target === elAnalysisModal) closeAnalysisModal();
    });
  }

  function renderAnalysisResults(data) {
    if (!elAnalysisModal || !elAnalysisBody) return;
    const analytes = data.analytes || [];
    if (data.error) {
      elAnalysisBody.innerHTML =
        '<tr><td colspan="6" style="color:#f87171">'
        + 'Analysis error: ' + escHtml(data.error) + '</td></tr>';
      elAnalysisModal.hidden = false;
      return;
    }
    if (!analytes.length) return;
    elAnalysisBody.innerHTML = '';
    analytes.forEach((a) => {
      const tr = document.createElement('tr');
      const excluded = a.excluded_by_validation;
      const concStr = excluded ? 'EXCLUDED'
        : (a.concentration_display || (a.concentration !== null ? String(a.concentration) : '--'));
      const sigStr = excluded ? '--'
        : (a.signal !== null ? String(a.signal) : '--');
      const rangeCls = excluded ? 'out-of-range'
        : (a.in_range ? 'in-range' : 'out-of-range');
      const rangeStr = excluded ? 'Excluded'
        : (a.in_range ? 'Yes' : 'No');
      if (excluded) tr.classList.add('validation-excluded');
      tr.innerHTML =
        '<td>' + escHtml(a.analyte) + '</td>'
        + '<td><strong>' + concStr + '</strong></td>'
        + '<td>' + escHtml(a.unit || '') + '</td>'
        + '<td>' + sigStr + '</td>'
        + '<td>' + escHtml(a.fit_type || '') + '</td>'
        + '<td class="' + rangeCls + '">' + rangeStr + '</td>';
      elAnalysisBody.appendChild(tr);
    });
    elAnalysisModal.hidden = false;
  }

  /* ---- Button Handlers ---- */
  elBtnRun.onclick = async () => {
    const body = {
      recipe: elRecipe.value,
      calibration_version: elCalib.value || '',
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
    if (Ultra.resetCharts) Ultra.resetCharts();

    completedSteps = 0;
    elPhase.textContent = '--';
    elLabel.textContent = 'Idle';
    elTip.textContent = 'Tip: none';
    elBar.style.width = '0%';
    elBar.textContent = '0 / 0';
    elElapsed.textContent = 'Elapsed: 0.0s';
    elGrid.innerHTML = '';
    elStepList.innerHTML = '';
    if (Ultra.clearTimingMarkers) Ultra.clearTimingMarkers();
    closeAnalysisModal();
    if (elAnalysisBody) elAnalysisBody.innerHTML = '';

    elBtnNewRun.style.display = 'none';
    elBtnRun.style.display = '';
    elBtnRun.disabled = false;
  };

  elBtnPause.onclick = () =>
    fetch('/api/pause', { method: 'POST' });
  elBtnResume.onclick = () =>
    fetch('/api/resume', { method: 'POST' });
  elBtnAbort.onclick = async () => {
    elBtnAbort.disabled = true;
    elBtnAbort.textContent = 'Aborting\u2026';
    elLabel.textContent = 'Aborting\u2026';
    try {
      await fetch('/api/abort', { method: 'POST' });
      updateButtons(false, false);
      elLabel.textContent = 'Aborted';
      if (elBar) elBar.style.width = '0%';
      showNewRun();
    } catch (e) {
      elBtnAbort.disabled = false;
      elBtnAbort.textContent = 'Abort';
    }
  };

  elBtnSmStart.onclick = () =>
    fetch('/api/state-machine/start', {
      method: 'POST',
    });
  elBtnSmStop.onclick = () =>
    fetch('/api/state-machine/stop', {
      method: 'POST',
    });

  /* ---- Expose for cross-file calls ---- */
  Ultra.fetchRecipeList = fetchRecipeList;
  Ultra._fillSelect = _fillSelect;
  Ultra.loadRecipes = loadRecipes;

  Ultra.loadCalibVersions = loadCalibVersions;

  Ultra.initRun = async function () {
    await Promise.all([loadRecipes(), loadCalibVersions()]);
    await loadQuickRunDefaults();
    await loadStatus();

    if (!state.isRunning && elRecipe.value === '') {
      const crpOpt = [...elRecipe.options].find(
        (o) => o.value.startsWith('crp'),
      );
      if (crpOpt) elRecipe.value = crpOpt.value;
    }
    connectWS();
  };
})();

/* Ultra RPi -- Config & recipes tab */
(function () {
  'use strict';
  const { $ } = window.Ultra;

  /* -- Config & recipes (S3 machine + global recipes) -- */
  function initConfigRecipes() {
    const sel = $('#cfg-recipe-select');
    const taM = $('#cfg-machine-yaml');
    const taR = $('#cfg-recipe-yaml');
    const msgM = $('#cfg-machine-msg');
    const msgR = $('#cfg-recipe-msg');
    const msgY = $('#cfg-yaml-msg');
    const snEl = $('#cfg-device-sn');
    if (!sel || !taM || !taR) return;

    /* --- helpers --- */
    function fmtApiErr(j, fb) {
      if (!j || j.detail === undefined) return fb;
      const d = j.detail;
      return typeof d === 'string' ? d : JSON.stringify(d);
    }
    async function parseJson(r) {
      try { return await r.json(); } catch (_) { return {}; }
    }
    function setMsg(pre, text, isErr, isInfo) {
      if (!pre) return;
      pre.textContent = text || '';
      pre.classList.toggle('cfg-msg-err', !!isErr);
      pre.classList.toggle('cfg-msg-info', !!isInfo && !isErr);
    }
    function btnLoad(btn, on) {
      if (!btn) return;
      btn.classList.toggle('btn-loading', on);
      btn.disabled = on;
      if (on) btn.dataset.origText = btn.textContent;
    }
    function btnDone(btn) {
      if (!btn) return;
      btn.classList.remove('btn-loading');
      btn.disabled = false;
    }

    /* --- sidebar nav --- */
    document.querySelectorAll('.cfg-nav-btn').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.cfg-nav-btn').forEach(
          (x) => x.classList.remove('cfg-nav-active'),
        );
        b.classList.add('cfg-nav-active');
        const p = b.dataset.panel;
        document.querySelectorAll('.cfg-main > .cfg-panel').forEach((s) => {
          s.classList.toggle(
            'cfg-panel-visible',
            s.id === `cfg-panel-${p}`,
          );
        });
      });
    });

    /* --- sub-tabs (visual / yaml) --- */
    document.querySelectorAll('.cfg-subtab').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.cfg-subtab').forEach(
          (x) => x.classList.remove('cfg-subtab-active'),
        );
        b.classList.add('cfg-subtab-active');
        const t = b.dataset.subtab;
        document.querySelectorAll('.cfg-subtab-content').forEach((c) => {
          c.classList.toggle(
            'cfg-subtab-visible',
            c.id === `cfg-subtab-${t}`,
          );
        });
        if (t === 'yaml') builderToYaml();
        if (t === 'visual') yamlToBuilder();
      });
    });

    /* === Machine Settings === */
    const btnMLoad = $('#cfg-machine-load');
    const btnMSave = $('#cfg-machine-save');

    async function loadMachineSettings(apply) {
      btnLoad(btnMLoad, true);
      setMsg(msgM, apply ? 'Reloading…' : '');
      const q = apply ? '?apply=1' : '';
      try {
        const res = await fetch(`/api/machine-settings${q}`);
        const j = await parseJson(res);
        if (!res.ok) {
          setMsg(msgM, fmtApiErr(j, 'Failed'), true);
          taM.value = '';
          return;
        }
        taM.value = j.yaml_text || '';
        if (snEl) snEl.textContent = j.device_sn || '';
        if (j.source === 'defaults') {
          setMsg(msgM, 'No S3 object yet — showing effective config.', false, true);
        } else if (j.applied) {
          setMsg(msgM, 'Reloaded from S3 and applied.', false, true);
        } else {
          setMsg(msgM, 'Loaded from S3.', false, true);
        }
      } catch (e) {
        setMsg(msgM, String(e), true);
      } finally {
        btnDone(btnMLoad);
      }
    }

    btnMLoad.addEventListener('click', () => loadMachineSettings(true));
    btnMSave.addEventListener('click', async () => {
      btnLoad(btnMSave, true);
      setMsg(msgM, 'Saving…');
      try {
        const res = await fetch('/api/machine-settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ yaml_text: taM.value }),
        });
        const j = await parseJson(res);
        if (!res.ok) {
          setMsg(msgM, fmtApiErr(j, 'Save failed'), true);
          return;
        }
        setMsg(msgM, j.message || 'Saved.', false, false);
      } catch (e) {
        setMsg(msgM, String(e), true);
      } finally {
        btnDone(btnMSave);
      }
    });

    /* === Recipe Editor State === */
    let stepSchemas = {};
    let stepDescriptions = {};

    const CARTRIDGE_PORTS = [
      { name: 'PP1', loc: 8 },  { name: 'PP2', loc: 9 },
      { name: 'PP3', loc: 10 }, { name: 'PP4', loc: 11 },
      { name: 'PP5', loc: 12 }, { name: 'PP6', loc: 13 },
      { name: 'PP7', loc: 14 }, { name: 'PP8', loc: 15 },
    ];
    const CARTRIDGE_PORT_NAMES = CARTRIDGE_PORTS.map((p) => p.name);

    const SLOT_MAP = [
      { loc: 18, name: 'SERUM' },
      { loc: 21, name: 'S1' },  { loc: 22, name: 'S2' },
      { loc: 23, name: 'S3' },  { loc: 24, name: 'S4' },
      { loc: 25, name: 'S5' },  { loc: 26, name: 'S6' },
      { loc: 27, name: 'S7' },  { loc: 28, name: 'S8' },
      { loc: 29, name: 'S9' },
      { loc: 33, name: 'M1' },  { loc: 34, name: 'M2' },
      { loc: 35, name: 'M3' },  { loc: 36, name: 'M4' },
      { loc: 37, name: 'M5' },  { loc: 38, name: 'M6' },
      { loc: 39, name: 'M7' },  { loc: 40, name: 'M8' },
      { loc: 41, name: 'M9' },  { loc: 42, name: 'M10' },
      { loc: 43, name: 'M11' }, { loc: 44, name: 'M12' },
      { loc: 45, name: 'M13' }, { loc: 46, name: 'M14' },
      { loc: 47, name: 'M15' },
    ];
    const LOC_TO_NAME = Object.fromEntries(SLOT_MAP.map((s) => [s.loc, s.name]));

    const DEFAULT_INCLUDE_START = { name: 'A', label: 'Centrifuge', include: '_common.yaml#centrifuge_phase' };
    const DEFAULT_INCLUDE_END   = { name: 'C', label: 'Lock', include: '_common.yaml#lock_phase' };

    let builderModel = {
      name: '', description: '',
      constants: {}, reader: {}, peak_detect: {},
      wells: {},
      includeStart: { ...DEFAULT_INCLUDE_START },
      includeEnd:   { ...DEFAULT_INCLUDE_END },
      phases: [{ name: 'B', label: 'Pipetting', steps: [] }],
    };

    /* --- fetch schemas once --- */
    async function loadSchemas() {
      if (Object.keys(stepSchemas).length) return;
      try {
        const res = await fetch('/api/protocol/step-schemas');
        const j = await res.json();
        stepSchemas = j.schemas || {};
        stepDescriptions = j.descriptions || {};
      } catch (_) {}
    }

    /* --- recipe list --- */
    async function loadRecipeListForCfg() {
      const list = await Ultra.fetchRecipeList();
      Ultra._fillSelect(sel, list);
    }

    /* --- load recipe YAML and parse into builder --- */
    async function loadRecipeYaml() {
      const slug = sel.value;
      if (!slug) return;
      setMsg(msgR, ''); setMsg(msgY, '');
      try {
        const res = await fetch(
          `/api/recipes/${encodeURIComponent(slug)}/yaml`,
        );
        const j = await parseJson(res);
        if (!res.ok) {
          setMsg(msgR, fmtApiErr(j, 'Failed'), true);
          return;
        }
        taR.value = j.yaml_text || '';
        yamlToBuilder();
        const hint = j.source === 's3' ? 'Loaded from S3.' : 'Packaged recipe.';
        setMsg(msgR, hint, false, true);
        setMsg(msgY, hint, false, true);
      } catch (e) {
        setMsg(msgR, String(e), true);
      }
    }

    /* === YAML <-> Builder model sync === */
    function parseYaml(text) {
      if (typeof jsyaml !== 'undefined') return jsyaml.load(text) || {};
      return {};
    }

    function dumpYaml(obj) {
      if (typeof jsyaml === 'undefined') return JSON.stringify(obj, null, 2);

      const KEY_ORDER = [
        'name', 'description', 'reader', 'peak_detect', 'constants', 'wells', 'phases',
      ];
      const ordered = {};
      for (const k of KEY_ORDER) {
        if (obj[k] !== undefined) ordered[k] = obj[k];
      }
      for (const k of Object.keys(obj)) {
        if (!(k in ordered)) ordered[k] = obj[k];
      }

      const wellsBlock = ordered.wells;
      delete ordered.wells;
      let yaml = jsyaml.dump(ordered, {
        lineWidth: 120,
        noRefs: true,
        sortKeys: false,
        quotingType: "'",
        forceQuotes: false,
      });

      if (wellsBlock && Object.keys(wellsBlock).length) {
        const maxNameLen = Math.max(...Object.keys(wellsBlock).map((n) => n.length));
        let wellsYaml = 'wells:\n';
        for (const [wn, wv] of Object.entries(wellsBlock)) {
          const pad = ' '.repeat(Math.max(0, maxNameLen - wn.length));
          const parts = [];
          if (wv.loc !== undefined) parts.push(`loc: ${wv.loc}`);
          if (wv.reagent !== undefined) parts.push(`reagent: ${wv.reagent}`);
          if (wv.volume_ul !== undefined) parts.push(`volume_ul: ${wv.volume_ul}`);
          wellsYaml += `  ${wn}:${pad} {${parts.join(', ')}}\n`;
        }

        const phasesIdx = yaml.indexOf('phases:');
        if (phasesIdx !== -1) {
          yaml = yaml.slice(0, phasesIdx) + wellsYaml + '\n' + yaml.slice(phasesIdx);
        } else {
          yaml += '\n' + wellsYaml;
        }
      }
      return yaml;
    }

    function yamlToBuilder() {
      try {
        const text = taR.value;
        if (!text.trim()) return;
        const raw = parseYaml(text);
        if (!raw || typeof raw !== 'object') return;
        builderModel.name = raw.name || '';
        builderModel.description = raw.description || '';
        builderModel.constants = raw.constants || {};
        builderModel.reader = raw.reader || {};
        builderModel.peak_detect = raw.peak_detect || {};

        builderModel.wells = {};
        if (raw.wells && typeof raw.wells === 'object') {
          for (const [k, v] of Object.entries(raw.wells)) {
            if (CARTRIDGE_PORT_NAMES.includes(k)) continue;
            builderModel.wells[k] = typeof v === 'object' ? { ...v } : {};
          }
        }

        builderModel.includeStart = { ...DEFAULT_INCLUDE_START };
        builderModel.includeEnd = { ...DEFAULT_INCLUDE_END };
        builderModel.phases = [];

        if (Array.isArray(raw.phases)) {
          for (const ph of raw.phases) {
            if (ph.include) {
              const ref = String(ph.include);
              if (ref.includes('centrifuge')) {
                builderModel.includeStart = { name: ph.name || 'A', label: ph.label || 'Centrifuge', include: ref };
              } else if (ref.includes('lock')) {
                builderModel.includeEnd = { name: ph.name || 'C', label: ph.label || 'Lock', include: ref };
              }
            } else if (Array.isArray(ph.steps)) {
              const phase = { name: ph.name || '', label: ph.label || '', steps: [] };
              for (const s of ph.steps) {
                const p = { ...s };
                delete p.type; delete p.label;
                phase.steps.push({
                  type: s.type || '',
                  label: s.label || '',
                  params: p,
                });
              }
              builderModel.phases.push(phase);
            }
          }
        }
        if (!builderModel.phases.length) {
          builderModel.phases = [{ name: 'B', label: 'Pipetting', steps: [] }];
        }
        renderBuilder();
      } catch (e) {
        console.warn('yamlToBuilder:', e);
      }
    }

    function builderToYaml() {
      const obj = {};
      if (builderModel.name) obj.name = builderModel.name;
      if (builderModel.description) obj.description = builderModel.description;
      if (Object.keys(builderModel.reader).length) obj.reader = builderModel.reader;
      if (Object.keys(builderModel.peak_detect).length) obj.peak_detect = builderModel.peak_detect;
      if (Object.keys(builderModel.constants).length) obj.constants = builderModel.constants;

      const allWells = { ...builderModel.wells };
      const usedPorts = new Set();
      for (const ph of builderModel.phases) {
        for (const s of ph.steps) {
          for (const v of Object.values(s.params)) {
            if (CARTRIDGE_PORT_NAMES.includes(String(v))) usedPorts.add(String(v));
          }
        }
      }
      for (const pp of CARTRIDGE_PORTS) {
        if (usedPorts.has(pp.name)) {
          allWells[pp.name] = { loc: pp.loc, reagent: 'Cartridge', volume_ul: 0 };
        }
      }
      if (Object.keys(allWells).length) obj.wells = allWells;

      const is = builderModel.includeStart;
      const ie = builderModel.includeEnd;
      obj.phases = [
        { name: is.name, label: is.label, include: is.include },
        ...builderModel.phases.map((ph) => ({
          name: ph.name,
          label: ph.label,
          steps: ph.steps.map((s) => {
            const step = { type: s.type };
            if (s.label) step.label = s.label;
            for (const [k, v] of Object.entries(s.params)) {
              if (v !== '' && v !== undefined) step[k] = v;
            }
            return step;
          }),
        })),
        { name: ie.name, label: ie.label, include: ie.include },
      ];
      taR.value = dumpYaml(obj);
    }

    /* === New Recipe === */
    function resetBuilderForNew() {
      builderModel = {
        name: '', description: '',
        constants: {}, reader: {}, peak_detect: {},
        wells: {},
        includeStart: { ...DEFAULT_INCLUDE_START },
        includeEnd:   { ...DEFAULT_INCLUDE_END },
        phases: [{ name: 'B', label: 'Pipetting', steps: [] }],
      };
      taR.value = '';
      sel.value = '';
      renderBuilder();
      setMsg(msgR, 'New recipe — add wells and steps, then Save as new.', false, true);
    }
    $('#cfg-new-recipe').addEventListener('click', resetBuilderForNew);

    /* === Visual Builder Rendering === */
    const paletteEl = $('#rb-step-palette');
    const protoListEl = $('#rb-protocol-list');
    const stepCountEl = $('#rb-step-count');
    const wellsTbody = $('#rb-wells-table tbody');

    function wellNames() {
      return [...Object.keys(builderModel.wells), ...CARTRIDGE_PORT_NAMES];
    }

    function renderBuilder() {
      $('#rb-name').value = builderModel.name || '';
      $('#rb-desc').value = builderModel.description || '';
      renderWellsTable();
      renderProtocolList();
      renderPalette();
    }

    /* --- wells table --- */
    function slotOptions(selectedLoc, excludeLocs) {
      return SLOT_MAP.map((s) => {
        const taken = excludeLocs && excludeLocs.has(s.loc) && s.loc !== selectedLoc;
        return `<option value="${s.loc}"${s.loc === selectedLoc ? ' selected' : ''}${taken ? ' disabled' : ''}>${s.name} (${s.loc})</option>`;
      }).join('');
    }

    function usedSlotLocs() {
      const locs = new Set();
      for (const w of Object.values(builderModel.wells)) {
        if (w.loc) locs.add(w.loc);
      }
      return locs;
    }

    function renderWellsTable() {
      wellsTbody.innerHTML = '';
      const used = usedSlotLocs();
      for (const [name, w] of Object.entries(builderModel.wells)) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><select data-field="loc">${slotOptions(w.loc || 0, used)}</select></td>
          <td><input value="${w.reagent || ''}" data-field="reagent"></td>
          <td><input type="number" value="${w.volume_ul || 0}" data-field="volume_ul"></td>
          <td><button class="rb-well-del" title="Remove">&times;</button></td>
        `;
        tr.querySelector('.rb-well-del').addEventListener('click', () => {
          delete builderModel.wells[name];
          renderWellsTable();
          refreshWellDropdowns();
        });
        const locSel = tr.querySelector('[data-field="loc"]');
        locSel.addEventListener('change', () => {
          const newLoc = Number(locSel.value);
          const newName = LOC_TO_NAME[newLoc] || name;
          const oldData = builderModel.wells[name];
          oldData.loc = newLoc;
          if (newName !== name) {
            delete builderModel.wells[name];
            builderModel.wells[newName] = oldData;
          }
          renderWellsTable();
          refreshWellDropdowns();
        });
        tr.querySelectorAll('input').forEach((inp) => {
          inp.addEventListener('change', () => {
            const field = inp.dataset.field;
            builderModel.wells[name][field] = field === 'reagent' ? inp.value : Number(inp.value);
          });
        });
        wellsTbody.appendChild(tr);
      }
    }
    $('#rb-well-add').addEventListener('click', () => {
      const used = usedSlotLocs();
      const avail = SLOT_MAP.find((s) => !used.has(s.loc));
      if (!avail) return;
      builderModel.wells[avail.name] = { loc: avail.loc, reagent: '', volume_ul: 0 };
      renderWellsTable();
      refreshWellDropdowns();
    });

    function refreshWellDropdowns() {
      const names = wellNames();
      protoListEl.querySelectorAll('select[data-wellref]').forEach((sel) => {
        const cur = sel.value;
        sel.innerHTML = '<option value="">--</option>' +
          names.map((n) => `<option${n === cur ? ' selected' : ''}>${n}</option>`).join('');
      });
    }

    /* --- palette --- */
    function renderPalette() {
      paletteEl.innerHTML = '';
      const types = Object.keys(stepSchemas).length
        ? Object.keys(stepSchemas)
        : [];
      for (const t of types.sort()) {
        const card = document.createElement('div');
        card.className = 'rb-step-card';
        const desc = stepDescriptions[t] || '';
        card.innerHTML = `<span class="rb-card-name">${t}</span>` +
          (desc ? `<span class="rb-card-desc">${desc}</span>` : '');
        if (desc) card.title = desc;
        card.draggable = true;
        card.addEventListener('dragstart', (e) => {
          e.dataTransfer.setData('text/plain', t);
          e.dataTransfer.effectAllowed = 'copy';
        });
        card.addEventListener('dblclick', () => {
          addStepToProtocol(t);
        });
        paletteEl.appendChild(card);
      }
    }

    /* --- protocol list --- */
    function allSteps() {
      const out = [];
      for (const ph of builderModel.phases) {
        for (const s of ph.steps) out.push(s);
      }
      return out;
    }

    function flatIdxToPhase(flatIdx) {
      let count = 0;
      for (let pi = 0; pi < builderModel.phases.length; pi++) {
        const ph = builderModel.phases[pi];
        for (let si = 0; si < ph.steps.length; si++) {
          if (count === flatIdx) return { pi, si };
          count++;
        }
      }
      return null;
    }

    function renderIncludeBanner(ref, label) {
      const div = document.createElement('div');
      div.className = 'rb-include-banner';
      div.innerHTML = `<span class="rb-inc-label">${label}</span>` +
        `<span class="rb-inc-ref">${ref}</span>`;
      if (ref.includes('centrifuge')) {
        const skipRow = document.createElement('div');
        skipRow.style.cssText = 'margin-top:4px;display:flex;gap:12px;';
        const skipSpin = builderModel.constants.skip_centrifuge_spin || false;
        const skipShake = builderModel.constants.skip_centrifuge_shake || false;
        skipRow.innerHTML =
          `<label style="font-size:.85rem;cursor:pointer">` +
          `<input type="checkbox" id="cfg-skip-spin" ${skipSpin ? 'checked' : ''}> Skip Spin</label>` +
          `<label style="font-size:.85rem;cursor:pointer">` +
          `<input type="checkbox" id="cfg-skip-shake" ${skipShake ? 'checked' : ''}> Skip Shake</label>`;
        skipRow.querySelector('#cfg-skip-spin').addEventListener('change', (e) => {
          if (e.target.checked) {
            builderModel.constants.skip_centrifuge_spin = true;
          } else {
            delete builderModel.constants.skip_centrifuge_spin;
          }
        });
        skipRow.querySelector('#cfg-skip-shake').addEventListener('change', (e) => {
          if (e.target.checked) {
            builderModel.constants.skip_centrifuge_shake = true;
          } else {
            delete builderModel.constants.skip_centrifuge_shake;
          }
        });
        div.appendChild(skipRow);
      }
      return div;
    }

    function renderPhaseHeader(phase, phaseIdx) {
      const div = document.createElement('div');
      div.className = 'rb-phase-header';
      div.innerHTML = `
        <input class="rb-input rb-phase-name" value="${phase.name}" placeholder="name"
          title="Phase name (e.g. B, C)" data-field="name">
        <input class="rb-input rb-phase-label" value="${phase.label}" placeholder="label"
          title="Phase label" data-field="label">
        <button class="rb-phase-del" title="Remove phase">&times;</button>
      `;
      div.querySelector('[data-field="name"]').addEventListener('change', (e) => {
        builderModel.phases[phaseIdx].name = e.target.value;
      });
      div.querySelector('[data-field="label"]').addEventListener('change', (e) => {
        builderModel.phases[phaseIdx].label = e.target.value;
      });
      div.querySelector('.rb-phase-del').addEventListener('click', () => {
        if (builderModel.phases.length <= 1) return;
        builderModel.phases.splice(phaseIdx, 1);
        renderProtocolList();
      });
      return div;
    }

    function renderProtocolList() {
      protoListEl.innerHTML = '';
      const steps = allSteps();
      stepCountEl.textContent = `(${steps.length} steps)`;

      protoListEl.appendChild(
        renderIncludeBanner(builderModel.includeStart.include, builderModel.includeStart.label)
      );

      let globalIdx = 0;
      builderModel.phases.forEach((phase, phaseIdx) => {
        protoListEl.appendChild(renderPhaseHeader(phase, phaseIdx));

        if (!phase.steps.length) {
          const hint = document.createElement('p');
          hint.className = 'rb-drop-hint';
          hint.dataset.phaseIdx = phaseIdx;
          hint.textContent = 'Drag steps here or double-click to add';
          protoListEl.appendChild(hint);
        }

        phase.steps.forEach((s, localIdx) => {
          const idx = globalIdx++;
          const div = document.createElement('div');
          div.className = 'rb-proto-step';
          div.draggable = true;
          div.dataset.idx = idx;
          const schema = stepSchemas[s.type] || [];

          let paramsHtml = '';
          for (const p of schema) {
            const val = s.params[p.name] !== undefined ? s.params[p.name] : (p.default !== undefined ? p.default : '');
            if (p.well_ref) {
              const opts = wellNames().map((n) =>
                `<option${n === String(val) ? ' selected' : ''}>${n}</option>`
              ).join('');
              paramsHtml += `<div class="rb-param"><label>${p.name}</label>
                <select data-pkey="${p.name}" data-wellref="1">
                  <option value="">--</option>${opts}
                </select></div>`;
            } else if (p.type === 'boolean') {
              const ck = val === true || val === 'true' ? ' checked' : '';
              paramsHtml += `<div class="rb-param"><label>${p.name}</label>
                <input type="checkbox" data-pkey="${p.name}"${ck}></div>`;
            } else {
              paramsHtml += `<div class="rb-param"><label>${p.name}</label>
                <input value="${val}" data-pkey="${p.name}"></div>`;
            }
          }

          div.innerHTML = `
            <div class="rb-step-header">
              <span class="rb-step-num">${idx + 1}</span>
              <span class="rb-step-type">${s.type}</span>
              <input class="rb-input rb-step-label-input" value="${s.label}" placeholder="label"
                data-field="label">
              <button class="rb-step-del" title="Remove">&times;</button>
            </div>
            <div class="rb-step-params">${paramsHtml}</div>
          `;

          div.querySelector('.rb-step-del').addEventListener('click', () => {
            removeStep(idx);
          });
          div.querySelector('[data-field="label"]').addEventListener('change', (e) => {
            s.label = e.target.value;
          });
          div.querySelectorAll('[data-pkey]').forEach((inp) => {
            inp.addEventListener('change', () => {
              const k = inp.dataset.pkey;
              if (inp.type === 'checkbox') {
                s.params[k] = inp.checked;
              } else {
                const v = inp.value;
                s.params[k] = /^-?\d+(\.\d+)?$/.test(v) ? Number(v) : v;
              }
            });
          });

          div.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('application/x-step-idx', String(idx));
            e.dataTransfer.effectAllowed = 'move';
          });
          div.addEventListener('dragover', (e) => {
            e.preventDefault();
            div.classList.add('rb-drag-over');
          });
          div.addEventListener('dragleave', () => {
            div.classList.remove('rb-drag-over');
          });
          div.addEventListener('drop', (e) => {
            e.preventDefault();
            div.classList.remove('rb-drag-over');
            const fromIdx = e.dataTransfer.getData('application/x-step-idx');
            if (fromIdx !== '') {
              reorderStep(parseInt(fromIdx, 10), idx);
            } else {
              const typ = e.dataTransfer.getData('text/plain');
              if (typ) insertStepAt(typ, idx);
            }
          });

          protoListEl.appendChild(div);
        });
      });

      const addPhaseBtn = document.createElement('button');
      addPhaseBtn.className = 'btn btn-sm btn-outline';
      addPhaseBtn.textContent = '+ Add Phase';
      addPhaseBtn.style.margin = '6px 0';
      addPhaseBtn.addEventListener('click', () => {
        const letters = builderModel.phases.map((p) => p.name);
        let next = 'B';
        for (let c = 66; c <= 90; c++) {
          if (!letters.includes(String.fromCharCode(c))) { next = String.fromCharCode(c); break; }
        }
        builderModel.phases.push({ name: next, label: '', steps: [] });
        renderProtocolList();
      });
      protoListEl.appendChild(addPhaseBtn);

      protoListEl.appendChild(
        renderIncludeBanner(builderModel.includeEnd.include, builderModel.includeEnd.label)
      );
    }

    function addStepToProtocol(type) {
      const schema = stepSchemas[type] || [];
      const params = {};
      for (const p of schema) {
        if (p.default !== undefined) params[p.name] = p.default;
      }
      const last = builderModel.phases[builderModel.phases.length - 1];
      last.steps.push({ type, label: '', params });
      renderProtocolList();
    }

    function insertStepAt(type, beforeIdx) {
      const schema = stepSchemas[type] || [];
      const params = {};
      for (const p of schema) {
        if (p.default !== undefined) params[p.name] = p.default;
      }
      const loc = flatIdxToPhase(beforeIdx);
      if (loc) {
        builderModel.phases[loc.pi].steps.splice(loc.si, 0, { type, label: '', params });
      } else {
        builderModel.phases[builderModel.phases.length - 1].steps.push({ type, label: '', params });
      }
      renderProtocolList();
    }

    function removeStep(flatIdx) {
      const loc = flatIdxToPhase(flatIdx);
      if (loc) {
        builderModel.phases[loc.pi].steps.splice(loc.si, 1);
        renderProtocolList();
      }
    }

    function reorderStep(fromIdx, toIdx) {
      if (fromIdx === toIdx) return;
      const fromLoc = flatIdxToPhase(fromIdx);
      if (!fromLoc) return;
      const [item] = builderModel.phases[fromLoc.pi].steps.splice(fromLoc.si, 1);
      const adjustedTo = fromIdx < toIdx ? toIdx - 1 : toIdx;
      const toLoc = flatIdxToPhase(adjustedTo);
      if (toLoc) {
        builderModel.phases[toLoc.pi].steps.splice(toLoc.si, 0, item);
      } else {
        builderModel.phases[builderModel.phases.length - 1].steps.push(item);
      }
      renderProtocolList();
    }

    /* drop zone on protocol list itself */
    protoListEl.addEventListener('dragover', (e) => {
      e.preventDefault();
      protoListEl.classList.add('rb-drop-over');
    });
    protoListEl.addEventListener('dragleave', (e) => {
      if (!protoListEl.contains(e.relatedTarget)) {
        protoListEl.classList.remove('rb-drop-over');
      }
    });
    protoListEl.addEventListener('drop', (e) => {
      e.preventDefault();
      protoListEl.classList.remove('rb-drop-over');
      if (e.target.closest('.rb-proto-step')) return;
      const typ = e.dataTransfer.getData('text/plain');
      if (typ) addStepToProtocol(typ);
    });

    /* metadata inputs */
    $('#rb-name').addEventListener('change', (e) => {
      builderModel.name = e.target.value;
    });
    $('#rb-desc').addEventListener('change', (e) => {
      builderModel.description = e.target.value;
    });

    /* === Liquid Simulation === */
    let simTimer = null;
    let simIdx = 0;
    const simWellsEl = $('#rb-sim-wells');
    const simLabel = $('#rb-sim-step-label');
    let simVolumes = {};

    function simReset() {
      simIdx = 0;
      simVolumes = {};
      for (const [name, w] of Object.entries(builderModel.wells)) {
        simVolumes[name] = w.volume_ul || 0;
      }
      renderSimWells();
      highlightStep(-1);
      simLabel.textContent = '';
      if (simTimer) { clearInterval(simTimer); simTimer = null; }
      $('#rb-sim-play').disabled = false;
      $('#rb-sim-pause').disabled = true;
    }

    function renderSimWells() {
      simWellsEl.innerHTML = '';
      const maxVol = Math.max(1, ...Object.values(simVolumes).map(Math.abs));
      for (const [name, vol] of Object.entries(simVolumes)) {
        const pct = Math.min(100, Math.max(0, (Math.abs(vol) / maxVol) * 100));
        const div = document.createElement('div');
        div.className = 'rb-sim-well';
        div.innerHTML = `
          <span class="rb-sim-well-name">${name}</span>
          <div class="rb-sim-bar-wrap">
            <div class="rb-sim-bar" style="height:${pct}%"></div>
          </div>
          <span class="rb-sim-vol">${Math.round(vol)}</span>
        `;
        simWellsEl.appendChild(div);
      }
    }

    function highlightStep(idx) {
      protoListEl.querySelectorAll('.rb-proto-step').forEach((el, i) => {
        el.classList.toggle('rb-sim-active', i === idx);
        if (i === idx) el.scrollIntoView({ block: 'nearest' });
      });
    }

    function simStep() {
      const steps = allSteps();
      if (simIdx >= steps.length) {
        clearInterval(simTimer); simTimer = null;
        $('#rb-sim-play').disabled = false;
        $('#rb-sim-pause').disabled = true;
        simLabel.textContent = 'Done';
        highlightStep(-1);
        return;
      }
      const s = steps[simIdx];
      highlightStep(simIdx);
      simLabel.textContent = `Step ${simIdx + 1}: ${s.type} ${s.label || ''}`;

      const schema = stepSchemas[s.type] || [];
      const srcKey = s.params.source || s.params.well || '';
      const dstKey = s.params.target || s.params.dest || '';

      for (const p of schema) {
        if (p.volume_out && srcKey && simVolumes[srcKey] !== undefined) {
          const vol = Number(s.params[p.name]) || 0;
          if (vol > 0) simVolumes[srcKey] -= vol;
        }
        if (p.volume_in && dstKey && simVolumes[dstKey] !== undefined) {
          const vol = Number(s.params[p.name]) || 0;
          if (vol > 0) simVolumes[dstKey] += vol;
        }
        if (p.volume_out && p.volume_in && srcKey && dstKey) {
          break;
        }
      }
      renderSimWells();
      simIdx++;
    }

    $('#rb-sim-play').addEventListener('click', () => {
      if (simIdx === 0) simReset();
      const speed = parseInt($('#rb-sim-speed').value, 10) || 5;
      const ms = Math.max(50, 1000 / speed);
      simTimer = setInterval(simStep, ms);
      $('#rb-sim-play').disabled = true;
      $('#rb-sim-pause').disabled = false;
    });
    $('#rb-sim-pause').addEventListener('click', () => {
      if (simTimer) { clearInterval(simTimer); simTimer = null; }
      $('#rb-sim-play').disabled = false;
      $('#rb-sim-pause').disabled = true;
    });
    $('#rb-sim-reset').addEventListener('click', simReset);

    /* === Save functions === */
    async function saveRecipeYaml(slug, yamlText, msgEl) {
      setMsg(msgEl, 'Saving…');
      try {
        const res = await fetch(
          `/api/recipes/${encodeURIComponent(slug)}/yaml`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ yaml_text: yamlText }),
          },
        );
        const j = await parseJson(res);
        if (!res.ok) {
          setMsg(msgEl, fmtApiErr(j, 'Save failed'), true);
          return false;
        }
        if (Ultra.loadRecipes) await Ultra.loadRecipes();
        await loadRecipeListForCfg();
        setMsg(msgEl, j.message || `Saved "${slug}".`, false, false);
        return true;
      } catch (e) {
        setMsg(msgEl, String(e), true);
        return false;
      }
    }

    /* Visual builder save */
    $('#cfg-recipe-save').addEventListener('click', async () => {
      const slug = sel.value;
      if (!slug) return;
      builderToYaml();
      await saveRecipeYaml(slug, taR.value, msgR);
    });
    /* Visual builder save-as */
    $('#cfg-save-as').addEventListener('click', async () => {
      const slug = $('#cfg-save-as-slug').value.trim();
      if (!slug) { setMsg(msgR, 'Enter a slug name.', true); return; }
      builderToYaml();
      if (await saveRecipeYaml(slug, taR.value, msgR)) {
        await loadRecipeListForCfg();
        sel.value = slug;
      }
    });
    /* Raw YAML save */
    $('#cfg-yaml-save').addEventListener('click', async () => {
      const slug = sel.value;
      if (!slug) return;
      await saveRecipeYaml(slug, taR.value, msgY);
    });
    /* Raw YAML save-as */
    $('#cfg-yaml-save-as').addEventListener('click', async () => {
      const slug = $('#cfg-yaml-save-as-slug').value.trim();
      if (!slug) { setMsg(msgY, 'Enter a slug name.', true); return; }
      if (await saveRecipeYaml(slug, taR.value, msgY)) {
        await loadRecipeListForCfg();
        sel.value = slug;
      }
    });

    /* Sync button */
    $('#cfg-sync').addEventListener('click', async () => {
      setMsg(msgR, 'Syncing…');
      try {
        const res = await fetch('/api/config/sync-recipes', { method: 'POST' });
        const j = await res.json();
        if (!res.ok) {
          setMsg(msgR, fmtApiErr(j, 'Sync failed'), true);
          return;
        }
        await loadRecipeListForCfg();
        if (Ultra.loadRecipes) await Ultra.loadRecipes();
        await loadRecipeYaml();
        setMsg(msgR, 'Synced from S3.');
      } catch (e) {
        setMsg(msgR, String(e), true);
      }
    });

    /* Delete recipe */
    $('#cfg-delete-recipe').addEventListener('click', async () => {
      const slug = sel.value;
      if (!slug) { setMsg(msgR, 'No recipe selected.', true); return; }
      if (!confirm(`Delete recipe "${slug}" from S3? This cannot be undone.`)) return;
      try {
        const res = await fetch(`/api/recipes/${encodeURIComponent(slug)}`, { method: 'DELETE' });
        const j = await parseJson(res);
        if (!res.ok) { setMsg(msgR, fmtApiErr(j, 'Delete failed'), true); return; }
        await loadRecipeListForCfg();
        if (Ultra.loadRecipes) await Ultra.loadRecipes();
        resetBuilderForNew();
        setMsg(msgR, j.message || `Deleted "${slug}".`);
      } catch (e) {
        setMsg(msgR, String(e), true);
      }
    });

    /* Reload recipe */
    $('#cfg-recipe-load').addEventListener('click', loadRecipeYaml);
    sel.addEventListener('change', loadRecipeYaml);

    /* === Common Protocol (_common.yaml) === */
    const taCommon = $('#cfg-common-yaml');
    const msgC = $('#cfg-common-msg');

    async function loadCommonProtocol() {
      const btn = $('#cfg-common-load');
      btnLoad(btn);
      setMsg(msgC, '');
      try {
        const res = await fetch('/api/common-protocol/yaml');
        const j = await parseJson(res);
        if (!res.ok) { setMsg(msgC, fmtApiErr(j, 'Failed'), true); return; }
        taCommon.value = j.yaml_text || '';
        const hint = j.source === 's3' ? 'Loaded from S3.' : 'Packaged file.';
        setMsg(msgC, hint, false, true);
      } catch (e) {
        setMsg(msgC, String(e), true);
      } finally {
        btnDone(btn);
      }
    }

    $('#cfg-common-load').addEventListener('click', loadCommonProtocol);
    $('#cfg-common-save').addEventListener('click', async () => {
      const btn = $('#cfg-common-save');
      btnLoad(btn);
      setMsg(msgC, 'Saving…');
      try {
        const res = await fetch('/api/common-protocol/yaml', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ yaml_text: taCommon.value }),
        });
        const j = await parseJson(res);
        if (!res.ok) { setMsg(msgC, fmtApiErr(j, 'Save failed'), true); return; }
        setMsg(msgC, j.message || 'Saved.', false, false);
      } catch (e) {
        setMsg(msgC, String(e), true);
      } finally {
        btnDone(btn);
      }
    });

    /* === Calibration panel === */
    const calibAssaySel = $('#cfg-calib-assay');
    const calibVerSel = $('#cfg-calib-version');
    const calibYaml = $('#cfg-calib-yaml');
    const calibMsg = $('#cfg-calib-msg');
    let calibTree = {};

    async function loadCalibTree() {
      try {
        const res = await fetch('/api/calibration');
        const data = await res.json();
        calibTree = data.assays || {};
        calibAssaySel.innerHTML = '';
        for (const assay of Object.keys(calibTree).sort()) {
          const opt = document.createElement('option');
          opt.value = assay;
          opt.textContent = assay;
          calibAssaySel.appendChild(opt);
        }
        fillCalibVersions();
      } catch (e) {
        setMsg(calibMsg, String(e), true);
      }
    }

    function fillCalibVersions() {
      const assay = calibAssaySel.value;
      calibVerSel.innerHTML = '';
      const versions = calibTree[assay] || [];
      for (const v of versions) {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        calibVerSel.appendChild(opt);
      }
    }

    if (calibAssaySel) {
      calibAssaySel.addEventListener('change', () => {
        fillCalibVersions();
        loadCalibConfig();
        updateCalibDownloadLinks();
      });
    }

    if (calibVerSel) {
      calibVerSel.addEventListener('change', () => {
        loadCalibConfig();
        updateCalibDownloadLinks();
      });
    }

    async function loadCalibConfig() {
      const assay = calibAssaySel.value;
      const ver = calibVerSel.value;
      if (!assay || !ver) return;
      try {
        const res = await fetch(
          `/api/calibration/${assay}/${ver}/config`,
        );
        if (!res.ok) {
          calibYaml.value = '';
          setMsg(calibMsg, 'Config not found', true);
          return;
        }
        const j = await res.json();
        calibYaml.value = j.yaml_text || '';
        setMsg(calibMsg, '');
      } catch (e) {
        setMsg(calibMsg, String(e), true);
      }
    }

    function updateCalibDownloadLinks() {
      const assay = calibAssaySel.value;
      const ver = calibVerSel.value;
      const dlFit = $('#cfg-calib-dl-fitting');
      const dlVal = $('#cfg-calib-dl-validation');
      if (dlFit) {
        dlFit.href = assay && ver
          ? `/api/calibration/${assay}/${ver}/file/fitting_protocol_sheet.xlsx`
          : '#';
      }
      if (dlVal) {
        dlVal.href = assay && ver
          ? `/api/calibration/${assay}/${ver}/file/validation_rules_sheet.xlsx`
          : '#';
      }
    }

    const btnCalibSave = $('#cfg-calib-yaml-save');
    if (btnCalibSave) {
      btnCalibSave.addEventListener('click', async () => {
        const assay = calibAssaySel.value;
        const ver = calibVerSel.value;
        if (!assay || !ver) return;
        btnLoad(btnCalibSave);
        try {
          const res = await fetch(
            `/api/calibration/${assay}/${ver}/config`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ yaml_text: calibYaml.value }),
            },
          );
          const j = await parseJson(res);
          if (!res.ok) {
            setMsg(calibMsg, fmtApiErr(j, 'Save failed'), true);
            return;
          }
          setMsg(calibMsg, j.message || 'Saved.', false, false);
        } catch (e) {
          setMsg(calibMsg, String(e), true);
        } finally {
          btnDone(btnCalibSave);
        }
      });
    }

    function setupCalibUpload(inputId, filename) {
      const input = $(inputId);
      if (!input) return;
      input.addEventListener('change', async () => {
        const file = input.files[0];
        if (!file) return;
        const assay = calibAssaySel.value;
        const ver = calibVerSel.value;
        if (!assay || !ver) return;
        const form = new FormData();
        form.append('file', file);
        try {
          const res = await fetch(
            `/api/calibration/${assay}/${ver}/file/${filename}`,
            { method: 'POST', body: form },
          );
          const j = await parseJson(res);
          if (!res.ok) {
            setMsg(calibMsg, fmtApiErr(j, 'Upload failed'), true);
            return;
          }
          setMsg(calibMsg, j.message || 'Uploaded.', false, false);
        } catch (e) {
          setMsg(calibMsg, String(e), true);
        }
        input.value = '';
      });
    }
    setupCalibUpload('#cfg-calib-ul-fitting', 'fitting_protocol_sheet.xlsx');
    setupCalibUpload('#cfg-calib-ul-validation', 'validation_rules_sheet.xlsx');

    const btnCalibNew = $('#cfg-calib-new');
    if (btnCalibNew) {
      btnCalibNew.addEventListener('click', async () => {
        const assay = calibAssaySel.value || prompt('Assay name:');
        const ver = prompt('New version (e.g. v1.1):');
        if (!assay || !ver) return;
        const yaml = calibYaml.value
          || '# Calibration config\nparameters:\n  version: "' + ver + '"\n';
        try {
          const res = await fetch(
            `/api/calibration/${assay}/${ver}/config`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ yaml_text: yaml }),
            },
          );
          const j = await parseJson(res);
          if (!res.ok) {
            setMsg(calibMsg, fmtApiErr(j, 'Create failed'), true);
            return;
          }
          setMsg(calibMsg, j.message || 'Created.', false, false);
          await loadCalibTree();
          calibAssaySel.value = assay;
          fillCalibVersions();
          calibVerSel.value = ver;
          await loadCalibConfig();
          updateCalibDownloadLinks();
          if (Ultra.loadCalibVersions) Ultra.loadCalibVersions();
        } catch (e) {
          setMsg(calibMsg, String(e), true);
        }
      });
    }

    const btnCalibDel = $('#cfg-calib-delete');
    if (btnCalibDel) {
      btnCalibDel.addEventListener('click', async () => {
        const assay = calibAssaySel.value;
        const ver = calibVerSel.value;
        if (!assay || !ver) return;
        if (!confirm('Delete calibration ' + assay + '/' + ver + '?')) return;
        try {
          const res = await fetch(
            `/api/calibration/${assay}/${ver}`,
            { method: 'DELETE' },
          );
          const j = await parseJson(res);
          if (!res.ok) {
            setMsg(calibMsg, fmtApiErr(j, 'Delete failed'), true);
            return;
          }
          setMsg(calibMsg, j.message || 'Deleted.', false, false);
          await loadCalibTree();
          if (Ultra.loadCalibVersions) Ultra.loadCalibVersions();
        } catch (e) {
          setMsg(calibMsg, String(e), true);
        }
      });
    }

    /* === Tab activation === */
    window.__cfgTabActivate = async function () {
      await Promise.all([
        loadRecipeListForCfg(),
        loadMachineSettings(false),
        loadSchemas(),
        loadCommonProtocol(),
        loadCalibTree(),
      ]);
      renderPalette();
      await loadRecipeYaml();
      await loadCalibConfig();
      updateCalibDownloadLinks();
    };
  }

  Ultra.initConfigRecipes = initConfigRecipes;
})();

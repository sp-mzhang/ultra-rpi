/* Ultra RPi -- Engineering tab */
(function () {
  'use strict';
  const { $, hexDV, engCmd, engLog } = window.Ultra;
  const Ultra = window.Ultra;

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
    wireEngFcHeater();
    wireEngCamera();
    wireEngFans();
    wireEngAccel();
    wireEngTemp();
    wireEngDevCmd();
    wireEngConsole();
    wireSimpleCommandButtons();
    wireEngMotorCurrent();
    restoreEngConnection();
  }

  async function restoreEngConnection() {
    try {
      const r = await fetch('/api/stm32/connected');
      if (!r.ok) return;
      const j = await r.json();
      if (!j.connected) return;
      engConnected = true;
      const btn = $('#eng-connect');
      if (btn) {
        btn.textContent = 'Disconnect';
        btn.classList.add('connected');
      }
      $('#eng-conn-status').textContent = 'Connected';
      $('#eng-conn-status').className =
        'eng-connected';
      setEngControls(true);
      engLog('Reconnected (session restored)');
    } catch (_) { /* ignore */ }
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
          const fwV = $('#fw-view');
          const cfgV = $('#cfg-view');
          runV.hidden = true;
          engV.hidden = true;
          if (fwV) fwV.hidden = true;
          if (cfgV) cfgV.hidden = true;
          if (v === 'engineering') {
            engV.hidden = false;
          } else if (v === 'firmware') {
            if (fwV) fwV.hidden = false;
          } else if (v === 'config') {
            if (cfgV) cfgV.hidden = false;
            if (typeof window.__cfgTabActivate === 'function') {
              window.__cfgTabActivate();
            }
          } else {
            runV.hidden = false;
          }
          if (v !== 'engineering') {
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
  function updatePositionDisplay(d) {
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
  }

  async function pollEngPosition() {
    try {
      const r = await fetch('/api/stm32/position');
      if (!r.ok) return;
      const d = await r.json();
      updatePositionDisplay(d);
    } catch (_) { /* ignore */ }
  }

  let jogBusy = false;

  function setJogButtonsEnabled(enabled) {
    document.querySelectorAll('.eng-jog-btn')
      .forEach((b) => { b.disabled = !enabled; });
  }

  async function fetchFreshPosition() {
    try {
      const r = await fetch('/api/stm32/position');
      if (!r.ok) return null;
      return r.json();
    } catch (_) { return null; }
  }

  /* ---- MOTION wiring ---- */
  function wireEngMotion() {
    document.querySelectorAll('.eng-jog-btn')
      .forEach((btn) => {
        btn.addEventListener('click', async () => {
          if (jogBusy) return;
          jogBusy = true;
          setJogButtonsEnabled(false);

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

          const status = await fetchFreshPosition();
          if (!status) {
            engLog('Jog: failed to read position');
            jogBusy = false;
            setJogButtonsEnabled(true);
            return;
          }

          const g = status.gantry || {};
          const curX = g.x_mm || 0;
          const curY = g.y_mm || 0;
          const curZ = g.z_mm || 0;
          const curLift = (
            (status.lift || {}).position_mm || 0
          );

          if (axis === 'x') {
            await engCmd('move_gantry', {
              x_mm: curX + step * dir,
              y_mm: curY,
              speed,
            }, true, 60);
          } else if (axis === 'y') {
            await engCmd('move_gantry', {
              x_mm: curX,
              y_mm: curY + step * dir,
              speed,
            }, true, 60);
          } else if (axis === 'z') {
            await engCmd('move_z_axis', {
              position_mm: curZ + step * dir,
              speed,
            }, true, 60);
          } else if (axis === 'lift') {
            await engCmd('lift_move', {
              target_mm: curLift + step * dir,
              speed,
            }, true, 60);
          }

          await pollEngPosition();
          jogBusy = false;
          setJogButtonsEnabled(true);
        });
      });

    $('#eng-x-end').onclick = () => {
      const spd = parseFloat($('#eng-vel-x').value);
      engCmd('move_gantry', {
        x_mm: 72.0, speed: spd,
      }, true, 60);
    };
    $('#eng-y-front').onclick = () => {
      const spd = parseFloat($('#eng-vel-y').value);
      engCmd('move_gantry', {
        y_mm: 9999, speed: spd,
      }, true, 60);
    };
    $('#eng-z-bottom').onclick = () => {
      const spd = parseFloat($('#eng-vel-z').value);
      engCmd('move_z_axis', {
        position_mm: -23.81, speed: spd,
      }, true, 60);
    };

    $('#eng-goto-btn').onclick = async () => {
      const x = parseFloat($('#eng-goto-x').value);
      const y = parseFloat($('#eng-goto-y').value);
      const z = parseFloat($('#eng-goto-z').value);
      const v = parseFloat($('#eng-goto-vel').value);
      await engCmd('home_z_axis', {}, true, 30);
      await engCmd('move_gantry', {
        x_mm: x, y_mm: y, speed: v,
      }, true, 60);
      await engCmd('move_z_axis', {
        position_mm: z, speed: v,
      }, true, 60);
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
      }, true, 60);
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
        volume: parseFloat(
          $('#eng-asp-vol').value,
        ),
        speed: pumpSpd(),
      });
    };
    $('#eng-llf-aspirate').onclick = () => {
      const wt = $('#eng-well-type').value;
      engCmd('smart_aspirate', {
        volume: parseFloat(
          $('#eng-asp-vol').value,
        ),
        speed: pumpSpd(),
        well_id: wt === 'large' ? 1 : 0,
        air_slug_ul: parseFloat(
          $('#eng-air-slug').value,
        ),
      });
    };
    $('#eng-dispense').onclick = () => {
      engCmd('pump_dispense', {
        volume: parseFloat(
          $('#eng-disp-vol').value,
        ),
        speed: pumpSpd(),
      });
    };

    $('#eng-wd-go').onclick = () => {
      engCmd('well_dispense', {
        z_depth_mm: parseFloat(
          $('#eng-wd-depth').value,
        ),
        volume: parseFloat(
          $('#eng-wd-vol').value,
        ),
        speed: parseFloat(
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
        volume: parseFloat(
          $('#eng-cd-vol').value,
        ),
        vel: parseFloat(
          $('#eng-cd-vel').value,
        ),
        reasp: parseFloat(
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
        vel: parseFloat(
          $('#eng-cb-vel').value,
        ),
        for_vol: parseFloat(
          $('#eng-cb-fwd').value,
        ),
        back_vol: parseFloat(
          $('#eng-cb-bak').value,
        ),
        reasp: parseFloat(
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
        mix_vol: parseFloat(
          $('#eng-tm-vol').value,
        ),
        speed: parseFloat(
          $('#eng-tm-speed').value,
        ),
        cycles: parseInt(
          $('#eng-tm-cycles').value,
        ),
        pull_vol: parseFloat(
          $('#eng-tm-pull').value,
        ),
      });
    };
  }

  /* ---- CENTRIFUGE wiring ---- */

  async function cfugeRefreshStatus() {
    const r = await engCmd(
      'centrifuge_status', {}, false, 3,
    );
    if (!r) return;
    $('#eng-cfuge-status').textContent =
      JSON.stringify(r, null, 2);
    const actualEl = $('#eng-cfuge-actual');
    if (r.angle_001deg != null && actualEl) {
      const deg = r.angle_001deg / 100.0;
      actualEl.textContent = deg.toFixed(1);
      const tgt = parseFloat(
        $('#eng-cfuge-angle').value,
      ) || 0;
      let err = Math.abs(deg - tgt);
      if (err > 180) err = 360 - err;
      if (err <= 3) {
        actualEl.style.color = 'var(--green)';
      } else if (err <= 5) {
        actualEl.style.color = '#f0ad4e';
      } else {
        actualEl.style.color = 'var(--red)';
      }
    }
    const led = $('#eng-cfuge-pwr-led');
    if (led) {
      const on = r.driver_online;
      led.classList.toggle('green', !!on);
      led.classList.toggle('red', !on);
    }
  }

  function wireEngCentrifuge() {
    $('#eng-cfuge-start').onclick = async () => {
      await engCmd('centrifuge_start', {
        rpm: parseInt($('#eng-cfuge-rpm').value),
        duration: parseInt(
          $('#eng-cfuge-dur').value,
        ),
      }, false);
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-angle-go').onclick = async () => {
      await engCmd('centrifuge_move_angle', {
        angle_001deg: Math.round(
          parseFloat(
            $('#eng-cfuge-angle').value,
          ) * 100,
        ),
        move_rpm: parseInt(
          $('#eng-cfuge-move-rpm').value,
        ),
      }, false);
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-refresh').onclick = () =>
      cfugeRefreshStatus();

    $('#eng-cfuge-pwr-on').onclick = async () => {
      await engCmd('centrifuge_power', {
        enable: true,
      });
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-pwr-off').onclick = async () => {
      await engCmd('centrifuge_power', {
        enable: false,
      });
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-home').onclick = async () => {
      await engCmd('centrifuge_home', {});
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-enc-align').onclick = async () => {
      await engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0013,
      }, false, 10);
      cfugeRefreshStatus();
    };
    $('#eng-cfuge-clear-err').onclick = async () => {
      await engCmd('centrifuge_bldc_cmd', {
        bldc_cmd: 0x0006,
      }, false, 5);
      cfugeRefreshStatus();
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
      if (r && r.data && r.data.length >= 16) {
        const dv = hexDV(r.data);
        $('#eng-pid-p-gain').value =
          dv.getInt16(0, true);
        $('#eng-pid-p-shift').value =
          dv.getUint16(2, true);
        $('#eng-pid-i-gain').value =
          dv.getInt16(4, true);
        $('#eng-pid-i-shift').value =
          dv.getUint16(6, true);
      }
      if (r) engLog(`PID: ${JSON.stringify(r)}`);
    };
    $('#eng-pid-set').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
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
        }, false, 5,
      );
      engLog(`Set PID: ${JSON.stringify(r)}`);
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
        if (r && r.data && r.data.length >= 8) {
          $(thFields[i]).value =
            hexDV(r.data).getUint32(0, true);
        }
        engLog(
          `Thresh[${i}]: ${JSON.stringify(r)}`,
        );
      }
    };
    $('#eng-th-set').onclick = async () => {
      for (let i = 0; i < thSetCmds.length; i++) {
        const r = await engCmd(
          'centrifuge_bldc_cmd', {
            bldc_cmd: thSetCmds[i],
            data_u32: parseInt(
              $(thFields[i]).value,
            ),
          }, false, 5,
        );
        engLog(
          `Set Thresh[${i}]: ${JSON.stringify(r)}`,
        );
      }
    };

    $('#eng-curr-soft-get').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x001A,
        }, false, 5,
      );
      if (r && r.data && r.data.length >= 4) {
        $('#eng-curr-soft').value =
          hexDV(r.data).getUint16(0, true);
      }
      engLog(`Soft OC: ${JSON.stringify(r)}`);
    };
    $('#eng-curr-soft-set').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x001B,
          data_u16: parseInt(
            $('#eng-curr-soft').value,
          ),
        }, false, 5,
      );
      engLog(`Set Soft OC: ${JSON.stringify(r)}`);
    };
    $('#eng-curr-max-get').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x0050,
        }, false, 5,
      );
      if (r && r.data && r.data.length >= 4) {
        $('#eng-curr-max').value =
          hexDV(r.data).getUint16(0, true);
      }
      engLog(`Max Out: ${JSON.stringify(r)}`);
    };
    $('#eng-curr-max-set').onclick = async () => {
      const r = await engCmd(
        'centrifuge_bldc_cmd', {
          bldc_cmd: 0x0051,
          data_u16: parseInt(
            $('#eng-curr-max').value,
          ),
        }, false, 5,
      );
      engLog(`Set Max Out: ${JSON.stringify(r)}`);
    };
  }

  /* ---- Motor Current Sensing ---- */
  function wireEngMotorCurrent() {
    const MAX_CS = 31;
    const CHART_HISTORY = 200;
    const AXIS_COLORS = {x: '#3b82f6', y: '#22c55e', z: '#f59e0b'};

    let evtSrc = null;
    let streaming = false;
    const history = {x: [], y: [], z: []};

    function updateAxis(ax, cs, pwm, faultsObj) {
      const pct = Math.round((cs / MAX_CS) * 100);
      const bar = $(`#mc-bar-${ax}`);
      const val = $(`#mc-val-${ax}`);
      if (bar) bar.style.height = pct + '%';
      if (val) val.textContent = `cs=${cs} pwm=${pwm}`;
      const fd = $(`#mc-faults-${ax}`);
      if (fd && faultsObj) {
        const active = Object.entries(faultsObj)
          .filter(([, v]) => v).map(([k]) => k);
        fd.textContent = active.length ? active.join(' ') : '';
      }
    }

    function pushHistory(sample) {
      for (const ax of ['x', 'y', 'z']) {
        history[ax].push(sample[ax]?.cs_actual ?? 0);
        if (history[ax].length > CHART_HISTORY)
          history[ax].shift();
      }
    }

    function drawChart() {
      const canvas = $('#mc-chart');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const W = canvas.width;
      const H = canvas.height;
      ctx.clearRect(0, 0, W, H);

      ctx.strokeStyle = 'rgba(128,128,128,0.15)';
      ctx.lineWidth = 1;
      for (let g = 0; g <= 4; g++) {
        const gy = H - (g / 4) * H;
        ctx.beginPath(); ctx.moveTo(0, gy);
        ctx.lineTo(W, gy); ctx.stroke();
      }

      const len = history.x.length;
      if (len < 2) return;

      for (const ax of ['x', 'y', 'z']) {
        ctx.strokeStyle = AXIS_COLORS[ax];
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        for (let i = 0; i < len; i++) {
          const px = (i / (CHART_HISTORY - 1)) * W;
          const py = H - (history[ax][i] / MAX_CS) * H;
          i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
        }
        ctx.stroke();
      }
    }

    $('#eng-motor-snap').onclick = async () => {
      try {
        const r = await fetch('/api/motor-status');
        if (!r.ok) { engLog('Motor status: ' + r.statusText); return; }
        const d = await r.json();
        for (const ax of ['x', 'y', 'z']) {
          const a = d[ax];
          if (!a) continue;
          updateAxis(ax, a.cs_actual, a.pwm_scale_sum, a.faults);
        }
        pushHistory(d);
        drawChart();
        engLog('Motor snapshot OK');
      } catch (e) {
        engLog('Motor snapshot error: ' + e);
      }
    };

    const toggleBtn = $('#eng-motor-stream-toggle');

    function stopStream() {
      if (evtSrc) { evtSrc.close(); evtSrc = null; }
      streaming = false;
      if (toggleBtn) toggleBtn.textContent = 'Start Stream';
    }
    function startStream() {
      history.x.length = 0;
      history.y.length = 0;
      history.z.length = 0;
      evtSrc = new EventSource('/api/motor-telemetry/stream');
      streaming = true;
      if (toggleBtn) toggleBtn.textContent = 'Stop Stream';

      evtSrc.onmessage = (ev) => {
        try {
          const d = JSON.parse(ev.data);
          for (const ax of ['x', 'y', 'z']) {
            const a = d[ax];
            if (a) updateAxis(ax, a.cs_actual, a.pwm_scale_sum, null);
          }
          pushHistory(d);
          drawChart();
        } catch (_) { /* skip bad frame */ }
      };
      evtSrc.onerror = () => {
        engLog('Motor telemetry stream disconnected');
        stopStream();
      };
    }

    if (toggleBtn) {
      toggleBtn.onclick = () => {
        streaming ? stopStream() : startStream();
      };
    }
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
    async function cartRefreshAngle(targetDeg) {
      const r = await engCmd(
        'centrifuge_status', {}, false, 3,
      );
      const actEl = $('#eng-cart-actual');
      const errEl = $('#eng-cart-error');
      if (!r || r.angle_001deg == null) return;
      const deg = r.angle_001deg / 100.0;
      actEl.textContent = deg.toFixed(1);
      if (targetDeg != null) {
        let err = Math.abs(deg - targetDeg);
        if (err > 180) err = 360 - err;
        errEl.textContent = err.toFixed(1);
        if (err <= 3) {
          errEl.style.color = 'var(--green)';
        } else if (err <= 5) {
          errEl.style.color = '#f0ad4e';
        } else {
          errEl.style.color = 'var(--red)';
        }
      }
    }

    const gotoParams = () => ({
      angle_open_initial_deg: parseInt(
        $('#eng-cart-open-init').value,
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
    $('#eng-cart-goto-serum').onclick = async () => {
      const init = parseInt(
        $('#eng-cart-open-init').value,
      );
      await engCmd(
        'centrifuge_goto_serum', gotoParams(),
      );
      cartRefreshAngle((init - 180 + 360) % 360);
    };
    $('#eng-cart-goto-pipette').onclick = async () => {
      const init = parseInt(
        $('#eng-cart-open-init').value,
      );
      await engCmd(
        'centrifuge_goto_pipette', gotoParams(),
      );
      cartRefreshAngle((init - 90 + 360) % 360);
    };
    $('#eng-cart-goto-blister').onclick = async () => {
      const init = parseInt(
        $('#eng-cart-open-init').value,
      );
      await engCmd(
        'centrifuge_goto_blister', gotoParams(),
      );
      cartRefreshAngle((init - 270 + 360) % 360);
    };
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
    let ahPollId = null;

    async function ahRefreshStatus() {
      const r = await engCmd(
        'air_heater_get_status', {}, false, 3,
      );
      if (!r) return;
      const s = (k) => r[k] != null ? r[k] : '--';
      $('#eng-ah-ntc1').textContent =
        s('prim_temp_c');
      $('#eng-ah-ntc2').textContent =
        s('sec_temp_c');
      $('#eng-ah-st-heat').textContent =
        s('heater_duty');
      $('#eng-ah-st-fan').textContent =
        s('fan_duty');
      $('#eng-ah-st-en').textContent =
        s('heater_en');
      const ctrlOn = r.ctrl_enabled;
      $('#eng-ah-st-ctrl').textContent =
        ctrlOn ? 'ON' : 'OFF';
      $('#eng-ah-ctrl-state').textContent =
        ctrlOn
          ? (r.ctrl_heating ? 'HEATING' : 'IDLE')
          : 'OFF';
    }

    $('#eng-ah-ctrl-start').onclick = async () => {
      await engCmd('air_heater_set_ctrl', {
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
      ahRefreshStatus();
      if (!ahPollId) {
        ahPollId = setInterval(ahRefreshStatus, 2000);
      }
    };
    $('#eng-ah-ctrl-stop').onclick = async () => {
      await engCmd('air_heater_set_ctrl', {
        enable: false,
        setpoint_c: 0, hysteresis_c: 0,
        heater_duty: 0, fan_duty: 0,
      });
      if (ahPollId) {
        clearInterval(ahPollId);
        ahPollId = null;
      }
      ahRefreshStatus();
    };
    $('#eng-ah-get-status').onclick = () =>
      ahRefreshStatus();
  }

  /* ---- FC HEATER wiring ---- */
  function wireEngFcHeater() {
    const dutySl = $('#eng-fc-duty');
    const dutyLbl = $('#eng-fc-duty-val');
    if (dutySl && dutyLbl) {
      dutySl.oninput = () => {
        dutyLbl.textContent = dutySl.value;
      };
    }

    $('#eng-fc-enable').onchange = () => {
      engCmd('fc_heater_set_en', {
        enable: $('#eng-fc-enable').checked,
      });
    };

    $('#eng-fc-duty-set').onclick = () => {
      engCmd('fc_heater_set_duty', {
        pct: parseInt(dutySl.value),
      });
    };

    /* -- Status -- */
    async function fcRefresh(nonBlocking) {
      const lt = nonBlocking ? 0.3 : undefined;
      const r = await engCmd(
        'fc_heater_get_status', {}, false, 3, lt,
      );
      if (!r) return;
      const s = (k) => r[k] != null ? r[k] : '--';
      $('#eng-fc-temp').textContent =
        typeof r.temp_c === 'number'
          ? r.temp_c.toFixed(2) + ' \u00b0C'
          : '--';
      $('#eng-fc-st-duty').textContent = s('heater_duty');
      $('#eng-fc-st-en').textContent =
        r.heater_en ? 'ON' : 'OFF';
      $('#eng-fc-st-otp').textContent =
        r.otp ? 'YES' : 'no';
      $('#eng-fc-st-ctrl').textContent =
        r.ctrl_enabled ? 'ON' : 'OFF';
      $('#eng-fc-st-heating').textContent =
        r.ctrl_heating ? 'YES' : 'no';
      return r;
    }
    $('#eng-fc-get-status').onclick = () => fcRefresh();

    /* -- Live temperature chart -- */
    const TEMP_HISTORY = 200;
    const tempHistory = [];
    let tempSetpoint = null;

    function drawTempChart() {
      const canvas = $('#fc-temp-chart');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const W = canvas.width;
      const H = canvas.height;
      ctx.clearRect(0, 0, W, H);

      if (tempHistory.length < 2) return;

      let tMin = Infinity, tMax = -Infinity;
      for (const v of tempHistory) {
        if (v < tMin) tMin = v;
        if (v > tMax) tMax = v;
      }
      if (tempSetpoint != null) {
        if (tempSetpoint < tMin) tMin = tempSetpoint;
        if (tempSetpoint > tMax) tMax = tempSetpoint;
      }
      const pad = 1.0;
      tMin -= pad; tMax += pad;
      if (tMax - tMin < 2) {
        tMin -= 1; tMax += 1;
      }

      ctx.strokeStyle = 'rgba(128,128,128,0.15)';
      ctx.lineWidth = 1;
      ctx.font = '10px monospace';
      ctx.fillStyle = '#888';
      for (let g = 0; g <= 4; g++) {
        const gy = H - (g / 4) * H;
        ctx.beginPath();
        ctx.moveTo(30, gy);
        ctx.lineTo(W, gy);
        ctx.stroke();
        const label = (tMin + (g / 4) * (tMax - tMin))
          .toFixed(1);
        ctx.fillText(label, 0, gy + 3);
      }

      if (tempSetpoint != null) {
        const spy = H
          - ((tempSetpoint - tMin) / (tMax - tMin)) * H;
        ctx.strokeStyle = '#ef4444';
        ctx.lineWidth = 1.5;
        ctx.setLineDash([6, 4]);
        ctx.beginPath();
        ctx.moveTo(30, spy);
        ctx.lineTo(W, spy);
        ctx.stroke();
        ctx.setLineDash([]);
      }

      const len = tempHistory.length;
      ctx.strokeStyle = '#3b82f6';
      ctx.lineWidth = 2;
      ctx.beginPath();
      for (let i = 0; i < len; i++) {
        const px = 30
          + (i / (TEMP_HISTORY - 1)) * (W - 30);
        const py = H
          - ((tempHistory[i] - tMin) / (tMax - tMin)) * H;
        i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
      }
      ctx.stroke();
    }

    /* -- PID start/stop + polling -- */
    let fcPollId = null;
    const pidState = $('#eng-fc-pid-state');

    async function fcPoll() {
      const r = await fcRefresh(true);
      if (!r) return;
      if (typeof r.temp_c === 'number') {
        tempHistory.push(r.temp_c);
        if (tempHistory.length > TEMP_HISTORY)
          tempHistory.shift();
      }
      if (r.ctrl_enabled) {
        tempSetpoint = r.ctrl_setpoint_c;
      }
      drawTempChart();
    }

    $('#eng-fc-pid-start').onclick = async () => {
      const sp = parseFloat(
        $('#eng-fc-pid-sp').value,
      );
      const kp = parseFloat(
        $('#eng-fc-pid-kp').value,
      );
      const ki = parseFloat(
        $('#eng-fc-pid-ki').value,
      );
      const kd = parseFloat(
        $('#eng-fc-pid-kd').value,
      );
      await engCmd('fc_heater_set_ctrl', {
        setpoint_x10: Math.round(sp * 10),
        kp_x1000: Math.round(kp * 1000),
        ki_x1000: Math.round(ki * 1000),
        kd_x1000: Math.round(kd * 1000),
        enable: true,
      });
      tempSetpoint = sp;
      if (pidState) pidState.textContent = 'RUNNING';
      if (!fcPollId) {
        fcPollId = setInterval(fcPoll, 500);
      }
    };

    $('#eng-fc-pid-stop').onclick = async () => {
      await engCmd('fc_heater_set_ctrl', {
        setpoint_x10: 0, kp_x1000: 0,
        ki_x1000: 0, kd_x1000: 0,
        enable: false,
      });
      if (fcPollId) {
        clearInterval(fcPollId);
        fcPollId = null;
      }
      if (pidState) pidState.textContent = 'IDLE';
      fcRefresh();
    };

    /* -- Liquid Test Sequence -- */
    const seqStatus = $('#eng-fc-seq-status');

    $('#eng-fc-seq-run').onclick = async () => {
      const body = {
        source_well: $('#eng-fc-seq-source').value || 'M1',
        aspirate_vol_ul: parseFloat(
          $('#eng-fc-seq-asp-vol').value,
        ),
        cart_vol_ul: parseFloat(
          $('#eng-fc-seq-cart-vol').value,
        ),
        aspirate_speed_ul_s: parseFloat(
          $('#eng-fc-seq-asp-speed').value,
        ),
        cart_vel_ul_s: parseFloat(
          $('#eng-fc-seq-cart-vel').value,
        ),
      };
      if (seqStatus) seqStatus.textContent = 'STARTING...';
      try {
        const r = await fetch(
          '/api/fc-liquid-sequence', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(body),
          },
        );
        if (!r.ok) {
          const e = await r.json().catch(() => ({}));
          const msg = typeof e.detail === 'string'
            ? e.detail
            : JSON.stringify(e.detail || e);
          if (seqStatus)
            seqStatus.textContent = 'ERR: ' + msg;
          return;
        }
        if (seqStatus)
          seqStatus.textContent = 'RUNNING';
        pollSequenceStatus();
      } catch (e) {
        if (seqStatus)
          seqStatus.textContent =
            'ERR: ' + (e.message || e);
      }
    };

    let seqPollId = null;
    function pollSequenceStatus() {
      if (seqPollId) clearInterval(seqPollId);
      seqPollId = setInterval(async () => {
        try {
          const r = await fetch(
            '/api/fc-liquid-sequence/status',
          );
          if (!r.ok) return;
          const j = await r.json();
          if (seqStatus)
            seqStatus.textContent =
              j.step || j.state || '--';
          if (j.state === 'idle'
            || j.state === 'done'
            || j.state === 'aborted'
            || j.state === 'error') {
            clearInterval(seqPollId);
            seqPollId = null;
          }
        } catch (_) {}
      }, 500);
    }

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

  Ultra.initEngineering = initEngineering;
  Ultra.stopEngPolling = stopEngPolling;
  Ultra.doEngDisconnect = doEngDisconnect;
})();

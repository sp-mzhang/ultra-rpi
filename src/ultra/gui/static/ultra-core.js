/* Ultra RPi -- Shared namespace, DOM helpers, state, and utilities */
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);

  function hexDV(hex) {
    const b = new Uint8Array(hex.length / 2);
    for (let i = 0; i < b.length; i++)
      b[i] = parseInt(hex.substr(i * 2, 2), 16);
    return new DataView(b.buffer);
  }

  const WAIT_DONE_CMDS = new Set([]);

  async function engCmd(
    cmd, params = {},
    waitDone = undefined,
    timeout = 30,
    lockTimeout = undefined,
  ) {
    if (waitDone === undefined) {
      waitDone = WAIT_DONE_CMDS.has(cmd);
    }
    const body = {
      cmd, params,
      wait_done: waitDone,
      timeout_s: timeout,
    };
    if (lockTimeout !== undefined) {
      body.lock_timeout = lockTimeout;
    }
    try {
      const res = await fetch(
        '/api/stm32/command', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(body),
        },
      );
      if (res.status === 503 && lockTimeout != null) {
        return null;
      }
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

  function drawTimeSeries(canvas, dataArr, opts) {
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const W = canvas.width;
    const H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const maxPts = opts.maxPoints || 200;
    if (dataArr.length < 2) return;

    let dMin = Infinity, dMax = -Infinity;
    for (const v of dataArr) {
      if (v < dMin) dMin = v;
      if (v > dMax) dMax = v;
    }
    if (opts.extraValues) {
      for (const v of opts.extraValues) {
        if (v != null) {
          if (v < dMin) dMin = v;
          if (v > dMax) dMax = v;
        }
      }
    }
    const pad = opts.pad != null ? opts.pad : 1.0;
    dMin -= pad; dMax += pad;
    if (dMax - dMin < 2) {
      dMin -= 1; dMax += 1;
    }

    const LEFT = opts.leftMargin || 30;

    ctx.strokeStyle = 'rgba(128,128,128,0.15)';
    ctx.lineWidth = 1;
    ctx.font = '10px monospace';
    ctx.fillStyle = '#888';
    for (let g = 0; g <= 4; g++) {
      const gy = H - (g / 4) * H;
      ctx.beginPath();
      ctx.moveTo(LEFT, gy);
      ctx.lineTo(W, gy);
      ctx.stroke();
      const label = (dMin + (g / 4) * (dMax - dMin))
        .toFixed(opts.decimals != null ? opts.decimals : 1);
      ctx.fillText(label, 0, gy + 3);
    }

    if (opts.hLines) {
      for (const hl of opts.hLines) {
        if (hl.value == null) continue;
        const ly = H
          - ((hl.value - dMin) / (dMax - dMin)) * H;
        ctx.strokeStyle = hl.color || '#ef4444';
        ctx.lineWidth = 1.5;
        ctx.setLineDash(hl.dash || [6, 4]);
        ctx.beginPath();
        ctx.moveTo(LEFT, ly);
        ctx.lineTo(W, ly);
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }

    const series = opts.series || [
      { data: dataArr, color: '#3b82f6', width: 2 },
    ];
    for (const s of series) {
      const arr = s.data || dataArr;
      const len = arr.length;
      ctx.strokeStyle = s.color || '#3b82f6';
      ctx.lineWidth = s.width || 2;
      ctx.beginPath();
      for (let i = 0; i < len; i++) {
        const px = LEFT
          + (i / (maxPts - 1)) * (W - LEFT);
        const py = H
          - ((arr[i] - dMin) / (dMax - dMin)) * H;
        i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
      }
      ctx.stroke();
    }
  }

  function wireSlider(sliderId, labelId) {
    const sl = $(sliderId);
    const lbl = $(labelId);
    if (sl && lbl) {
      sl.oninput = () => {
        lbl.textContent = sl.value;
      };
    }
    return sl;
  }

  function statusDump(cmd, preId, timeout) {
    return async () => {
      const r = await engCmd(cmd, {}, false, timeout || 3);
      const el = $(preId);
      if (el) {
        el.textContent = r
          ? JSON.stringify(r, null, 2)
          : 'No response';
      }
    };
  }

  window.Ultra = {
    $,
    hexDV,
    engCmd,
    engLog,
    drawTimeSeries,
    wireSlider,
    statusDump,

    state: {
      ws: null,
      wellDefs: {},
      isPaused: false,
      isRunning: false,
    },
  };
})();

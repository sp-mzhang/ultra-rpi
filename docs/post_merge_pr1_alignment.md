# Post-merge PR #1 Alignment Fix

## Context

PR #1 (`sp-gcohen/accel-stream-gui`) rewrites `stm32_interface.py` into separate RX/TX worker threads. It already includes our three features (foil_detect, pre_dispense_cb, pressure streaming), but the pressure sample dict keys differ from what `runner.collect_pressure` expects.

## The Problem

The PR's RX dispatch builds pressure samples with **short keys**:
```python
{'ts': ..., 'p': ..., 'pos': ...}
```

Our `runner.collect_pressure` in `src/ultra/protocol/runner.py` reads **long keys**:
```python
s.get('timestamp_ms', 0)
s.get('pressure', 0)
s.get('position', 0)
```

The CSV writer also uses the long key names. After merge, all pressure values would silently read as 0.

## Fix

**Option chosen: normalize keys at the source** (PR's `_dispatch_rx` in `stm32_interface.py`) so all downstream consumers (runner, CSV writer, GUI) work unchanged.

### Step 1 -- Merge PR #1

```bash
git checkout sp-mzhang-initial-lab-release
git merge sp-gcohen/accel-stream-gui
```

No textual conflicts expected (`git merge-tree` confirmed clean).

### Step 2 -- Align pressure sample keys in `stm32_interface.py`

In the PR's `_dispatch_rx` method (the RX worker thread), where `MSG_PRESSURE` samples are built, change the short keys to match what `runner.collect_pressure` expects:

- `'ts'` -> `'timestamp_ms'`
- `'p'` -> `'pressure'`
- `'pos'` -> `'position'`

This is ~3 line changes in `_dispatch_rx`.

### Step 3 -- Verify `smart_aspirate_at` still passes `foil_detect`

Confirm in the merged `stm32_interface.py` that:
- `smart_aspirate_at(..., foil_detect=True)` passes `'foil_detect': foil_detect` in the cmd dict
- `_pack_command` for `'smart_aspirate'` passes `foil_detect=cmd.get('foil_detect', True)` to `fp.pack_smart_aspirate`

Both are expected to be present from the PR; just a visual check.

### Step 4 -- Verify `pre_dispense_cb` in `cart_dispense_at` / `_bf_at`

Confirm the callback is invoked right before the `cart_dispense` / `cart_dispense_bf` `send_command_wait_done` call. Expected to be present from the PR.

### Step 5 -- Hardware validation

Run one CRP protocol and verify:
- Pressure streaming data appears in the GUI Pressure tab
- Pressure CSV has non-zero `timestamp_ms`, `pressure`, `position` columns
- Timing markers align with pressure batches
- Sensorgram shows peak data (separate from this fix, but good to confirm)

## Files to modify

- `src/ultra/hw/stm32_interface.py` -- ~3 lines in `_dispatch_rx` (rename pressure sample keys)

## Files to verify (read-only)

- `src/ultra/protocol/runner.py` -- `collect_pressure` reads `timestamp_ms`, `pressure`, `position`
- `src/ultra/hw/frame_protocol.py` -- `LIQUID_FLAG_FOIL_DETECT` and `pack_smart_aspirate` foil_detect param

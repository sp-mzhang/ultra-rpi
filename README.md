# ultra-rpi

RPi controller for SiPhox Ultra STM32-based blood-analysis hardware.

## Overview

Lean, headless Python service that:

- Controls Ultra STM32 hardware over UART (centrifuge, gantry, pump, lift,
  drawer, LEDs)
- Acquires optical reader data from PProc MCU over USB serial
- Runs data-driven YAML protocol recipes with pause/resume
- Serves a web GUI on port 8080 for real-time monitoring and control
- Uploads status and results to AWS IoT cloud

## Quick Start

```bash
# Install in development mode
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run the application
python -m ultra.app

# Open browser to http://localhost:8080
```

## Configuration

Edit `config/ultra_default.yaml` to set serial ports, baud rates,
default recipe, and other parameters.

## Recipes

Protocol recipes live in `src/ultra/protocol/recipes/*.yaml`.
See `tsh_ultra.yaml` and `quick_demo.yaml` for examples.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full software
architecture. The [docs/](docs/) folder holds design documentation that
ships with the repository.

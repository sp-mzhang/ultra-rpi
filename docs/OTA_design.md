# STM32H735 Custom UART Bootloader

## Why a Custom Bootloader?

The STM32H735ZGT6 includes a system bootloader in ROM that supports
over-the-air firmware updates via UART. However, the ROM bootloader
uses **fixed pin assignments** defined by ST (see AN2606). For USART2,
the ROM bootloader only listens on **PA2 (TX) / PA3 (RX)**.

On the SiPhox Ultra Compute Carrier PCB, the Raspberry Pi CM5 UART3
(`/dev/ttyAMA3`) is routed to STM32 **USART2 on PD5 (TX) / PD6 (RX)**
with hardware flow control on PD3 (CTS) / PD4 (RTS). PA2 and PA3 are
not connected to the RPi.

```
 ROM Bootloader expects:    PA2/PA3  (not connected)
 PCB actually routes:       PD5/PD6  (connected to RPi UART3)
```

Because the ROM bootloader cannot be reconfigured, a **custom
bootloader** in flash sector 0 is required. It initializes USART2
on the correct pins (PD5/PD6) and implements a simple, reliable
firmware update protocol over that same UART connection.

### Reference

- **AN2606** -- STM32 system memory boot mode (Table 6 for
  STM32H723/733/725/735/730 bootloader pin assignments)
- **AN5419** -- Getting started with STM32H723/733, STM32H725/735
  and STM32H730 hardware development


## Flash Layout

```
 STM32H735 Flash (1 MB)
 ┌──────────────────────────────────────────────────┐
 │ Sector 0  0x08000000  128 KB                     │
 │ ┌──────────────────────────────────────────────┐ │
 │ │ Custom UART Bootloader                       │ │
 │ │  • Checks RTC backup register for magic      │ │
 │ │  • USART2 on PD5/PD6, 115200 8N1            │ │
 │ │  • Jumps to app if no update requested       │ │
 │ └──────────────────────────────────────────────┘ │
 │ Sectors 1-7  0x08020000  896 KB                  │
 │ ┌──────────────────────────────────────────────┐ │
 │ │ Application Firmware                         │ │
 │ │  • Linked at 0x08020000 (APP linker script)  │ │
 │ │  • VTOR offset = 0x20000                     │ │
 │ └──────────────────────────────────────────────┘ │
 └──────────────────────────────────────────────────┘
```


## Boot Decision Flow

On every reset the bootloader runs first:

```
 1. Is RTC_BKP0R == 0xDEADBEEF?
    ├─ YES → Clear flag, enter UART update mode
    └─ NO  → Continue
 2. Is there a valid application at 0x08020000?
    (Valid = stack pointer in DTCM or AXI SRAM range)
    ├─ YES → Jump to application
    └─ NO  → Enter UART update mode (safety fallback)
```

The application triggers an update by writing `0xDEADBEEF` to the
RTC backup register and performing a software reset. This is done
via the `ENTER_BOOTLOADER` command (0x8009) sent from the RPi.


## OTA Update Protocol

Baud: **115200**, 8N1, no hardware flow control.

```
 Phase 1 — Handshake
   Bootloader → Host : 0x7F  (READY, every 500 ms)
   Host → Bootloader  : 0x7F  (SYNC)
   Bootloader → Host : 0x79  (ACK)

 Phase 2 — Firmware info
   Host → Bootloader  : [fw_size : 4 bytes LE] [crc32 : 4 bytes LE]
   Bootloader erases required sectors ...
   Bootloader → Host : 0x79  (ACK)

 Phase 3 — Data transfer
   For each 1024-byte chunk:
     Host → Bootloader  : [data : up to 1024 bytes]
     Bootloader → Host : 0x79  (ACK)

 Phase 4 — Verification
   Bootloader computes CRC-32 over written flash
   Bootloader → Host : 0x79  (OK) or 0x1F  (NACK)
   Bootloader resets into the new application
```

- CRC-32: IEEE 802.3 (same as Python `binascii.crc32`)
- Flash word: 256-bit (32 bytes); last chunk padded with `0xFF`
- Bootloader timeout: 60 seconds. If no host connects, jumps to
  the application (if valid) or keeps waiting.


## How the RPi Triggers an Update

1. RPi sends `ENTER_BOOTLOADER` (command ID `0x8009`) to the
   running STM32 application at **921600 baud, 8N1, rtscts=True**
   using the standard SOH-framed binary protocol.

2. The STM32 application writes `0xDEADBEEF` to `RTC_BKP0R` and
   performs `NVIC_SystemReset()`.

3. On reset, the bootloader reads the magic value, clears it, and
   enters UART update mode at **115200 baud, 8N1, rtscts=False**.

4. The RPi OTA script syncs with the bootloader and streams the
   new firmware binary.


## Building

Requires `arm-none-eabi-gcc`:

```bash
cd ultra-firmware/bootloader
make clean && make
# Output: build/bootloader.bin (should be < 8 KB)
```


## Initial Flash (One-Time via SWD)

The bootloader must be flashed once using an ST-Link debugger:

```bash
# Option A: st-flash
st-flash --reset write build/bootloader.bin 0x08000000

# Option B: openocd
openocd -f interface/stlink.cfg -f target/stm32h7x.cfg \
    -c "program build/bootloader.bin 0x08000000 verify reset exit"
```

After this initial flash, all subsequent firmware updates use OTA
over UART from the RPi.


## Application Linker Changes

The application firmware must be linked to start at `0x08020000`
instead of `0x08000000`:

- Use `STM32H735ZGTX_APP.ld` (FLASH ORIGIN = `0x08020000`,
  LENGTH = 896K)
- Enable `USER_VECT_TAB_ADDRESS` in `system_stm32h7xx.c` with
  `VECT_TAB_OFFSET = 0x20000`


## Recovery

If the application is corrupted and cannot process
`ENTER_BOOTLOADER`:

1. Power-cycle the STM32 -- the bootloader in sector 0 runs first.
2. Since the app is corrupt, the bootloader enters update mode
   automatically.
3. Flash from the RPi with `--skip-reset` (skips the app command).

If the bootloader itself is corrupt, re-flash via SWD.


## Files

| File                      | Description                        |
|---------------------------|------------------------------------|
| `src/main.c`              | Bootloader C source                |
| `startup_bl.s`            | Bootloader startup assembly        |
| `bootloader.ld`           | Linker script (sector 0, 128 KB)   |
| `Makefile`                | Build with `arm-none-eabi-gcc`     |
| `README.md`               | This document                      |

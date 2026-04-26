# "Paranoia" Entropy Scanner v1

**Entropy scanner for binary files** – finds high-entropy regions (crypto keys, encrypted/compressed data) with predictable memory usage, streaming merge, and JSON output.

---

## Features

- **Streaming core** – generator-based; O(1) memory in text mode for both `mmap` and stream paths.
- **Rolling histogram** – entropy recompute is **O(256) per step** (independent of window size).
- **True streaming merge** – merges overlapping windows on the fly without storing all hits.
- **JSON output** with hard limit (`MAX_JSON_HITS = 1,000,000`) to prevent memory blow (bounded, not O(1)).
- **Smart context** – computes highest-entropy subwindow (CPU-expensive) only for strong hits (`threshold + 0.2`).
- **Multi-scale (`--auto`)** – scans with windows 32, 64, 128, 256 (reads file multiple times).
- **STDIN support** – up to 100 MB (buffered in memory to enable analysis).
- **Robustness fixes** – no infinite loop for `chunk=1`, safe `mmap` fallback, graceful cancellation.

---

## Installation

```bash
chmod +x paranoia.py
sudo cp paranoia.py /usr/local/bin/   # optional


usage: paranoia.py [-h] [-t THRESHOLD] [-c CHUNK] [-s STEP] [-o OUTPUT]
                   [--smart] [--auto] [--quiet] [--json] [--version]
                   path

Paranoia Entropy Scanner v6.1 — rolling histogram, streaming merge, JSON limit

positional arguments:
  path                  Path to file or '-' for stdin

options:
  -h, --help            show this help message and exit
  -t, --threshold THRESHOLD
                        Entropy threshold (0–8). For AES/RSA keys use 7.2+ (default: 6.0)
  -c, --chunk CHUNK     Window size in bytes (default: 256)
  -s, --step STEP       Step size in bytes (default: 64)
                        Small steps (e.g., 1) drastically increase CPU time
  -o, --output OUTPUT   Save results to file
  --smart               Show highest-entropy sub-window (only for strong hits)
  --auto                Multi-scale scan (32,64,128,256) – reads file multiple times
  --quiet               Suppress progress bar
  --json                JSON output (machine-readable, capped at 1,000,000 hits)
  --version             Show version

## Examples
Basic firmware scan
./paranoia.py firmware.bin -t 7.2 -c 64 -s 16 --smart
Scan raw device (requires root)
sudo ./paranoia.py /dev/sda -c 4096 --auto

Reading raw devices may be slow and requires appropriate permissions.

Pipe with JSON output
cat encrypted.bin | ./paranoia.py - -t 6.0 --json > report.json
Quiet mode for automation
./paranoia.py firmware.bin -t 7.5 -c 32 -s 8 --quiet -o hits.txt
Output examples
Text mode (merged intervals)
File: firmware.bin | Size: 1048576 bytes
Params: threshold=7.2, step=16, smart=True, auto=False
Window sizes: [64]
--------------------------------------------------------------------------------
0x0000a4f0 - 0x0000a530 (window 64)
0x0001b800 - 0x0001b900 (window 64)
--------------------------------------------------------------------------------
Done. Found suspicious regions: 2

JSON mode (capped)
{
  "total_hits": 2,
  "params": {
    "threshold": "7.2",
    "chunk": "64",
    "step": "16"
  },
  "results": [
    {"offset": 42224, "entropy": 7.8941, "context": ".J...", "chunk_size": 64},
    {"offset": 112640, "entropy": 7.9320, "context": ".d...", "chunk_size": 64}
  ]
}
## When to use

- Firmware analysis
- Reverse engineering
- Searching for embedded keys or encrypted blobs
- Quick triage of unknown binaries

## When not to use

- As a standalone secret detector (high false positives)
- For distinguishing compression vs encryption
- For structured formats without preprocessing
## Entropy reference

- ~0–3: text / zero-filled
- ~4–6: structured binary
- ~7–8: compressed or encrypted data


## Performance notes

- **Step size** – keep `--step ≥ 8` for large files  
  `--step 1` is **extremely slow** on large inputs (≈ O(N × chunk))

- **Memory**
  - Text mode: O(1)
  - JSON mode: bounded by `MAX_JSON_HITS`

- **Large files (>2 GB on 32-bit)** – automatically falls back to stream mode

- **Multi-scale (`--auto`)** – reads the file once per window size


## Limitations

- **Entropy ≠ secret** – high entropy may indicate compression, media data, or randomness  
- Does not distinguish encryption vs compression  
- Results require manual interpretation or additional heuristics  


## License

MIT – see `LICENSE` file.

## Author
Courtesyl

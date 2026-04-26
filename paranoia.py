#!/usr/bin/env python3

import math
import mmap
import os
import sys
import argparse
import json
import io

MAX_CHUNK_SIZE = 10 * 1024 * 1024
DEFAULT_SCALES = [32, 64, 128, 256]
STDIN_MAX_SIZE = 100 * 1024 * 1024
MAX_JSON_HITS = 1_000_000
SMART_MARGIN = 0.2

def calculate_entropy(data):
    if not data:
        return -1.0
    counts = [0] * 256
    for b in data:
        counts[b] += 1
    entropy = 0.0
    length = len(data)
    for c in counts:
        if c:
            p = c / length
            entropy -= p * math.log2(p)
    return entropy

def printable(data, max_len=32):
    return "".join(chr(b) if 32 <= b <= 126 else "." for b in data[:max_len])

def best_context_sliding(chunk, ctx_size=32):
    if len(chunk) <= ctx_size:
        return printable(chunk)
    counts = [0] * 256
    for b in chunk[:ctx_size]:
        counts[b] += 1

    def entropy_from_counts(cnt, sz):
        e = 0.0
        for c in cnt:
            if c:
                p = c / sz
                e -= p * math.log2(p)
        return e

    max_ent = entropy_from_counts(counts, ctx_size)
    best = chunk[:ctx_size]
    for i in range(1, len(chunk) - ctx_size + 1):
        old = chunk[i-1]
        counts[old] -= 1
        new = chunk[i+ctx_size-1]
        counts[new] += 1
        ent = entropy_from_counts(counts, ctx_size)
        if ent > max_ent:
            max_ent = ent
            best = chunk[i:i+ctx_size]
    return printable(best)

def scan_block_rolling_generator(block, base_offset, chunk_size, step, threshold, smart):
    if len(block) < chunk_size:
        if block:
            ent = calculate_entropy(block)
            if ent >= threshold:
                ctx = best_context_sliding(block) if smart else printable(block[:32])
                yield {
                    "offset": base_offset,
                    "entropy": round(ent, 4),
                    "context": ctx,
                    "chunk_size": len(block),
                    "partial": True
                }
        return

    actual_step = step if step <= chunk_size else chunk_size
    if actual_step <= 0:
        return

    window = block[:chunk_size]
    counts = [0] * 256
    for b in window:
        counts[b] += 1

    def entropy_from_counts(cnt, sz):
        e = 0.0
        for c in cnt:
            if c:
                p = c / sz
                e -= p * math.log2(p)
        return e

    entropy = entropy_from_counts(counts, chunk_size)

    offset = 0
    if entropy >= threshold:
        use_smart = smart and entropy >= threshold + SMART_MARGIN
        ctx = best_context_sliding(window) if use_smart else printable(window[:32])
        yield {
            "offset": base_offset + offset,
            "entropy": round(entropy, 4),
            "context": ctx,
            "chunk_size": chunk_size
        }

    max_start = len(block) - chunk_size
    for offset in range(actual_step, max_start + 1, actual_step):
        for i in range(offset - actual_step, offset):
            old_byte = block[i]
            counts[old_byte] -= 1
        for i in range(offset + chunk_size - actual_step, offset + chunk_size):
            new_byte = block[i]
            counts[new_byte] += 1
        entropy = entropy_from_counts(counts, chunk_size)
        if entropy >= threshold:
            use_smart = smart and entropy >= threshold + SMART_MARGIN
            ctx = best_context_sliding(block[offset:offset+chunk_size]) if use_smart else printable(block[offset:offset+chunk_size][:32])
            yield {
                "offset": base_offset + offset,
                "entropy": round(entropy, 4),
                "context": ctx,
                "chunk_size": chunk_size
            }

def merge_intervals_streaming(intervals_iter):
    current = None
    for start, end in intervals_iter:
        if current is None:
            current = (start, end)
        else:
            cur_start, cur_end = current
            if start <= cur_end:
                current = (cur_start, max(cur_end, end))
            else:
                yield current
                current = (start, end)
    if current is not None:
        yield current

def collect_hits_from_generator(hits_gen, out_f, csize):
    def intervals_gen():
        for hit in hits_gen:
            start = hit["offset"]
            end = start + csize
            yield (start, end)
    count = 0
    for start, end in merge_intervals_streaming(intervals_gen()):
        print(f"0x{start:08x} - 0x{end:08x} (window {csize})", file=out_f)
        count += 1
    return count

def scan_file_mmap(f, file_size, args, out_f):
    with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
        if args.json:
            all_hits = []
        else:
            total_merged = 0

        for csize in args.chunk_sizes:
            if not args.quiet and not args.json:
                sys.stderr.write(f"Mmap scanning with window {csize}...\n")

            gen = scan_block_rolling_generator(mm, 0, csize, args.step, args.threshold, args.smart)

            if args.json:
                for hit in gen:
                    if len(all_hits) >= MAX_JSON_HITS:
                        sys.stderr.write(f"JSON hit limit reached ({MAX_JSON_HITS}), truncating.\n")
                        break
                    all_hits.append(hit)
            else:
                merged = collect_hits_from_generator(gen, out_f, csize)
                total_merged += merged

    if args.json:
        return all_hits[:MAX_JSON_HITS]
    else:
        return total_merged

def scan_file_stream(f, file_size, args, out_f):
    if args.json:
        all_hits = []
    else:
        total_merged = 0

    if args.auto and len(args.chunk_sizes) > 1 and not args.quiet and not args.json:
        sys.stderr.write("WARNING: --auto will read the file multiple times.\n")

    for csize in args.chunk_sizes:
        if not args.quiet and not args.json:
            sys.stderr.write(f"Stream scanning with window {csize}...\n")

        if not args.is_stdin:
            f.seek(0)

        buffer_size = 1024 * 1024
        leftover = b''
        global_offset = 0

        def block_generator():
            nonlocal leftover, global_offset
            while True:
                raw = f.read(buffer_size)
                if not raw and not leftover:
                    break
                block = leftover + raw
                if len(block) >= csize:
                    process_len = len(block) - csize + 1
                    for hit in scan_block_rolling_generator(block[:process_len + csize - 1], global_offset,
                                                            csize, args.step, args.threshold, args.smart):
                        yield hit
                    overlap = csize - 1
                    leftover = block[-overlap:] if overlap > 0 and overlap < len(block) else (b'' if overlap == 0 else block)
                    global_offset += process_len
                else:
                    if not raw:
                        for hit in scan_block_rolling_generator(block, global_offset,
                                                                csize, args.step, args.threshold, args.smart):
                            yield hit
                        break
                    leftover = block
                if not args.quiet and not args.json and file_size > 0:
                    progress = min(100.0, (global_offset / file_size) * 100)
                    sys.stderr.write(f"\r[Progress ({csize}b): {progress:>5.1f}%]")
                    sys.stderr.flush()

        if args.json:
            for hit in block_generator():
                if len(all_hits) >= MAX_JSON_HITS:
                    sys.stderr.write(f"JSON hit limit reached ({MAX_JSON_HITS}), truncating.\n")
                    break
                all_hits.append(hit)
        else:
            merged = collect_hits_from_generator(block_generator(), out_f, csize)
            total_merged += merged

    if args.json:
        return all_hits[:MAX_JSON_HITS]
    else:
        return total_merged

def scan_file(args):
    is_stdin = args.path == '-'
    file_size = 0
    f = None

    if is_stdin:
        if args.auto:
            sys.stderr.write("--auto not supported for stdin.\n")
            return
        first_chunk = sys.stdin.buffer.read(STDIN_MAX_SIZE + 1)
        if len(first_chunk) > STDIN_MAX_SIZE:
            sys.stderr.write(f"stdin exceeds {STDIN_MAX_SIZE} bytes limit. Aborting.\n")
            return
        data = first_chunk
        file_size = len(data)
        f = io.BytesIO(data)
        f.name = '<stdin>'
        args.is_stdin = True
    else:
        if not os.path.exists(args.path):
            sys.stderr.write(f"File '{args.path}' not found.\n")
            return
        file_size = os.path.getsize(args.path)
        if file_size == 0:
            sys.stderr.write("File is empty.\n")
            return
        f = open(args.path, "rb")
        args.is_stdin = False

    if not (0.0 <= args.threshold <= 8.0):
        sys.stderr.write("Threshold must be 0..8, reset to 6.0\n")
        args.threshold = 6.0
    if args.chunk <= 0 or args.step <= 0:
        sys.stderr.write("Chunk and step must be > 0.\n")
        if f and not is_stdin:
            f.close()
        return
    if args.step > args.chunk:
        sys.stderr.write(f"step={args.step} > chunk={args.chunk}, forcing step={args.chunk}\n")
        args.step = args.chunk

    if args.auto:
        chunk_sizes = [min(s, file_size if file_size > 0 else MAX_CHUNK_SIZE) for s in DEFAULT_SCALES]
        chunk_sizes = list(dict.fromkeys(chunk_sizes))
    else:
        chunk_sizes = [min(args.chunk, file_size if file_size > 0 else args.chunk)]
    chunk_sizes = [min(c, MAX_CHUNK_SIZE) for c in chunk_sizes]
    args.chunk_sizes = chunk_sizes

    out_f = sys.stdout
    if args.output:
        try:
            out_f = open(args.output, 'w', encoding='utf-8', buffering=1)
        except (OSError, PermissionError, IOError) as e:
            sys.stderr.write(f"Cannot open {args.output}: {e}\n")
            if f and not is_stdin:
                f.close()
            return

    try:
        if not args.json:
            print(f"File: {args.path if not is_stdin else '<stdin>'}", file=out_f)
            print(f"Size: {file_size if file_size > 0 else 'unknown'} bytes", file=out_f)
            print(f"Params: threshold={args.threshold}, step={args.step}, smart={args.smart}, auto={args.auto}", file=out_f)
            print(f"Window sizes: {chunk_sizes}", file=out_f)
            print("-" * 80, file=out_f)

        use_mmap = (not is_stdin and
                    not (sys.maxsize <= 2**32 and file_size > 2 * 1024**3) and
                    file_size > 0)

        if use_mmap:
            result = scan_file_mmap(f, file_size, args, out_f)
        else:
            result = scan_file_stream(f, file_size, args, out_f)

        if args.json:
            json.dump({
                "total_hits": len(result),
                "params": {k: str(v) for k, v in vars(args).items() if not callable(v)},
                "results": result
            }, out_f, indent=2)
        else:
            print("-" * 80, file=out_f)
            msg = f"Done. Found suspicious regions: {result}"
            if not args.quiet:
                sys.stderr.write("\r" + " " * 80 + "\r")
                print(msg, file=sys.stderr)
            if args.output:
                print(msg, file=out_f)

    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted by user.\n")
        sys.exit(1)
    except MemoryError:
        sys.stderr.write("\nMemoryError: Not enough memory to process the file.\n")
        sys.exit(1)
    except (OSError, ValueError, RuntimeError) as e:
        sys.stderr.write(f"\nSystem error: {e}\n")
    except Exception as e:
        sys.stderr.write(f"\nUnexpected error: {e}\n")
    finally:
        if not is_stdin and f:
            f.close()
        if args.output and out_f is not sys.stdout:
            out_f.close()

def main():
    parser = argparse.ArgumentParser(
        description="'Paranoia' Entropy Scanner v1 — rolling histogram, true streaming merge, JSON limit",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("path", help="Path to file or '-' for stdin")
    parser.add_argument("-t", "--threshold", type=float, default=6.0,
                        help="Entropy threshold (0-8). For AES/RSA keys use 7.2+")
    parser.add_argument("-c", "--chunk", type=int, default=256, help="Window size in bytes")
    parser.add_argument("-s", "--step", type=int, default=64, help="Step size in bytes. Small steps (e.g., 1) drastically increase CPU time.")
    parser.add_argument("-o", "--output", help="Save results to file")
    parser.add_argument("--smart", action="store_true", help="Show highest‑entropy sub‑window (only for high entropy hits)")
    parser.add_argument("--auto", action="store_true", help="Multi‑scale scan (32,64,128,256)")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress bar")
    parser.add_argument("--json", action="store_true", help="JSON output (machine‑readable) – hits capped at 1M.")
    parser.add_argument("--version", action="version", version="'Paranoia' v1 (MIT)")

    args = parser.parse_args()
    args.path = args.path.strip('"').strip("'")
    scan_file(args)

if __name__ == "__main__":
    main()
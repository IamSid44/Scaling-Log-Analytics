#!/usr/bin/env python3
"""CLI dashboard: queries the coordinator on a timer and redraws the analytics
while the stream is running, recording its own query latency.

    python3 dashboard.py node01:50051 --interval 0.5
    python3 dashboard.py node01:50051 --once            # one snapshot, then exit
    python3 dashboard.py node01:50051 --samples 200 --latency-out lat.csv

The panel is redrawn by moving the cursor home and overwriting (no clear-screen,
so it does not flicker). --plain appends panels instead, for files and logs.
"""

import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc

import analytics_pb2 as pb
import analytics_pb2_grpc as pb_grpc
from aggregate import RT_SCALE, render_report

HOME = "\x1b[H"
CLEAR = "\x1b[2J"
HIDE = "\x1b[?25l"
SHOW = "\x1b[?25h"


def width():
    try:
        return max(60, os.get_terminal_size().columns)
    except OSError:
        return 100


def human(n):
    for unit, div in (("B", 10 ** 9), ("M", 10 ** 6), ("K", 10 ** 3)):
        if abs(n) >= div:
            return "%.2f%s" % (n / float(div), unit)
    return str(int(n))


def bytes_human(n):
    for unit, div in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if n >= div:
            return "%.2f %s" % (n / float(div), unit)
    return "%d B" % n


def percentile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def render(snap, stats, lat_ms, cols):
    """Build the whole panel as a list of lines."""
    L = []
    bar = "=" * min(cols, 78)

    def row(label, value):
        L.append("  %-26s %s" % (label, value))

    state = "COMPLETE" if snap.ingestion_done else "STREAMING"
    L.append(bar)
    L.append("  REAL-TIME LOG ANALYTICS          [%s]" % state)
    L.append(bar)

    rps = (stats.records_ingested / stats.stream_seconds) if stats.stream_seconds > 0 else 0.0
    row("records ingested", "%s  (%s rec/s)" % (human(stats.records_ingested), human(rps)))
    row("batches", human(stats.batches_ingested))
    row("stream time", "%.1f s" % stats.stream_seconds)
    row("workers", "%d  (%s)" % (snap.worker_count, stats.distribution))

    L.append("")
    L.append("  -- request counts " + "-" * max(0, min(cols, 78) - 20))
    row("TOTAL_REQUESTS", human(snap.total_requests))
    row("SUCCESSFUL_REQUESTS", human(snap.successful_requests))
    row("FAILED_REQUESTS", human(snap.failed_requests))
    row("STATUS 2xx / 3xx / 4xx / 5xx",
        "%s / %s / %s / %s" % (human(snap.status_2xx), human(snap.status_3xx),
                               human(snap.status_4xx), human(snap.status_5xx)))

    L.append("")
    L.append("  -- response time and volume " + "-" * max(0, min(cols, 78) - 30))
    if snap.total_requests > 0:
        row("AVERAGE_RESPONSE_TIME", "%.6f s" % (snap.rt_sum / RT_SCALE / snap.total_requests))
        row("MIN / MAX_RESPONSE_TIME",
            "%.6f s / %.6f s" % (snap.rt_min / RT_SCALE, snap.rt_max / RT_SCALE))
    else:
        row("AVERAGE_RESPONSE_TIME", "-")
        row("MIN / MAX_RESPONSE_TIME", "-")
    row("TOTAL_BYTES", bytes_human(snap.total_bytes))
    row("BUSIEST_INTERVAL", "%d  (%s requests in that 60s)"
        % (snap.busiest_interval, human(snap.busiest_count)))

    L.append("")
    L.append("  -- top servers " + "-" * max(0, min(cols, 78) - 17))
    L.append("     %-8s %12s %16s" % ("server", "requests", "avg response"))
    for s in snap.top_servers[:8]:
        avg = s.rt_sum / RT_SCALE / s.count if s.count else 0.0
        L.append("     %-8d %12s %14.6f s" % (s.server_id, human(s.count), avg))

    L.append("")
    L.append("  -- top endpoints " + "-" * max(0, min(cols, 78) - 19))
    L.append("     %-8s %12s %16s" % ("endpoint", "requests", "bytes"))
    for e in snap.top_endpoints[:8]:
        L.append("     %-8d %12s %16s" % (e.endpoint_id, human(e.count),
                                          bytes_human(e.bytes)))

    L.append("")
    L.append("  -- per-worker load " + "-" * max(0, min(cols, 78) - 21))
    # shows the skew of the `hash` distribution as it happens
    tot = sum(w.records for w in stats.workers) or 1
    for w in stats.workers:
        share = 100.0 * w.records / tot
        blocks = int(share / 4)
        L.append("     %-6s %-22s %10s  %5.1f%%  %s"
                 % (w.worker_id, w.address, human(w.records), share,
                    "#" * blocks + ("" if w.healthy else "  [UNHEALTHY]")))

    if lat_ms:
        s = sorted(lat_ms)
        L.append("")
        L.append("  -- query latency (this client, %d samples) %s"
                 % (len(s), "-" * max(0, min(cols, 78) - 45)))
        row("last / p50 / p95 / max",
            "%.1f / %.1f / %.1f / %.1f ms"
            % (lat_ms[-1], percentile(s, 0.50), percentile(s, 0.95), s[-1]))
        row("coordinator merge (last)", "%.1f ms" % (snap.merge_seconds * 1000))

    L.append(bar)
    return L


def main():
    ap = argparse.ArgumentParser(description="CLI dashboard for the streaming analytics")
    ap.add_argument("address", nargs="?", default="localhost:50051")
    ap.add_argument("--interval", type=float, default=0.5,
                    help="seconds between refreshes (default 0.5)")
    ap.add_argument("--top-k", type=int, default=0)
    ap.add_argument("--once", action="store_true",
                    help="print one snapshot and exit")
    ap.add_argument("--samples", type=int, default=0,
                    help="stop after this many refreshes (0 = until interrupted "
                         "or ingestion completes)")
    ap.add_argument("--follow", action="store_true",
                    help="keep refreshing even after ingestion completes")
    ap.add_argument("--latency-out", default=None,
                    help="write every query latency to this CSV")
    ap.add_argument("--report-out", default=None,
                    help="write the final report in reference format here on exit")
    ap.add_argument("--plain", action="store_true",
                    help="no cursor control -- append each snapshot instead "
                         "(use when the output is a file or a SLURM log)")
    args = ap.parse_args()

    channel = grpc.insecure_channel(
        args.address,
        options=[("grpc.max_receive_message_length", 256 * 1024 * 1024)])
    try:
        grpc.channel_ready_future(channel).result(timeout=60.0)
    except grpc.FutureTimeoutError:
        sys.exit("dashboard: no coordinator answering at %s" % args.address)
    stub = pb_grpc.CoordinatorServiceStub(channel)

    interactive = sys.stdout.isatty() and not args.plain
    lat_ms = []
    # each sample also keeps the totals it saw (monotonicity check)
    samples = []
    n = 0
    snap = None

    # SIGTERM/SIGINT end the loop cleanly, so the CSV is still written
    stopping = {"now": False}

    def _stop(signum, frame):
        stopping["now"] = True

    try:
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
    except (ValueError, AttributeError):
        pass                     # not the main thread, or no such signal here

    if interactive:
        sys.stdout.write(CLEAR + HIDE)
    try:
        while not stopping["now"]:
            t0 = time.perf_counter()
            try:
                snap = stub.GetAnalytics(pb.AnalyticsRequest(top_k=args.top_k),
                                         timeout=30.0)
                stats = stub.GetSystemStats(pb.StatsRequest(), timeout=30.0)
            except grpc.RpcError as e:
                sys.stdout.write("\n[dashboard] query failed: %s\n" % e.code().name)
                sys.stdout.flush()
                time.sleep(args.interval)
                continue
            ms = (time.perf_counter() - t0) * 1000.0
            lat_ms.append(ms)
            samples.append((ms, snap.total_requests, stats.records_ingested,
                            snap.merge_seconds * 1000.0,
                            1 if snap.ingestion_done else 0))

            cols = width()
            lines = render(snap, stats, lat_ms, cols)
            if interactive:
                # pad to the terminal width so no stale text is left behind
                sys.stdout.write(HOME + "\n".join(l.ljust(cols - 1)[:cols - 1]
                                                  for l in lines) + "\n")
            else:
                sys.stdout.write("\n".join(lines) + "\n\n")
            sys.stdout.flush()

            n += 1
            if args.once:
                break
            if args.samples and n >= args.samples:
                break
            if snap.ingestion_done and not args.follow:
                break
            slept = 0.0
            while slept < args.interval and not stopping["now"]:
                step = min(0.05, args.interval - slept)
                time.sleep(step)
                slept += step
    except KeyboardInterrupt:
        pass
    finally:
        if interactive:
            sys.stdout.write(SHOW + "\n")
            sys.stdout.flush()

    if args.latency_out and samples:
        with open(args.latency_out, "w", newline="\n") as f:
            f.write("sample,latency_ms,total_requests,records_ingested,"
                    "merge_ms,ingestion_done\n")
            for i, (ms, tot, ing, merge, done) in enumerate(samples):
                f.write("%d,%.4f,%d,%d,%.4f,%d\n" % (i, ms, tot, ing, merge, done))
        sys.stderr.write("[dashboard] wrote %s (%d samples)\n"
                         % (args.latency_out, len(samples)))

    if args.report_out and snap is not None:
        k = args.top_k if args.top_k > 0 else len(snap.top_servers)
        with open(args.report_out, "w", newline="\n") as f:
            f.write(render_report(snap, k))
        sys.stderr.write("[dashboard] wrote %s\n" % args.report_out)

    if lat_ms:
        s = sorted(lat_ms)
        sys.stderr.write("QUERY_SAMPLES %d\nQUERY_P50_MS %.4f\nQUERY_P95_MS %.4f\n"
                         "QUERY_MAX_MS %.4f\n"
                         % (len(s), percentile(s, 0.50), percentile(s, 0.95), s[-1]))
    channel.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Generate a reproducible server-log dataset for the benchmarks.

File format written:
    line 1 : N K S
    then   : N lines of
             timestamp server_id endpoint_id user_id status_code response_time bytes_sent

Everything is driven by --seed, so the same command always produces the same
bytes. Distributions are chosen so the required computations are meaningful
rather than degenerate:

  server_id / endpoint_id
      Zipf-like popularity (weight ~ 1/(rank+1)^alpha). A uniform draw would
      make every server's count equal up to noise, so TOP_SERVERS would be an
      arbitrary tie-break and would not test the ordering rule at all. Zipf
      gives a clear, stable ranking.

  response_time
      Log-normal-ish: most requests fast, a long right tail. Written with
      --rt-decimals places (default 6, matching the required output precision
      and the 10^-6 fixed-point scale), so the parser is exercised across its
      full resolution rather than only the first three digits.

  status_code
      ~89% 2xx, 4% 3xx, 5% 4xx, 2% 5xx -- enough of each bucket that all four
      STATUS_* counters and the success/failure split are non-zero.

  bytes_sent
      Heavy-tailed integer, correlated with the endpoint (some endpoints serve
      big payloads), so TOP_ENDPOINTS by count and by bytes are not the same
      ordering -- which is exactly the case that catches a mixed-up field.

  timestamp
      Uniform over [--start, --start + --span), plus a deliberate burst of
      --burst-frac of all records into a single 60-second window, so
      BUSIEST_INTERVAL has an unambiguous winner instead of a near-tie.

Examples:
    python3 scripts/gen_logs.py -n 1000000 -o data/logs_1m.txt --seed 42
    python3 scripts/gen_logs.py -n 5000 -k 5 --servers 4 -o tests/small.txt
"""

import argparse
import random
import sys

CHUNK = 200000


def zipf_cum_weights(n, alpha):
    w, acc = [], 0.0
    for i in range(n):
        acc += 1.0 / ((i + 1) ** alpha)
        w.append(acc)
    return w


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-n", "--records", type=int, required=True)
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("-k", "--topk", type=int, default=10)
    ap.add_argument("--servers", type=int, default=64)
    ap.add_argument("--endpoints", type=int, default=512)
    ap.add_argument("--users", type=int, default=100000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", type=int, default=1700000000,
                    help="first timestamp (epoch seconds)")
    ap.add_argument("--span", type=int, default=86400,
                    help="time span in seconds (default: one day)")
    ap.add_argument("--burst-frac", type=float, default=0.04,
                    help="fraction of records concentrated in one 60s window")
    ap.add_argument("--alpha", type=float, default=1.1,
                    help="Zipf exponent for server/endpoint popularity")
    ap.add_argument("--rt-decimals", type=int, default=6,
                    help="decimal places written for response_time (default 6)")
    args = ap.parse_args()

    if args.records < 0:
        sys.exit("error: -n must be >= 0")
    if args.servers < 1 or args.endpoints < 1:
        sys.exit("error: --servers and --endpoints must be >= 1")
    if args.span < 60:
        sys.exit("error: --span must be >= 60")

    rng = random.Random(args.seed)

    srv_ids = list(range(args.servers))
    ep_ids = list(range(args.endpoints))
    srv_w = zipf_cum_weights(args.servers, args.alpha)
    ep_w = zipf_cum_weights(args.endpoints, args.alpha)

    # Per-endpoint payload scale, so bytes and counts rank differently.
    ep_scale = [rng.choice([200, 1200, 8000, 60000, 400000]) for _ in ep_ids]

    status_pool = [200] * 78 + [201] * 6 + [204] * 5 + [301] * 2 + [304] * 2 + \
                  [400] * 2 + [403] + [404] * 2 + [500] + [503]

    burst_start = args.start + rng.randrange(0, max(1, args.span - 60))
    n_burst = int(args.records * args.burst_frac)
    burst_at = set(rng.sample(range(args.records), n_burst)) if n_burst else set()

    written = 0
    with open(args.output, "w", buffering=1 << 20) as f:
        f.write(f"{args.records} {args.topk} {args.servers}\n")
        while written < args.records:
            m = min(CHUNK, args.records - written)
            servers = rng.choices(srv_ids, cum_weights=srv_w, k=m)
            endpoints = rng.choices(ep_ids, cum_weights=ep_w, k=m)
            statuses = rng.choices(status_pool, k=m)

            lines = []
            for j in range(m):
                idx = written + j
                if idx in burst_at:
                    ts = burst_start + rng.randrange(0, 60)
                else:
                    ts = args.start + rng.randrange(0, args.span)
                sid = servers[j]
                eid = endpoints[j]
                uid = rng.randrange(args.users)
                sc = statuses[j]
                # log-normal-ish response time, in seconds
                rt = round(rng.lognormvariate(-2.0, 0.9), args.rt_decimals)
                by = int(ep_scale[eid] * rng.lognormvariate(0.0, 0.7))
                lines.append(f"{ts} {sid} {eid} {uid} {sc} {rt:.{args.rt_decimals}f} {by}")
            f.write("\n".join(lines))
            f.write("\n")
            written += m

    print(f"wrote {args.records} records to {args.output} "
          f"(K={args.topk}, servers={args.servers}, endpoints={args.endpoints}, "
          f"span={args.span}s, seed={args.seed})")


if __name__ == "__main__":
    main()

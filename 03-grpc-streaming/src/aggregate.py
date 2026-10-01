"""Parsing, aggregation, merging and report rendering, shared by the worker,
the coordinator and the producer.

Response times are int64 microseconds and every field is an integer, so merging
partials is associative and the result is byte-identical to the C++
sequential reference for any number of workers, distribution or batch size.
"""

import heapq

RT_SCALE = 1000000
RT_PRECISION = 6

# min/max of an empty aggregate
RT_MIN_EMPTY = None
RT_MAX_EMPTY = None


def parse_rt_micros(tok):
    """'0.523481' -> 523481, exactly, with no float in the path.

    Mirrors read_fixed() in loganalytics.hpp, including its rule that
    digits past the sixth decimal are truncated, not rounded.
    """
    neg = False
    i = 0
    n = len(tok)
    if i < n and tok[i] in "+-":
        neg = tok[i] == "-"
        i += 1

    whole = 0
    while i < n and tok[i].isdigit():
        whole = whole * 10 + (ord(tok[i]) - 48)
        i += 1

    frac = 0
    if i < n and tok[i] == ".":
        i += 1
        scale = RT_SCALE
        while i < n and tok[i].isdigit():
            if scale > 1:
                scale //= 10
                frac += (ord(tok[i]) - 48) * scale
            i += 1

    v = whole * RT_SCALE + frac
    return -v if neg else v


def parse_rt_fast(tok):
    """parse_rt_micros for the common 'digits[.digits]' shape, ~4x faster.

    Anything else falls back to parse_rt_micros, so the two always agree.
    """
    whole, dot, frac = tok.partition(".")
    if whole.isdigit() and (not frac or frac.isdigit()) and (whole or frac):
        return int(whole) * RT_SCALE + (int((frac + "000000")[:6]) if frac else 0)
    if not whole and frac.isdigit():
        return int((frac + "000000")[:6])
    return parse_rt_micros(tok)


def parse_line(line):
    """One log line -> a 7-tuple, or None if it is malformed.

        timestamp server_id endpoint_id user_id status_code response_time bytes_sent
    """
    parts = line.split()
    if len(parts) < 7:
        return None
    try:
        return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]),
                int(parts[4]), parse_rt_fast(parts[5]), int(parts[6]))
    except ValueError:
        return None


def fold_lines(agg, text):
    """Parse a block of raw log lines and fold each record into `agg`.

    The worker's per-record work under the `raw` wire format. Blank lines are
    skipped and unparsable lines counted as malformed, as in 01-mpi.
    Returns the number of records folded.
    """
    add = agg.add
    n = 0
    for line in text.split("\n"):
        rec = parse_line(line)
        if rec is None:
            if line.strip():
                agg.malformed += 1
            continue
        add(*rec)
        n += 1
    return n


def read_header(path):
    """First line of the dataset: 'N K S'. Returns (N, K, S)."""
    with open(path) as f:
        first = f.readline().split()
    if len(first) < 3:
        raise ValueError("%s: first line must be 'N K S'" % path)
    return int(first[0]), int(first[1]), int(first[2])


class Aggregate(object):
    """A partial (or merged) analytics state, O(servers + endpoints + intervals) in size."""

    __slots__ = ("total", "successful", "failed", "rt_sum", "rt_min", "rt_max",
                 "bytes_total", "status", "servers", "endpoints", "intervals",
                 "batches", "busy_seconds", "malformed")

    def __init__(self):
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.rt_sum = 0
        self.rt_min = RT_MIN_EMPTY
        self.rt_max = RT_MAX_EMPTY
        self.bytes_total = 0
        self.status = [0, 0, 0, 0]          # 2xx, 3xx, 4xx, 5xx
        self.servers = {}                   # sid -> [count, rt_sum]
        self.endpoints = {}                 # eid -> [count, bytes]
        self.intervals = {}                 # interval_id -> count
        self.batches = 0
        self.busy_seconds = 0.0
        self.malformed = 0

    # -- folding ------------------------------------------------------------

    def add(self, ts, sid, eid, uid, sc, rt, nbytes):
        self.total += 1
        if sc < 400:
            self.successful += 1
        else:
            self.failed += 1

        # 1xx counts as successful but is in no STATUS_* bucket, nor is >= 600
        # (the reference convention; see tests/edge_status)
        if 200 <= sc < 300:
            self.status[0] += 1
        elif 300 <= sc < 400:
            self.status[1] += 1
        elif 400 <= sc < 500:
            self.status[2] += 1
        elif 500 <= sc < 600:
            self.status[3] += 1

        self.rt_sum += rt
        if self.rt_min is None or rt < self.rt_min:
            self.rt_min = rt
        if self.rt_max is None or rt > self.rt_max:
            self.rt_max = rt
        self.bytes_total += nbytes

        if sid >= 0:
            s = self.servers.get(sid)
            if s is None:
                self.servers[sid] = [1, rt]
            else:
                s[0] += 1
                s[1] += rt
        if eid >= 0:
            e = self.endpoints.get(eid)
            if e is None:
                self.endpoints[eid] = [1, nbytes]
            else:
                e[0] += 1
                e[1] += nbytes

        iv = ts // 60
        self.intervals[iv] = self.intervals.get(iv, 0) + 1

    def add_record(self, r):
        """Fold one protobuf Record."""
        self.add(r.timestamp, r.server_id, r.endpoint_id, r.user_id,
                 r.status_code, r.rt_micros, r.bytes_sent)

    # -- merging ------------------------------------------------------------

    def merge(self, other):
        """Fold another Aggregate into this one. Associative and commutative."""
        self.total += other.total
        self.successful += other.successful
        self.failed += other.failed
        self.rt_sum += other.rt_sum
        self.bytes_total += other.bytes_total
        for i in range(4):
            self.status[i] += other.status[i]

        if other.rt_min is not None:
            self.rt_min = other.rt_min if self.rt_min is None \
                else min(self.rt_min, other.rt_min)
        if other.rt_max is not None:
            self.rt_max = other.rt_max if self.rt_max is None \
                else max(self.rt_max, other.rt_max)

        for sid, (c, s) in other.servers.items():
            cur = self.servers.get(sid)
            if cur is None:
                self.servers[sid] = [c, s]
            else:
                cur[0] += c
                cur[1] += s
        for eid, (c, b) in other.endpoints.items():
            cur = self.endpoints.get(eid)
            if cur is None:
                self.endpoints[eid] = [c, b]
            else:
                cur[0] += c
                cur[1] += b
        for iv, c in other.intervals.items():
            self.intervals[iv] = self.intervals.get(iv, 0) + c

        self.batches += other.batches
        self.busy_seconds += other.busy_seconds
        self.malformed += other.malformed
        return self

    # -- ranking ------------------------------------------------------------

    def busiest_interval(self):
        """(interval_id, count). Ties go to the smallest interval id."""
        if not self.intervals:
            return (0, 0)
        best_id, best_count = None, -1
        for iv, c in self.intervals.items():
            # ties go to the smaller id, independent of dict order
            if best_id is None or c > best_count or (c == best_count and iv < best_id):
                best_id, best_count = iv, c
        return (best_id, best_count)

    def top_servers(self, k):
        """[(sid, count, rt_sum)], decreasing count then increasing id."""
        items = [(-c, sid, s) for sid, (c, s) in self.servers.items() if c > 0]
        items = heapq.nsmallest(k, items) if k and k > 0 else sorted(items)
        return [(sid, -negc, s) for negc, sid, s in items]

    def top_endpoints(self, k):
        items = [(-c, eid, b) for eid, (c, b) in self.endpoints.items() if c > 0]
        items = heapq.nsmallest(k, items) if k and k > 0 else sorted(items)
        return [(eid, -negc, b) for negc, eid, b in items]


# -- report rendering (reference report format) -----------------------------------

def render_report(snap, k):
    """AnalyticsSnapshot (or Aggregate) -> the reference report, as a string.

    The divisions happen in the same order as print_report() in loganalytics.hpp
    (rt_sum / RT_SCALE / total), so the doubles match to the last bit.
    """
    total = snap.total_requests
    rt_sum = snap.rt_sum

    if total > 0:
        avg = rt_sum / RT_SCALE / total
        mn = snap.rt_min / RT_SCALE
        mx = snap.rt_max / RT_SCALE
    else:
        avg = mn = mx = 0.0

    out = [
        "TOTAL_REQUESTS %d" % total,
        "SUCCESSFUL_REQUESTS %d" % snap.successful_requests,
        "FAILED_REQUESTS %d" % snap.failed_requests,
        "AVERAGE_RESPONSE_TIME %.*f" % (RT_PRECISION, avg),
        "MIN_RESPONSE_TIME %.*f" % (RT_PRECISION, mn),
        "MAX_RESPONSE_TIME %.*f" % (RT_PRECISION, mx),
        "TOTAL_BYTES %d" % snap.total_bytes,
        "STATUS_2XX %d" % snap.status_2xx,
        "STATUS_3XX %d" % snap.status_3xx,
        "STATUS_4XX %d" % snap.status_4xx,
        "STATUS_5XX %d" % snap.status_5xx,
        "BUSIEST_INTERVAL %d %d" % (snap.busiest_interval, snap.busiest_count),
        "TOP_SERVERS",
    ]
    for s in snap.top_servers[:k] if k else snap.top_servers:
        a = s.rt_sum / RT_SCALE / s.count if s.count > 0 else 0.0
        out.append("%d %d %.*f" % (s.server_id, s.count, RT_PRECISION, a))
    out.append("TOP_ENDPOINTS")
    for e in snap.top_endpoints[:k] if k else snap.top_endpoints:
        out.append("%d %d %d" % (e.endpoint_id, e.count, e.bytes))
    return "\n".join(out) + "\n"

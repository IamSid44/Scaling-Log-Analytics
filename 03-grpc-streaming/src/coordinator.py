#!/usr/bin/env python3
"""Coordinator: receives the producer's stream, routes batches to the workers,
and answers queries by merging the workers' partial states.

    python3 coordinator.py 0.0.0.0:50051 --workers node02:50100,node03:50100

It holds no analytics state itself. Each worker gets one long-lived ProcessBatch
stream fed from a bounded queue (backpressure: a full queue stalls the producer).
Ranking (top-K, busiest interval) happens only here, after the merge.
A query's snapshot is not a globally consistent cut, but the final answer is
exact: StreamRecords returns only after every worker has acknowledged its stream.
The worker streams are opened once, so a coordinator serves one producer stream.
"""

import argparse
import os
import queue
import sys
import threading
import time
from concurrent import futures

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc

import analytics_pb2 as pb
import analytics_pb2_grpc as pb_grpc
from aggregate import Aggregate

CHANNEL_OPTS = [
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
]


def batch_size(batch):
    """Records in a batch, in either wire format (see analytics.proto)."""
    return batch.count if batch.count else len(batch.records)


class WorkerLink(object):
    """One worker: a channel, an open ingestion stream, and a sender thread."""

    def __init__(self, address, worker_id, queue_depth):
        self.address = address
        self.worker_id = worker_id
        self.channel = grpc.insecure_channel(address, options=CHANNEL_OPTS)
        self.stub = pb_grpc.WorkerServiceStub(self.channel)
        self.q = queue.Queue(maxsize=queue_depth)
        self.records = 0
        self.batches = 0
        self.error = None
        self._ack = None
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run,
                                        name="send-" + self.worker_id, daemon=True)
        self._thread.start()

    def _drain(self):
        while True:
            item = self.q.get()
            if item is None:          # the sentinel that half-closes the stream
                return
            yield item

    def _run(self):
        try:
            self._ack = self.stub.ProcessBatch(self._drain())
        except grpc.RpcError as e:
            self.error = "%s: %s" % (e.code().name, e.details())
            # drain the queue so a sender blocked on a dead worker is released
            try:
                while True:
                    self.q.get_nowait()
            except queue.Empty:
                pass

    def send(self, batch, n_records):
        if self.error:
            return False
        self.q.put(batch)             # blocks when full -- this is backpressure
        self.records += n_records
        self.batches += 1
        return True

    def close(self, timeout=300.0):
        self.q.put(None)
        if self._thread:
            self._thread.join(timeout=timeout)
        return self._ack

    def healthy(self):
        return self.error is None


class CoordinatorServicer(pb_grpc.CoordinatorServiceServicer):

    def __init__(self, worker_addresses, distribution, top_k, queue_depth,
                 quiet=False):
        self.links = [WorkerLink(a, "w%d" % i, queue_depth)
                      for i, a in enumerate(worker_addresses)]
        self.distribution = distribution
        self.top_k = top_k
        self._quiet = quiet

        self._lock = threading.Lock()
        self._records = 0
        self._batches = 0
        self._t_first = None
        self._t_last = None
        self._ingesting = False
        self._done = False
        self._rr = 0
        # parallel fan-out for queries
        self._pool = futures.ThreadPoolExecutor(
            max_workers=max(4, len(self.links) * 2))

        for link in self.links:
            link.start()

    def _log(self, fmt, *args):
        if not self._quiet:
            sys.stderr.write("[coordinator] " + (fmt % args) + "\n")
            sys.stderr.flush()

    # -- routing ------------------------------------------------------------

    def _route(self, batch):
        """Send one batch to the workers.

        roundrobin: the whole batch to the next worker (default; even load).
        hash: records split by server_id % W (per-record work here; load follows the
        Zipf skew of the data). Both give the same final answer.
        """
        n = len(self.links)
        size = batch_size(batch)
        if n == 1:
            self.links[0].send(batch, size)
            return

        if self.distribution == "hash" and batch.count:
            # raw lines: read each line's server_id
            buckets = [[] for _ in range(n)]
            for line in batch.lines.decode().split("\n"):
                if not line:
                    continue
                parts = line.split(None, 2)
                try:
                    k = int(parts[1]) % n
                except (IndexError, ValueError):
                    k = 0                 # malformed: any worker will count it
                buckets[k].append(line)
            for i, lines in enumerate(buckets):
                if lines:
                    self.links[i].send(pb.RecordBatch(
                        lines="\n".join(lines).encode(), count=len(lines),
                        batch_id=batch.batch_id, source=batch.source), len(lines))
        elif self.distribution == "hash":
            buckets = [[] for _ in range(n)]
            for r in batch.records:
                buckets[r.server_id % n].append(r)
            for i, recs in enumerate(buckets):
                if recs:
                    self.links[i].send(pb.RecordBatch(
                        records=recs, batch_id=batch.batch_id, source=batch.source),
                        len(recs))
        else:
            self.links[self._rr % n].send(batch, size)
            self._rr += 1

    # -- RPCs ---------------------------------------------------------------

    def StreamRecords(self, request_iterator, context):
        with self._lock:
            self._ingesting = True
            self._done = False
            if self._t_first is None:
                self._t_first = time.perf_counter()
            t0 = self._t_first
        c0 = time.process_time()      # CPU of every thread in this process

        n_rec = n_batch = 0
        try:
            for batch in request_iterator:
                self._route(batch)
                size = batch_size(batch)
                n_rec += size
                n_batch += 1
                with self._lock:
                    self._records += size
                    self._batches += 1
                    self._t_last = time.perf_counter()
        except grpc.RpcError as e:
            self._log("ingestion stream aborted: %s", e)

        # close every worker stream and wait for its ack before returning,
        # so queries after this point see every record
        self._log("producer finished (%d records); draining workers ...", n_rec)
        for link in self.links:
            ack = link.close()
            if link.error:
                self._log("worker %s failed: %s", link.worker_id, link.error)
            elif ack is not None and ack.records != link.records:
                self._log("worker %s acknowledged %d of %d records",
                          link.worker_id, ack.records, link.records)

        elapsed = max(1e-9, time.perf_counter() - t0)
        cpu = time.process_time() - c0
        with self._lock:
            self._ingesting = False
            self._done = True

        # read by benchmark.sh: CPU / wall = cores busy
        sys.stderr.write("INGEST_CPU %.6f %.6f\n" % (cpu, elapsed))
        sys.stderr.flush()

        self._log("ingestion complete: %d record(s) in %d batch(es), %.3fs, %.0f rec/s",
                  n_rec, n_batch, elapsed, n_rec / elapsed)
        return pb.IngestSummary(records_received=n_rec, batches_received=n_batch,
                                seconds=elapsed, records_per_sec=n_rec / elapsed)

    def GetAnalytics(self, request, context):
        t0 = time.perf_counter()
        k = request.top_k if request.top_k > 0 else self.top_k

        def fetch(link):
            try:
                return link.stub.GetPartial(pb.PartialRequest(), timeout=30.0)
            except grpc.RpcError as e:
                self._log("GetPartial from %s failed: %s", link.worker_id, e.code().name)
                return None

        merged = Aggregate()
        for part in self._pool.map(fetch, self.links):
            if part is None:
                continue
            a = Aggregate()
            a.total = part.total_requests
            a.successful = part.successful_requests
            a.failed = part.failed_requests
            a.rt_sum = part.rt_sum
            # an empty worker sends 0/0 for min/max: skip them
            if part.total_requests > 0:
                a.rt_min = part.rt_min
                a.rt_max = part.rt_max
            a.bytes_total = part.total_bytes
            a.status = [part.status_2xx, part.status_3xx,
                        part.status_4xx, part.status_5xx]
            a.servers = {s.server_id: [s.count, s.rt_sum] for s in part.servers}
            a.endpoints = {e.endpoint_id: [e.count, e.bytes] for e in part.endpoints}
            a.intervals = {i.interval_id: i.count for i in part.intervals}
            a.batches = part.batches_processed
            a.busy_seconds = part.busy_seconds
            merged.merge(a)

        bi, bc = merged.busiest_interval()
        snap = pb.AnalyticsSnapshot(
            total_requests=merged.total,
            successful_requests=merged.successful,
            failed_requests=merged.failed,
            rt_sum=merged.rt_sum,
            rt_min=merged.rt_min if merged.rt_min is not None else 0,
            rt_max=merged.rt_max if merged.rt_max is not None else 0,
            total_bytes=merged.bytes_total,
            status_2xx=merged.status[0], status_3xx=merged.status[1],
            status_4xx=merged.status[2], status_5xx=merged.status[3],
            busiest_interval=bi, busiest_count=bc,
            worker_count=len(self.links))
        snap.top_servers.extend(
            pb.RankedServer(server_id=s, count=c, rt_sum=r)
            for s, c, r in merged.top_servers(k))
        snap.top_endpoints.extend(
            pb.RankedEndpoint(endpoint_id=e, count=c, bytes=b)
            for e, c, b in merged.top_endpoints(k))

        with self._lock:
            snap.ingestion_done = self._done
            if self._t_first is not None:
                end = self._t_last if self._done and self._t_last else time.perf_counter()
                snap.stream_seconds = end - self._t_first
        snap.merge_seconds = time.perf_counter() - t0
        return snap

    def GetSystemStats(self, request, context):
        with self._lock:
            stats = pb.SystemStats(records_ingested=self._records,
                                   batches_ingested=self._batches,
                                   ingestion_done=self._done,
                                   distribution=self.distribution)
            if self._t_first is not None:
                end = self._t_last if self._done and self._t_last else time.perf_counter()
                stats.stream_seconds = end - self._t_first
        stats.workers.extend(
            pb.WorkerStats(worker_id=l.worker_id, address=l.address,
                           records=l.records, batches=l.batches,
                           healthy=l.healthy())
            for l in self.links)
        return stats

    def Reset(self, request, context):
        for link in self.links:
            try:
                link.stub.Reset(pb.ResetRequest(), timeout=10.0)
            except grpc.RpcError:
                pass
        with self._lock:
            self._records = self._batches = 0
            self._t_first = self._t_last = None
            self._done = False
            self._rr = 0
        self._log("reset")
        return pb.ResetResponse(ok=True)


def wait_for_workers(addresses, timeout=60.0):
    """Block until every worker answers; return the ones that never did."""
    deadline = time.time() + timeout
    pending = list(addresses)
    while pending and time.time() < deadline:
        still = []
        for a in pending:
            ch = grpc.insecure_channel(a)
            try:
                grpc.channel_ready_future(ch).result(timeout=2.0)
            except grpc.FutureTimeoutError:
                still.append(a)
            finally:
                ch.close()
        pending = still
        if pending:
            time.sleep(0.5)
    return pending


def main():
    ap = argparse.ArgumentParser(description="Streaming analytics coordinator")
    ap.add_argument("address", nargs="?", default="0.0.0.0:50051",
                    help="host:port to bind (default 0.0.0.0:50051)")
    ap.add_argument("--workers", required=True,
                    help="comma-separated worker addresses, e.g. node02:50100,node03:50100")
    ap.add_argument("--distribution", choices=("roundrobin", "hash"),
                    default="roundrobin")
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--queue-depth", type=int, default=8,
                    help="batches buffered per worker before the sender blocks. "
                         "This is the backpressure knob (default 8)")
    ap.add_argument("--max-workers", type=int, default=16,
                    help="gRPC server thread pool size")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    addresses = [a.strip() for a in args.workers.split(",") if a.strip()]
    if not addresses:
        sys.exit("coordinator: --workers must name at least one worker")

    sys.stderr.write("[coordinator] waiting for %d worker(s): %s\n"
                     % (len(addresses), ", ".join(addresses)))
    sys.stderr.flush()
    missing = wait_for_workers(addresses)
    if missing:
        sys.exit("coordinator: these workers never answered: %s" % ", ".join(missing))

    _, _, port = args.address.rpartition(":")
    servicer = CoordinatorServicer(addresses, args.distribution, args.top_k,
                                   args.queue_depth, quiet=args.quiet)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.max_workers),
                         options=CHANNEL_OPTS)
    pb_grpc.add_CoordinatorServiceServicer_to_server(servicer, server)

    bound = server.add_insecure_port("0.0.0.0:%d" % int(port))
    if bound == 0:
        sys.exit("coordinator: could not bind port %s (already in use?)" % port)

    server.start()
    sys.stderr.write("[coordinator] listening on 0.0.0.0:%s | %d worker(s) | "
                     "distribution=%s | queue-depth=%d\n"
                     % (port, len(addresses), args.distribution, args.queue_depth))
    sys.stderr.flush()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1.0).wait()


if __name__ == "__main__":
    main()

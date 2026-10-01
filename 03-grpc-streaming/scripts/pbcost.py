"""Per-operation protobuf costs, to compare the cpp and python backends.

    python pbcost.py <src dir> <dataset>
"""
import sys, time
sys.path.insert(0, sys.argv[1])
from google.protobuf.internal import api_implementation
import analytics_pb2 as pb
from aggregate import parse_line, fold_lines, Aggregate

lines = []
with open(sys.argv[2]) as f:
    f.readline()
    for i, l in enumerate(f):
        if i >= 200000: break
        lines.append(l)
N = len(lines)

def t(fn, reps=1):
    t0 = time.perf_counter()
    for _ in range(reps): fn()
    return (time.perf_counter() - t0) / reps

# parsed wire: producer builds Record messages and a batch, serialises
def parsed():
    for i in range(0, N, 1000):
        buf = []
        for l in lines[i:i+1000]:
            ts, sid, eid, uid, sc, rt, nb = parse_line(l)
            buf.append(pb.Record(timestamp=ts, server_id=sid, endpoint_id=eid, user_id=uid,
                                 status_code=sc, rt_micros=rt, bytes_sent=nb))
        pb.RecordBatch(records=buf, batch_id=i).SerializeToString()
def parse_only():
    for l in lines: parse_line(l)
# raw wire: producer joins and ships
def raw():
    for i in range(0, N, 1000):
        pb.RecordBatch(lines="".join(lines[i:i+1000]).encode(), count=1000, batch_id=i).SerializeToString()
raw_msgs = [pb.RecordBatch(lines="".join(lines[i:i+1000]).encode(), count=1000).SerializeToString()
            for i in range(0, N, 1000)]
def coord_hop():   # coordinator: parse the incoming batch, re-serialise it for a worker
    for m in raw_msgs:
        b = pb.RecordBatch(); b.ParseFromString(m); b.SerializeToString()
def worker_fold():
    a = Aggregate()
    for m in raw_msgs:
        b = pb.RecordBatch(); b.ParseFromString(m); fold_lines(a, b.lines.decode())
    return a
agg = worker_fold()
def snapshot():    # worker GetPartial: build + serialise O(keys) message
    msg = pb.PartialState(total_requests=agg.total)
    msg.servers.extend(pb.ServerStat(server_id=s, count=c, rt_sum=r) for s, (c, r) in agg.servers.items())
    msg.endpoints.extend(pb.EndpointStat(endpoint_id=e, count=c, bytes=b) for e, (c, b) in agg.endpoints.items())
    msg.intervals.extend(pb.IntervalStat(interval_id=i, count=c) for i, c in agg.intervals.items())
    return msg.SerializeToString()
ser = snapshot()
def unsnap():      # coordinator decoding one partial
    p = pb.PartialState(); p.ParseFromString(ser)
    {s.server_id: [s.count, s.rt_sum] for s in p.servers}
    {e.endpoint_id: [e.count, e.bytes] for e in p.endpoints}
    {i.interval_id: i.count for i in p.intervals}

keys = len(agg.servers) + len(agg.endpoints) + len(agg.intervals)
print("backend", api_implementation.Type(), "| records", N, "| snapshot keys", keys)
print("  parse_line only         %6.2f us/record" % (t(parse_only) / N * 1e6))
print("  parsed-wire producer    %6.2f us/record" % (t(parsed) / N * 1e6))
print("  raw-wire producer       %6.3f us/record" % (t(raw) / N * 1e6))
print("  coordinator hop (raw)   %6.1f us/batch of 1000" % (t(coord_hop) / len(raw_msgs) * 1e6))
print("  worker fold (raw)       %6.2f us/record" % (t(worker_fold) / N * 1e6))
print("  worker snapshot         %6.2f ms" % (t(snapshot, 20) * 1e3))
print("  coordinator unsnapshot  %6.2f ms per partial" % (t(unsnap, 20) * 1e3))

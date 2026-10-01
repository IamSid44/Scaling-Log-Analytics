#!/usr/bin/env python3
"""Draw the gRPC message sequence of a demo run from the server's log.

Every line the server logged becomes one row, in order: a request from a client lane to
the server, an update the server pushed to a subscriber, or a rejected request with its
gRPC status code. Lanes are named from the log itself (the customer id on PlaceOrder,
the restaurant name on UpdateOrderStatus) and the node each client ran on.

Usage (needs matplotlib, so run it off the cluster):
    python3 scripts/plot_demo.py results/logs/server_demo.log results/demo_sequence.png
"""
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#d6d5d0"
PUSH = "#2a78d6"       # server -> client updates
REJECT = "#d03b3b"     # rejected requests

LINE = re.compile(r"^(\d\d:\d\d:\d\d)\.\d+ \[Server\] (.*)$")


def parse(path):
    events, names = [], {}
    for raw in open(path, encoding="utf-8", errors="replace"):
        m = LINE.match(raw.rstrip())
        if not m:
            continue
        t, msg = m.groups()
        msg = msg.strip()  # pushes are logged indented under the update that caused them
        if msg.startswith("REJECT "):
            rpc, node, rest = re.match(r"REJECT (\w+) from (\S+): (.*)", msg).groups()
            code = rest.split(":", 1)[0]
            events.append((t, node, "reject", "\u2717 {}: {}".format(rpc, code)))
        elif msg.startswith("pushed "):
            oid, status, node = re.match(r"pushed (\S+) (\S+) to (\S+)", msg).groups()
            events.append((t, node, "push", "push {} {}".format(oid, status)))
        elif " from " in msg:
            rpc, node, rest = re.match(r"(\w+) from (\S+): (.*)", msg).groups()
            if rpc == "PlaceOrder":
                oid, who, total = re.match(r"(\S+) placed by '([^']*)'.*total (\d+)", rest).groups()
                names.setdefault(node, who)
                label = "PlaceOrder \u2192 {} (total {})".format(oid, total)
            elif rpc == "UpdateOrderStatus":
                oid, new, who = re.match(r"(\S+) \S+ -> (\S+) by '([^']*)'", rest).groups()
                names.setdefault(node, who)
                label = "UpdateOrderStatus {} \u2192 {}".format(oid, new)
            elif rpc == "CancelOrder":
                oid = rest.split()[0]
                label = "CancelOrder {} \u2192 CANCELLED".format(oid)
            elif rpc == "GetOrderStatus":
                oid, status = re.match(r"(\S+) is (\S+)", rest).groups()
                label = "GetOrderStatus {} \u2192 {}".format(oid, status)
            elif rpc == "SubscribeToOrderUpdates":
                label = "SubscribeToOrderUpdates {}".format(re.search(r"for (\S+)", rest).group(1))
            elif rpc == "ListOrdersForRestaurant":
                label = "ListOrdersForRestaurant ({} pending)".format(re.search(r"has (\d+)", rest).group(1))
            else:
                label = rpc
            events.append((t, node, "request", label))
        elif "stream for" in msg and "closed" in msg:
            oid, node, why = re.search(r"stream for (\S+) to (\S+) closed \(([^)]*)\)", msg).groups()
            events.append((t, node, "close", "stream {} closed ({})".format(oid, why)))
    return events, names


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: plot_demo.py <server_demo.log> <out.png>")
    events, names = parse(sys.argv[1])
    if not events:
        sys.exit("no events found in " + sys.argv[1])

    clients = []
    for _, node, _, _ in events:
        if node not in clients:
            clients.append(node)
    # restaurant lanes: nodes that acted as a restaurant (pending list or status updates)
    restaurants = [n for n in clients
                   if any(e[1] == n and e[3].lstrip("✗ ").startswith(("UpdateOrderStatus", "ListOrdersForRestaurant"))
                          for e in events)]
    customers = [n for n in clients if n not in restaurants]
    lanes = customers + ["server"] + restaurants
    x = {lane: i for i, lane in enumerate(lanes)}
    xs = x["server"]

    rows = len(events)
    fig, ax = plt.subplots(figsize=(11, 1.6 + rows * 0.34), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_xlim(-1.15, len(lanes) - 0.4)
    ax.set_ylim(rows + 0.2, -1.4)
    ax.axis("off")

    for lane, i in x.items():
        ax.plot([i, i], [-0.6, rows], color=GRID, lw=1, zorder=0)
        if lane == "server":
            head = "server\n{}".format(server_node(sys.argv[1]))
        else:
            head = "{}\n{}".format(names.get(lane, lane), lane)
        ax.text(i, -0.9, head, ha="center", va="bottom", fontsize=10, color=INK, weight="bold")

    for row, (t, node, kind, label) in enumerate(events):
        y = row + 0.35
        ax.text(-1.1, y, t, ha="left", va="center", fontsize=8, color=INK_2, family="monospace")
        xc = x[node]
        if kind in ("request", "reject"):
            start, end = xc, xs
            color = REJECT if kind == "reject" else INK_2
            style = "-"
        else:  # push / close: server -> client
            start, end = xs, xc
            color = PUSH
            style = "--" if kind == "close" else "-"
        ax.annotate("", xy=(end, y), xytext=(start, y),
                    arrowprops=dict(arrowstyle="-|>", color=color, lw=2, linestyle=style,
                                    shrinkA=3, shrinkB=3, mutation_scale=12))
        ax.text((start + end) / 2, y - 0.12, label, ha="center", va="bottom", fontsize=8,
                color=INK, weight="bold" if kind == "reject" else "normal")

    legend = [
        Line2D([0], [0], color=INK_2, lw=2, label="request, client \u2192 server"),
        Line2D([0], [0], color=PUSH, lw=2, label="update pushed, server \u2192 subscriber (not requested)"),
        Line2D([0], [0], color=PUSH, lw=2, ls="--", label="stream closed by the server"),
        Line2D([0], [0], color=REJECT, lw=2, label="\u2717 rejected, with its gRPC status code"),
    ]
    ax.legend(handles=legend, loc="lower center", bbox_to_anchor=(0.5, 1.0 + 0.9 / (rows + 1.6)),
              ncol=2, frameon=False, fontsize=8.5, labelcolor=INK)
    fig.suptitle("Food ordering demo on a Slurm cluster: every message the server logged, in order",
                 x=0.02, ha="left", fontsize=12, color=INK, weight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(sys.argv[2], facecolor=SURFACE)
    print("wrote {} ({} events, lanes: {})".format(sys.argv[2], rows, ", ".join(lanes)))


def server_node(path):
    for raw in open(path, encoding="utf-8", errors="replace"):
        m = re.search(r"should connect to ([^:\s]+)", raw)
        if m:
            return m.group(1)
    return ""


if __name__ == "__main__":
    main()

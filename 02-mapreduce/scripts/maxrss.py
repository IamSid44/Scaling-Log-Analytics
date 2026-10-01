#!/usr/bin/env python3
"""Run a command and report its peak resident memory on stderr.

    python3 scripts/maxrss.py <command...>

Prints one line after the command exits:

    MAXRSS_KB <hostname> <kilobytes>

Used to wrap each MPI rank (`mpirun ... python3 maxrss.py ./loganalytics_mpi ...`)
so every rank reports its own peak RSS. getrusage(RUSAGE_CHILDREN) is exact and
needs nothing installed, unlike /usr/bin/time. The command's exit code is passed
through unchanged.
"""

import resource
import socket
import subprocess
import sys

if len(sys.argv) < 2:
    sys.exit("usage: maxrss.py <command...>")
rc = subprocess.call(sys.argv[1:])
kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss   # KiB on Linux
sys.stderr.write("MAXRSS_KB %s %d\n" % (socket.gethostname(), kb))
sys.exit(rc)

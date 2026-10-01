# Large-scale server log analytics (MPI)
#
#   make            build both binaries into bin/
#   make verify     build + run the correctness suite
#   make bench      build + run the benchmark sweep (prefer the SLURM job)
#   make plot       summary tables + plots from results/benchmark.csv
#   make clean      remove binaries
#   make distclean  also remove generated data/ and results/

CXX      ?= g++
MPICXX   ?= mpicxx
CXXFLAGS ?= -O3 -std=c++17 -Wall -Wextra -D_FILE_OFFSET_BITS=64

.PHONY: all verify bench plot clean distclean

all: bin/loganalytics_seq bin/loganalytics_mpi

bin:
	mkdir -p bin

bin/loganalytics_seq: src/loganalytics_seq.cpp src/loganalytics.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ src/loganalytics_seq.cpp

bin/loganalytics_mpi: src/loganalytics_mpi.cpp src/loganalytics.hpp | bin
	$(MPICXX) $(CXXFLAGS) -o $@ src/loganalytics_mpi.cpp

verify: all ; bash scripts/verify.sh
bench:  all ; bash scripts/benchmark.sh
plot:   ; python3 scripts/plot.py

clean:
	rm -rf bin
distclean: clean
	rm -rf data results

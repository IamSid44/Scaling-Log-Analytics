# MapReduce log analytics: make (build bin/), make verify, make clean, make distclean (also data/ and results/)

CXX      ?= g++
CXXFLAGS ?= -O3 -std=c++17 -Wall -Wextra -D_FILE_OFFSET_BITS=64

BINS = bin/mr_map bin/mr_reduce bin/mr_report bin/loganalytics_seq

.PHONY: all verify clean distclean

all: $(BINS)

bin:
	mkdir -p bin

bin/mr_map: src/mr_map.cpp src/mr_common.hpp src/loganalytics.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ $<
bin/mr_reduce: src/mr_reduce.cpp src/mr_common.hpp src/loganalytics.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ $<
bin/mr_report: src/mr_report.cpp src/mr_common.hpp src/loganalytics.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ $<
bin/loganalytics_seq: src/loganalytics_seq.cpp src/loganalytics.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ $<

verify: all ; bash scripts/verify.sh
clean:
	rm -rf bin
distclean: clean
	rm -rf data results

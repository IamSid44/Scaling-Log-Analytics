// Sequential reference. Correctness oracle for verify.sh and the
// single-core baseline for speed-up.
//
//   ./loganalytics_seq <input_file> [output_file]
//
// The report goes to <output_file> (or stdout); timings go to stderr so they
// never contaminate the required output.

#include <chrono>
#include <cstdio>
#include <string>

#include "loganalytics.hpp"

static double now_seconds() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
}

int main(int argc, char **argv) {
    std::string in_path, out_path;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (in_path.empty())            in_path = a;
        else if (out_path.empty())      out_path = a;
    }

    if (in_path.empty()) {
        std::fprintf(stderr,
                     "usage: %s <input_file> [output_file]\n", argv[0]);
        return 1;
    }

    const loga::Header hdr = loga::read_header(in_path);
    if (!hdr.ok) {
        std::fprintf(stderr, "error: cannot read header of '%s'\n", in_path.c_str());
        return 1;
    }
    const long long fsize = loga::file_size_of(in_path);
    if (fsize < 0) {
        std::fprintf(stderr, "error: cannot stat '%s'\n", in_path.c_str());
        return 1;
    }

    FILE *f = std::fopen(in_path.c_str(), "rb");
    if (!f) {
        std::fprintf(stderr, "error: cannot open '%s'\n", in_path.c_str());
        return 1;
    }

    loga::Aggregate agg;
    // server_id is in [0, S-1], so size the tables from the header. ensure()
    // still guards an out-of-range id in malformed input.
    if (hdr.S > 0) {
        agg.srv_count.assign((size_t)hdr.S, 0);
        agg.srv_rt_sum.assign((size_t)hdr.S, 0);
    }

    double t_read = 0.0;
    const double t0 = now_seconds();
    loga::parse_range(f, hdr.data_offset, fsize, /*skip_partial=*/false, agg, &t_read);
    const double t_solve = now_seconds() - t0;
    std::fclose(f);

    if (agg.overflow) {
        std::fprintf(stderr,
                     "error: response-time sum overflowed 64-bit fixed point\n");
        return 1;
    }

    loga::Report rep;
    rep.total = agg.total; rep.successful = agg.successful; rep.failed = agg.failed;
    rep.rt_sum = agg.rt_sum; rep.rt_min = agg.rt_min; rep.rt_max = agg.rt_max;
    rep.bytes_total = agg.bytes_total;
    for (int i = 0; i < 4; ++i) rep.status[i] = agg.status[i];
    rep.srv_count = agg.srv_count; rep.srv_rt_sum = agg.srv_rt_sum;
    rep.ep_count = agg.ep_count;   rep.ep_bytes = agg.ep_bytes;
    rep.srv_rt_sum.resize(rep.srv_count.size(), 0);
    rep.ep_bytes.resize(rep.ep_count.size(), 0);

    // Busiest interval; ties go to the smaller id.
    for (const auto &kv : agg.interval) {
        if (kv.second > rep.busiest_count ||
            (kv.second == rep.busiest_count && kv.first < rep.busiest_id)) {
            rep.busiest_count = kv.second;
            rep.busiest_id = kv.first;
        }
    }
    if (rep.busiest_count == 0) rep.busiest_id = 0;

    const double t1 = now_seconds();
    FILE *out = (out_path.empty() || out_path == "-")
                ? stdout : std::fopen(out_path.c_str(), "wb");
    if (!out) {
        std::fprintf(stderr, "error: cannot write '%s'\n", out_path.c_str());
        return 1;
    }
    loga::print_report(out, rep, hdr.K);
    if (out != stdout) std::fclose(out);
    const double t_write = now_seconds() - t1;

    if (agg.malformed)
        std::fprintf(stderr, "WARNING malformed_lines %lld\n", agg.malformed);
    // The header says how many records there should be. In the MPI program the
    // same check is what catches a record lost or duplicated at a rank
    // boundary, so both programs make it.
    if (hdr.N > 0 && agg.total + agg.malformed != hdr.N)
        std::fprintf(stderr, "WARNING record_count %lld does not match header N %lld\n",
                     agg.total + agg.malformed, hdr.N);

    std::fprintf(stderr, "IO_MODE serial\n");
    std::fprintf(stderr, "N %lld\n", hdr.N);
    std::fprintf(stderr, "P 1\n");
    std::fprintf(stderr, "NODES 1\n");
    std::fprintf(stderr, "BYTES %lld\n", fsize);
    std::fprintf(stderr, "TIME_SOLVE %.6f\n", t_solve);
    std::fprintf(stderr, "TIME_COMP %.6f\n", t_solve);
    std::fprintf(stderr, "TIME_COMM 0.000000\n");
    std::fprintf(stderr, "TIME_READ %.6f\n", t_read);
    std::fprintf(stderr, "TIME_IMBALANCE 0.000000\n");
    std::fprintf(stderr, "TIME_IO %.6f\n", t_write);
    std::fprintf(stderr, "TIME_TOTAL %.6f\n", t_solve + t_write);
    return 0;
}

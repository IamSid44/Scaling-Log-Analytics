// MPI server log analytics.
//
// Two input-distribution strategies:
//   --io split    (default) every rank reads its own byte range of the file.
//   --io scatter  rank 0 reads the whole file and MPI_Scatterv's the bytes.
//
// Aggregation is the same in both: reductions whose volume depends on the
// number of servers, endpoints and intervals, never on N.
//
// Timing note: the collectives are blocking, so a rank that finishes parsing
// early would otherwise sit inside MPI_Allreduce waiting for the slowest rank
// and charge that wait to communication. A barrier before each timed
// communication region separates the two: TIME_IMBALANCE is the straggler wait,
// TIME_COMM is actual data movement.
//
//   mpirun -np <P> ./loganalytics_mpi <input> [output] [--io split|scatter]

#include <mpi.h>

#include <algorithm>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "loganalytics.hpp"

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank = 0, P = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);

    std::string in_path, out_path, io_mode = "split";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--io" && i + 1 < argc)              io_mode = argv[++i];
        else if (in_path.empty())                     in_path = a;
        else if (out_path.empty())                    out_path = a;
    }

    if (in_path.empty() || (io_mode != "split" && io_mode != "scatter")) {
        if (rank == 0)
            std::fprintf(stderr,
                "usage: mpirun -np <P> %s <input> [output] "
                "[--io split|scatter]\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const loga::Header hdr = loga::read_header(in_path);
    const long long fsize = loga::file_size_of(in_path);
    if (!hdr.ok || fsize < 0) {
        if (rank == 0)
            std::fprintf(stderr, "error: cannot read '%s'\n", in_path.c_str());
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    const double t_start = MPI_Wtime();
    double t_comm = 0.0, t_comp = 0.0, t_read = 0.0, t_imbal = 0.0, mark;

    loga::Aggregate agg;

    const long long data_lo = hdr.data_offset;
    const long long data_len = fsize - data_lo;

    if (io_mode == "split") {
        const long long lo = data_lo + (data_len * rank) / P;
        const long long hi = (rank == P - 1) ? fsize
                                             : data_lo + (data_len * (rank + 1)) / P;

        FILE *f = std::fopen(in_path.c_str(), "rb");
        if (!f) {
            std::fprintf(stderr, "rank %d: cannot open '%s'\n", rank, in_path.c_str());
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Is lo already a line start? If the previous byte is a newline it is,
        // and skipping would drop a whole record.
        bool skip_partial = false;
        if (lo > data_lo) {
            char prev = '\0';
            if (loga::seek_to(f, lo - 1) && std::fread(&prev, 1, 1, f) == 1)
                skip_partial = (prev != '\n');
            else
                skip_partial = true;
        }

        mark = MPI_Wtime();
        loga::parse_range(f, lo, hi, skip_partial, agg, &t_read);
        t_comp += MPI_Wtime() - mark;
        std::fclose(f);

    } else {
        // MPI_Scatterv's counts and displacements are int, so the whole data
        // section has to fit in 2 GB. Silently truncating would corrupt the
        // result, so refuse instead. split has no such ceiling.
        if (data_len > 2147483647LL) {
            if (rank == 0)
                std::fprintf(stderr,
                    "error: --io scatter handles at most 2 GB of log data "
                    "(%lld bytes here); use --io split\n", data_len);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        std::vector<char> whole;
        std::vector<int> counts(P, 0), displs(P, 0);

        if (rank == 0) {
            FILE *f = std::fopen(in_path.c_str(), "rb");
            if (!f) {
                std::fprintf(stderr, "error: cannot open '%s'\n", in_path.c_str());
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            whole.resize((size_t)data_len);
            mark = MPI_Wtime();
            loga::seek_to(f, data_lo);
            size_t got = std::fread(whole.data(), 1, (size_t)data_len, f);
            whole.resize(got);
            t_read += MPI_Wtime() - mark;
            std::fclose(f);

            // Cut at line boundaries so no rank gets a partial record.
            long long prev = 0;
            for (int r = 0; r < P; ++r) {
                long long target = ((long long)whole.size() * (r + 1)) / P;
                long long cut = target;
                if (r == P - 1) {
                    cut = (long long)whole.size();
                } else {
                    while (cut < (long long)whole.size() && whole[(size_t)cut] != '\n')
                        ++cut;
                    if (cut < (long long)whole.size()) ++cut;
                }
                if (cut < prev) cut = prev;
                counts[r] = (int)(cut - prev);
                displs[r] = (int)prev;
                prev = cut;
            }
        }

        // Ranks 1..P-1 idle while rank 0 reads; that wait is imbalance, not
        // transfer cost, so absorb it here before timing the scatter.
        mark = MPI_Wtime();
        MPI_Barrier(MPI_COMM_WORLD);
        t_imbal += MPI_Wtime() - mark;

        mark = MPI_Wtime();
        MPI_Bcast(counts.data(), P, MPI_INT, 0, MPI_COMM_WORLD);
        int mine = counts[rank];
        std::vector<char> chunk((size_t)(mine > 0 ? mine : 0));
        MPI_Scatterv(rank == 0 ? whole.data() : nullptr, counts.data(), displs.data(),
                     MPI_CHAR, chunk.data(), mine, MPI_CHAR, 0, MPI_COMM_WORLD);
        t_comm += MPI_Wtime() - mark;
        if (rank == 0) { std::vector<char>().swap(whole); }

        mark = MPI_Wtime();
        loga::parse_buffer(chunk.data(), chunk.data() + chunk.size(), agg);
        t_comp += MPI_Wtime() - mark;
    }

    // ------------------------------------------------------------ reduction
    long long ov = agg.overflow ? 1 : 0, ov_any = 0;
    // S is authoritative for the server range; endpoints have no header field
    // so that size comes from the data.
    long long local_ids[2] = {std::max((long long)agg.srv_count.size(), hdr.S),
                              (long long)agg.ep_count.size()};
    long long global_ids[2] = {0, 0};
    long long iv_lo = agg.iv_min, iv_hi = agg.iv_max, g_iv_lo = 0, g_iv_hi = 0;

    // Absorb parse-time imbalance so it is not charged to communication.
    mark = MPI_Wtime();
    MPI_Barrier(MPI_COMM_WORLD);
    t_imbal += MPI_Wtime() - mark;

    mark = MPI_Wtime();
    MPI_Allreduce(local_ids, global_ids, 2, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&iv_lo, &g_iv_lo, 1, MPI_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
    MPI_Allreduce(&iv_hi, &g_iv_hi, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&ov, &ov_any, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    t_comm += MPI_Wtime() - mark;

    if (ov_any) {
        if (rank == 0)
            std::fprintf(stderr,
                         "error: response-time sum overflowed 64-bit fixed point\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const size_t n_srv = (size_t)global_ids[0];
    const size_t n_ep  = (size_t)global_ids[1];

    mark = MPI_Wtime();
    agg.srv_count.resize(n_srv, 0);  agg.srv_rt_sum.resize(n_srv, 0);
    agg.ep_count.resize(n_ep, 0);    agg.ep_bytes.resize(n_ep, 0);

    // Dense interval histogram over the globally observed span, so every rank
    // reduces an identically sized buffer.
    long long span = 0;
    bool have_intervals = (g_iv_lo <= g_iv_hi);
    if (have_intervals) span = g_iv_hi - g_iv_lo + 1;
    if (span < 0 || span > loga::MAX_INTERVAL_SPAN) {
        if (rank == 0)
            std::fprintf(stderr,
                "error: timestamps span %lld 60-second intervals, above the %lld "
                "limit -- check the input's time range\n",
                span, loga::MAX_INTERVAL_SPAN);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    std::vector<long long> iv_local((size_t)(have_intervals ? span : 0), 0);
    for (const auto &kv : agg.interval)
        iv_local[(size_t)(kv.first - g_iv_lo)] += kv.second;
    t_comp += MPI_Wtime() - mark;

    std::vector<long long> srv_count, srv_rt, ep_count, ep_bytes, iv_global;
    if (rank == 0) {
        srv_count.assign(n_srv, 0); srv_rt.assign(n_srv, 0);
        ep_count.assign(n_ep, 0);   ep_bytes.assign(n_ep, 0);
        iv_global.assign(iv_local.size(), 0);
    }

    long long scal_in[9] = {agg.total, agg.successful, agg.failed, agg.bytes_total,
                            agg.status[0], agg.status[1], agg.status[2], agg.status[3],
                            agg.rt_sum};
    long long scal_out[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    long long rt_min_out = 0, rt_max_out = 0, malformed_out = 0;

    mark = MPI_Wtime();
    MPI_Reduce(scal_in, scal_out, 9, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    // Empty ranks keep rt_min = LLONG_MAX and rt_max = LLONG_MIN, the identities
    // for MIN and MAX, so they need no special case.
    MPI_Reduce(&agg.rt_min, &rt_min_out, 1, MPI_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&agg.rt_max, &rt_max_out, 1, MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&agg.malformed, &malformed_out, 1, MPI_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    if (n_srv) {
        MPI_Reduce(agg.srv_count.data(), rank == 0 ? srv_count.data() : nullptr,
                   (int)n_srv, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(agg.srv_rt_sum.data(), rank == 0 ? srv_rt.data() : nullptr,
                   (int)n_srv, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    }
    if (n_ep) {
        MPI_Reduce(agg.ep_count.data(), rank == 0 ? ep_count.data() : nullptr,
                   (int)n_ep, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(agg.ep_bytes.data(), rank == 0 ? ep_bytes.data() : nullptr,
                   (int)n_ep, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    }
    if (!iv_local.empty()) {
        MPI_Reduce(iv_local.data(), rank == 0 ? iv_global.data() : nullptr,
                   (int)iv_local.size(), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    }
    t_comm += MPI_Wtime() - mark;

    const double t_solve = MPI_Wtime() - t_start;

    double t_write = 0.0;
    if (rank == 0) {
        loga::Report rep;
        rep.total = scal_out[0]; rep.successful = scal_out[1]; rep.failed = scal_out[2];
        rep.bytes_total = scal_out[3];
        for (int i = 0; i < 4; ++i) rep.status[i] = scal_out[4 + i];
        rep.rt_sum = scal_out[8];
        rep.rt_min = rt_min_out; rep.rt_max = rt_max_out;
        rep.srv_count.swap(srv_count); rep.srv_rt_sum.swap(srv_rt);
        rep.ep_count.swap(ep_count);   rep.ep_bytes.swap(ep_bytes);

        // Scanning upward and replacing only on a strictly greater count gives
        // the required "ties go to the smaller interval id".
        for (size_t i = 0; i < iv_global.size(); ++i) {
            if (iv_global[i] > rep.busiest_count) {
                rep.busiest_count = iv_global[i];
                rep.busiest_id = g_iv_lo + (long long)i;
            }
        }
        if (rep.busiest_count == 0) rep.busiest_id = 0;

        const double w0 = MPI_Wtime();
        FILE *out = (out_path.empty() || out_path == "-")
                    ? stdout : std::fopen(out_path.c_str(), "wb");
        if (!out) {
            std::fprintf(stderr, "error: cannot write '%s'\n", out_path.c_str());
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        loga::print_report(out, rep, hdr.K);
        if (out != stdout) std::fclose(out);
        t_write = MPI_Wtime() - w0;

        if (malformed_out)
            std::fprintf(stderr, "WARNING malformed_lines %lld\n", malformed_out);
        // This is the direct guard on the byte-range split: if a record were
        // lost or double-counted at a rank boundary, the global count would
        // no longer equal the N the header declares.
        if (hdr.N > 0 && rep.total + malformed_out != hdr.N)
            std::fprintf(stderr,
                         "WARNING record_count %lld does not match header N %lld\n",
                         rep.total + malformed_out, hdr.N);
    }

    // Report the slowest rank: that is what the wall clock sees.
    double mx_solve = 0, mx_comm = 0, mx_comp = 0, mx_read = 0, mx_imbal = 0;
    MPI_Reduce(&t_solve, &mx_solve, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&t_comm,  &mx_comm,  1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&t_comp,  &mx_comp,  1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&t_read,  &mx_read,  1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&t_imbal, &mx_imbal, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    // How many distinct machines the ranks actually landed on. Recorded per
    // run and written into every CSV row, so no table has to assume a
    // placement: P=8 is packed 2-per-node when only 6-7 nodes are allocated,
    // and that has to be visible rather than inferred.
    char host[MPI_MAX_PROCESSOR_NAME];
    std::memset(host, 0, sizeof(host));
    int host_len = 0;
    MPI_Get_processor_name(host, &host_len);

    std::vector<char> all_hosts;
    if (rank == 0) all_hosts.resize((size_t)P * MPI_MAX_PROCESSOR_NAME, '\0');
    MPI_Gather(host, MPI_MAX_PROCESSOR_NAME, MPI_CHAR,
               rank == 0 ? all_hosts.data() : nullptr, MPI_MAX_PROCESSOR_NAME,
               MPI_CHAR, 0, MPI_COMM_WORLD);

    int nodes_used = 1;
    if (rank == 0) {
        std::set<std::string> distinct;
        for (int r = 0; r < P; ++r)
            distinct.insert(std::string(&all_hosts[(size_t)r * MPI_MAX_PROCESSOR_NAME]));
        nodes_used = (int)distinct.size();
    }

    if (rank == 0) {
        std::fprintf(stderr, "IO_MODE %s\n", io_mode.c_str());
        std::fprintf(stderr, "N %lld\n", hdr.N);
        std::fprintf(stderr, "P %d\n", P);
        std::fprintf(stderr, "NODES %d\n", nodes_used);
        std::fprintf(stderr, "BYTES %lld\n", fsize);
        std::fprintf(stderr, "TIME_SOLVE %.6f\n", mx_solve);
        std::fprintf(stderr, "TIME_COMP %.6f\n", mx_comp);
        std::fprintf(stderr, "TIME_COMM %.6f\n", mx_comm);
        std::fprintf(stderr, "TIME_READ %.6f\n", mx_read);
        std::fprintf(stderr, "TIME_IMBALANCE %.6f\n", mx_imbal);
        std::fprintf(stderr, "TIME_IO %.6f\n", t_write);
        std::fprintf(stderr, "TIME_TOTAL %.6f\n", mx_solve + t_write);
    }

    MPI_Finalize();
    return 0;
}

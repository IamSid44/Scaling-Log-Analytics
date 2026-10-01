// Job 2 reducer (exactly one task): merges the job-1 totals, picks the busiest interval,
// ranks the top K servers and endpoints, and prints the report with the shared
// print_report. Ranking needs a single reducer: with R reducers each one holds only a
// subset of the keys, and a local top K is not the global top K.
//   sort -m job1/part-* | mr_report > report.txt        (K from $LOG_K or -k)

#include "mr_common.hpp"

#include <cstdlib>

int main(int argc, char **argv) {
    long long K = 10;
    if (const char *e = std::getenv("LOG_K")) {
        const long long k = std::atoll(e);
        if (k > 0) K = k;
    }
    for (int i = 1; i < argc; ++i)
        if (std::strcmp(argv[i], "-k") == 0 && i + 1 < argc) K = std::atoll(argv[++i]);

    loga::Report r;
    long long best_iv = 0, best_count = 0;
    bool have_iv = false;
    bool have_g = false;

    // Keys arrive sorted, so replacing only on a strictly greater count keeps the
    // smallest interval id on ties, as the spec requires.
    long long run_iv = 0, run_count = 0;
    bool have_run = false;
    auto close_run = [&]() {
        if (have_run && (!have_iv || run_count > best_count)) {
            best_iv = run_iv; best_count = run_count; have_iv = true;
        }
        have_run = false;
    };
    long long records = 0, unknown = 0, hosts = 0;

    mr::LineReader in(stdin);
    const char *line;
    size_t n;

    while (in.next(line, n)) {
        if (n == 0) continue;
        const void *t = std::memchr(line, '\t', n);
        if (!t) { ++unknown; continue; }
        const size_t klen = (size_t)((const char *)t - line);
        const char *val = line + klen + 1;
        const size_t vlen = n - klen - 1;

        // H keys hold a hostname, not a number: count them before parse_key.
        if (klen >= 2 && line[0] == 'H' && line[1] == ':') { ++hosts; continue; }

        char type;
        long long id;
        if (!mr::parse_key(line, klen, type, id)) { ++unknown; continue; }
        ++records;

        long long v[mr::G_FIELDS];
        const int got = mr::read_fields(val, vlen, v, mr::G_FIELDS);

        switch (type) {
            case 'G':
                if (got < mr::G_FIELDS) { ++unknown; break; }
                // summed, so it also works as the reducer of a single job (one G per mapper)
                r.total       += v[0];
                r.successful  += v[1];
                r.failed      += v[2];
                r.rt_sum      += v[3];
                if (!have_g || v[4] < r.rt_min) r.rt_min = v[4];
                if (!have_g || v[5] > r.rt_max) r.rt_max = v[5];
                r.bytes_total += v[6];
                r.status[0]   += v[7];
                r.status[1]   += v[8];
                r.status[2]   += v[9];
                r.status[3]   += v[10];
                have_g = true;
                break;

            case 'S':
                if (got < 2 || id < 0) { ++unknown; break; }
                loga::ensure(r.srv_count, (size_t)id);
                loga::ensure(r.srv_rt_sum, (size_t)id);
                r.srv_count[(size_t)id]  += v[0];
                r.srv_rt_sum[(size_t)id] += v[1];
                break;

            case 'E':
                if (got < 2 || id < 0) { ++unknown; break; }
                loga::ensure(r.ep_count, (size_t)id);
                loga::ensure(r.ep_bytes, (size_t)id);
                r.ep_count[(size_t)id] += v[0];
                r.ep_bytes[(size_t)id] += v[1];
                break;

            case 'I':
                if (got < 1) { ++unknown; break; }
                // one record per interval after job 1, one per mapper in a single job
                if (have_run && id == run_iv) { run_count += v[0]; break; }
                close_run();
                run_iv = id; run_count = v[0]; have_run = true;
                break;

            default:
                ++unknown;
                break;
        }
    }

    close_run();

    if (r.total == 0) {
        r.rt_min = 0;
        r.rt_max = 0;
    }
    r.busiest_id = have_iv ? best_iv : 0;
    r.busiest_count = have_iv ? best_count : 0;

    loga::print_report(stdout, r, K);
    std::fflush(stdout);

    if (unknown)
        std::fprintf(stderr, "mr_report: %lld unusable record(s)\n", unknown);

    std::fprintf(stderr, "mr_report: map tasks ran on %lld distinct host(s)\n", hosts);
    mr::counter("loganalytics", "distinct_map_hosts", hosts);
    mr::counter("loganalytics", "report_in_records", records);
    return 0;
}

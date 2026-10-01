// Job 1 mapper: log lines on stdin -> aggregated key/value pairs on stdout.
//
// In-mapper combining: the whole split is folded into one loga::Aggregate and emitted once
// at EOF, so each mapper writes O(servers + endpoints + intervals) lines (~2,000 here)
// however many records it read. Parsing is the shared parse_line, unchanged.

#include "mr_common.hpp"

#ifdef _WIN32  // local testing only
#  include <cstdlib>
   static int mr_hostname(char *buf, size_t n) {
       const char *e = std::getenv("COMPUTERNAME");
       if (!e) e = std::getenv("HOSTNAME");
       if (!e) return -1;
       std::snprintf(buf, n, "%s", e);
       return 0;
   }
#else
#  include <unistd.h>
   static int mr_hostname(char *buf, size_t n) { return gethostname(buf, n); }
#endif

int main(void) {
    loga::Aggregate a;
    mr::LineReader in(stdin);
    mr::Writer out(stdout);

    const char *line;
    size_t n;
    long long lines = 0;

    // The "N K S" header is stripped before staging, so every line is a record.
    while (in.next(line, n)) {
        if (n == 0) continue;               // blank lines are skipped, not counted
        loga::parse_line(line, line + n, a);
        ++lines;
    }

    if (a.overflow) {
        std::fprintf(stderr, "mr_map: response-time sum overflowed int64\n");
        return 1;
    }

    // G: the global scalars
    {
        long long v[mr::G_FIELDS] = {
            a.total, a.successful, a.failed, a.rt_sum,
            // an empty split emits the min/max identities
            a.total ? a.rt_min : loga::LL_MAX,
            a.total ? a.rt_max : loga::LL_MIN,
            a.bytes_total, a.status[0], a.status[1], a.status[2], a.status[3]
        };
        mr::write_key(out, 'G', 0, 1);
        out.put('\t');
        mr::write_fields(out, v, mr::G_FIELDS);
        out.put('\n');
    }

    // H: this task's host and line count. The number of distinct H keys is the number
    // of nodes that ran map tasks; mr_report counts them and skips them otherwise.
    {
        char host[256];
        if (mr_hostname(host, sizeof(host)) != 0) std::strcpy(host, "unknown");
        host[sizeof(host) - 1] = '\0';
        out.put("H:", 2);
        out.put(host, std::strlen(host));
        out.put('\t');
        out.put_ll(1);            // one map task
        out.put(' ');
        out.put_ll(lines);        // lines it read
        out.put('\n');
    }

    long long emitted = 2;

    for (size_t i = 0; i < a.srv_count.size(); ++i) {
        if (a.srv_count[i] == 0) continue;
        mr::write_key(out, 'S', (long long)i, mr::ID_WIDTH);
        out.put('\t');
        out.put_ll(a.srv_count[i]);
        out.put(' ');
        out.put_ll(a.srv_rt_sum[i]);
        out.put('\n');
        ++emitted;
    }

    for (size_t i = 0; i < a.ep_count.size(); ++i) {
        if (a.ep_count[i] == 0) continue;
        mr::write_key(out, 'E', (long long)i, mr::ID_WIDTH);
        out.put('\t');
        out.put_ll(a.ep_count[i]);
        out.put(' ');
        out.put_ll(a.ep_bytes[i]);
        out.put('\n');
        ++emitted;
    }

    for (std::unordered_map<long long, long long>::const_iterator it = a.interval.begin();
         it != a.interval.end(); ++it) {
        mr::write_key(out, 'I', it->first, mr::IV_WIDTH);
        out.put('\t');
        out.put_ll(it->second);
        out.put('\n');
        ++emitted;
    }

    out.flush();

    if (a.malformed)
        std::fprintf(stderr, "mr_map: %lld malformed line(s) skipped\n", a.malformed);

    mr::counter("loganalytics", "input_lines", lines);
    mr::counter("loganalytics", "emitted_pairs", emitted);
    mr::counter("loganalytics", "malformed_lines", a.malformed);
    return 0;
}

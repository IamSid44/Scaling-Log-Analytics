// Shared parsing, aggregation and output for the analytics workload. Both programs include this so
// their output is identical by construction.
//
// Response times are accumulated as scaled integers (microseconds), not doubles:
// integer addition is associative, so the total does not depend on how the input
// was split and the MPI output is byte-identical to the sequential output for
// every P. Assumes sum(response_time) * 10^6 fits in int64; overflow is flagged.

#ifndef LOGANALYTICS_HPP
#define LOGANALYTICS_HPP

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

namespace loga {

static const long long RT_SCALE = 1000000;
// The clarified spec fixes response-time output at 6 decimal places, which is
// also the resolution RT_SCALE carries, so no value is ever rounded twice.
static const int RT_PRECISION = 6;
static const long long LL_MAX = 9223372036854775807LL;
static const long long LL_MIN = -LL_MAX - 1;

// Cap on the dense interval array built for the reduction; past this the
// timestamps are malformed rather than genuinely spanning 19 years.
static const long long MAX_INTERVAL_SPAN = 10000000LL;

struct Header {
    long long N = 0, K = 0, S = 0;
    long long data_offset = 0;
    bool ok = false;
};

// Per-process totals. Size is O(servers + endpoints + intervals), never O(N),
// which is what keeps the reduction cheap.
struct Aggregate {
    long long total = 0, successful = 0, failed = 0;
    long long rt_sum = 0;
    long long rt_min = LL_MAX;
    long long rt_max = LL_MIN;
    long long bytes_total = 0;
    long long status[4] = {0, 0, 0, 0};

    std::vector<long long> srv_count, srv_rt_sum;
    std::vector<long long> ep_count, ep_bytes;

    std::unordered_map<long long, long long> interval;
    long long iv_min = LL_MAX, iv_max = LL_MIN;

    long long malformed = 0;
    bool overflow = false;
};

struct Report {
    long long total = 0, successful = 0, failed = 0;
    long long rt_sum = 0, rt_min = LL_MAX, rt_max = LL_MIN;
    long long bytes_total = 0;
    long long status[4] = {0, 0, 0, 0};
    std::vector<long long> srv_count, srv_rt_sum;
    std::vector<long long> ep_count, ep_bytes;
    long long busiest_id = 0, busiest_count = 0;
};

inline void skip_blanks(const char *&p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r')) ++p;
}

inline bool read_ll(const char *&p, const char *end, long long &out) {
    skip_blanks(p, end);
    if (p >= end) return false;
    bool neg = false;
    if (*p == '-') { neg = true; ++p; }
    else if (*p == '+') { ++p; }
    if (p >= end || *p < '0' || *p > '9') return false;
    long long v = 0;
    while (p < end && *p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
    out = neg ? -v : v;
    return true;
}

// Decimal to integer scaled by RT_SCALE. Digits past 6 places are truncated.
inline bool read_fixed(const char *&p, const char *end, long long &out) {
    skip_blanks(p, end);
    if (p >= end) return false;
    bool neg = false;
    if (*p == '-') { neg = true; ++p; }
    else if (*p == '+') { ++p; }
    if (p >= end || ((*p < '0' || *p > '9') && *p != '.')) return false;

    long long whole = 0;
    while (p < end && *p >= '0' && *p <= '9') { whole = whole * 10 + (*p - '0'); ++p; }

    long long frac = 0, scale = RT_SCALE;
    if (p < end && *p == '.') {
        ++p;
        while (p < end && *p >= '0' && *p <= '9') {
            if (scale > 1) { scale /= 10; frac += (*p - '0') * scale; }
            ++p;
        }
    }
    long long v = whole * RT_SCALE + frac;
    out = neg ? -v : v;
    return true;
}

inline void ensure(std::vector<long long> &v, size_t idx) {
    if (idx >= v.size()) v.resize(idx + 1, 0);
}

// timestamp server_id endpoint_id user_id status_code response_time bytes_sent
inline void parse_line(const char *p, const char *end, Aggregate &a) {
    long long ts, sid, eid, uid, sc, rt, bytes;
    if (!read_ll(p, end, ts) || !read_ll(p, end, sid) || !read_ll(p, end, eid) ||
        !read_ll(p, end, uid) || !read_ll(p, end, sc) || !read_fixed(p, end, rt) ||
        !read_ll(p, end, bytes)) {
        ++a.malformed;
        return;
    }
    (void)uid;

    ++a.total;
    if (sc < 400) ++a.successful; else ++a.failed;

    if (sc >= 200 && sc < 300)      ++a.status[0];
    else if (sc >= 300 && sc < 400) ++a.status[1];
    else if (sc >= 400 && sc < 500) ++a.status[2];
    else if (sc >= 500 && sc < 600) ++a.status[3];
    // 1xx is successful but belongs to no bucket, per the spec

    if (a.rt_sum > LL_MAX - rt) a.overflow = true;
    a.rt_sum += rt;
    if (rt < a.rt_min) a.rt_min = rt;
    if (rt > a.rt_max) a.rt_max = rt;
    a.bytes_total += bytes;

    if (sid >= 0) {
        ensure(a.srv_count, (size_t)sid);
        ensure(a.srv_rt_sum, (size_t)sid);
        ++a.srv_count[(size_t)sid];
        a.srv_rt_sum[(size_t)sid] += rt;
    }
    if (eid >= 0) {
        ensure(a.ep_count, (size_t)eid);
        ensure(a.ep_bytes, (size_t)eid);
        ++a.ep_count[(size_t)eid];
        a.ep_bytes[(size_t)eid] += bytes;
    }

    const long long iv = ts / 60;
    ++a.interval[iv];
    if (iv < a.iv_min) a.iv_min = iv;
    if (iv > a.iv_max) a.iv_max = iv;
}

inline Header read_header(const std::string &path) {
    Header h;
    FILE *f = std::fopen(path.c_str(), "rb");
    if (!f) return h;
    char buf[256];
    size_t got = std::fread(buf, 1, sizeof(buf) - 1, f);
    std::fclose(f);
    buf[got] = '\0';

    const char *p = buf, *end = buf + got;
    if (!read_ll(p, end, h.N) || !read_ll(p, end, h.K) || !read_ll(p, end, h.S))
        return h;
    const void *nl = std::memchr(buf, '\n', got);
    h.data_offset = nl ? (long long)((const char *)nl - buf) + 1 : (long long)got;
    h.ok = true;
    return h;
}

inline long long file_size_of(const std::string &path) {
    FILE *f = std::fopen(path.c_str(), "rb");
    if (!f) return -1;
    fseeko(f, 0, SEEK_END);
    long long sz = (long long)ftello(f);
    std::fclose(f);
    return sz;
}

inline bool seek_to(FILE *f, long long off) {
    return fseeko(f, (off_t)off, SEEK_SET) == 0;
}

// Parses every line whose START offset lies in [lo, hi). A line straddling hi
// belongs to this rank because it started here, so parsing runs past hi to
// finish it; the next rank drops its leading partial line via skip_partial.
// The caller sets that flag by testing whether the byte before lo is a newline
// -- getting it wrong loses or duplicates one line per rank boundary.
//
// Reads in chunks, so memory is bounded by CHUNK, not by the range size.
// io_seconds, when given, accumulates time spent inside fread.
inline void parse_range(FILE *f, long long lo, long long hi,
                        bool skip_partial, Aggregate &a,
                        double *io_seconds = nullptr) {
    if (lo >= hi) return;
    const size_t CHUNK = 8u << 20;
    std::vector<char> buf(CHUNK);
    std::string carry;

    if (!seek_to(f, lo)) return;

    long long abs = lo;
    long long line_start = lo;
    bool skipping = skip_partial;

    while (true) {
        double t0 = 0.0;
        if (io_seconds) t0 = (double)std::chrono::duration<double>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        size_t got = std::fread(buf.data(), 1, CHUNK, f);
        if (io_seconds) *io_seconds += (double)std::chrono::duration<double>(
            std::chrono::steady_clock::now().time_since_epoch()).count() - t0;
        if (got == 0) break;

        const char *base = buf.data();
        size_t i = 0;
        while (i < got) {
            const void *nlp = std::memchr(base + i, '\n', got - i);
            const size_t j = nlp ? (size_t)((const char *)nlp - base) : got;

            if (!nlp) {                    // line continues into the next chunk
                carry.append(base + i, j - i);
                abs += (long long)(j - i);
                i = j;
                break;
            }

            if (skipping) {                // drop the boundary line
                skipping = false;
                carry.clear();
                abs += (long long)(j - i) + 1;
                line_start = abs;
                i = j + 1;
                continue;
            }

            if (line_start >= hi) return;  // belongs to the next rank

            if (carry.empty()) {
                if (j > i) parse_line(base + i, base + j, a);   // j == i: blank line
            } else {
                carry.append(base + i, j - i);
                parse_line(carry.data(), carry.data() + carry.size(), a);
                carry.clear();
            }
            abs += (long long)(j - i) + 1;
            line_start = abs;
            i = j + 1;
        }
    }

    // Last line with no trailing newline.
    if (!skipping && !carry.empty() && line_start < hi)
        parse_line(carry.data(), carry.data() + carry.size(), a);
}

// Parses a buffer of whole lines (scatter mode).
inline void parse_buffer(const char *p, const char *end, Aggregate &a) {
    while (p < end) {
        const void *nlp = std::memchr(p, '\n', (size_t)(end - p));
        const char *nl = nlp ? (const char *)nlp : end;
        if (nl > p) parse_line(p, nl, a);
        p = (nl < end) ? nl + 1 : end;
    }
}

// Top-K: decreasing count, then increasing id.
inline void top_k(const std::vector<long long> &count, long long K,
                  std::vector<std::pair<long long, long long>> &out) {
    out.clear();
    for (size_t i = 0; i < count.size(); ++i)
        if (count[i] > 0) out.emplace_back((long long)i, count[i]);

    std::sort(out.begin(), out.end(),
              [](const std::pair<long long, long long> &x,
                 const std::pair<long long, long long> &y) {
                  if (x.second != y.second) return x.second > y.second;
                  return x.first < y.first;
              });
    if (K >= 0 && (long long)out.size() > K) out.resize((size_t)K);
}

inline double rt_to_double(long long scaled) {
    return (double)scaled / (double)RT_SCALE;
}

inline void print_report(FILE *f, const Report &r, long long K) {
    const int precision = RT_PRECISION;
    const double avg = r.total > 0
        ? (double)r.rt_sum / (double)RT_SCALE / (double)r.total : 0.0;
    const double mn = r.total > 0 ? rt_to_double(r.rt_min) : 0.0;
    const double mx = r.total > 0 ? rt_to_double(r.rt_max) : 0.0;

    std::fprintf(f, "TOTAL_REQUESTS %lld\n", r.total);
    std::fprintf(f, "SUCCESSFUL_REQUESTS %lld\n", r.successful);
    std::fprintf(f, "FAILED_REQUESTS %lld\n", r.failed);
    std::fprintf(f, "AVERAGE_RESPONSE_TIME %.*f\n", precision, avg);
    std::fprintf(f, "MIN_RESPONSE_TIME %.*f\n", precision, mn);
    std::fprintf(f, "MAX_RESPONSE_TIME %.*f\n", precision, mx);
    std::fprintf(f, "TOTAL_BYTES %lld\n", r.bytes_total);
    std::fprintf(f, "STATUS_2XX %lld\n", r.status[0]);
    std::fprintf(f, "STATUS_3XX %lld\n", r.status[1]);
    std::fprintf(f, "STATUS_4XX %lld\n", r.status[2]);
    std::fprintf(f, "STATUS_5XX %lld\n", r.status[3]);
    std::fprintf(f, "BUSIEST_INTERVAL %lld %lld\n", r.busiest_id, r.busiest_count);

    std::vector<std::pair<long long, long long>> tk;

    std::fprintf(f, "TOP_SERVERS\n");
    top_k(r.srv_count, K, tk);
    for (const auto &e : tk) {
        const long long id = e.first, c = e.second;
        const double a = c > 0
            ? (double)r.srv_rt_sum[(size_t)id] / (double)RT_SCALE / (double)c : 0.0;
        std::fprintf(f, "%lld %lld %.*f\n", id, c, precision, a);
    }

    std::fprintf(f, "TOP_ENDPOINTS\n");
    top_k(r.ep_count, K, tk);
    for (const auto &e : tk) {
        const long long id = e.first, c = e.second;
        std::fprintf(f, "%lld %lld %lld\n", id, c, r.ep_bytes[(size_t)id]);
    }
}

}  // namespace loga

#endif

// Job 1 reducer, also used as the combiner: folds all values of one key.
//   G: every field summed, except rt_min (min) and rt_max (max).   S, E, I, H: summed.
// Sum, min and max over int64 are associative and commutative, and the output has the
// same format as the input, so Hadoop may apply it as a combiner any number of times.
//   ... | sort | mr_reduce > partial

#include "mr_common.hpp"

namespace {

struct Acc {
    char type = 0;
    long long v[mr::G_FIELDS] = {0};
    int n = 0;                   // number of fields in use
    bool open = false;
};

void emit(mr::Writer &out, const std::string &key, const Acc &a) {
    if (!a.open) return;
    out.put(key);
    out.put('\t');
    mr::write_fields(out, a.v, a.n);
    out.put('\n');
}

void start(Acc &a, char type) {
    a = Acc();
    a.type = type;
    a.open = true;
    switch (type) {
        case 'G':
            a.n = mr::G_FIELDS;
            a.v[4] = loga::LL_MAX;         // rt_min
            a.v[5] = loga::LL_MIN;         // rt_max
            break;
        case 'S': a.n = 2; break;        // count, rt_sum
        case 'E': a.n = 2; break;        // count, bytes
        case 'I': a.n = 1; break;        // count
        case 'H': a.n = 2; break;        // map tasks, lines
        default:  a.n = 0; break;
    }
}

void fold(Acc &a, const long long *v, int got) {
    if (a.type == 'G') {
        for (int i = 0; i < got && i < mr::G_FIELDS; ++i) {
            if (i == 4) { if (v[i] < a.v[4]) a.v[4] = v[i]; }
            else if (i == 5) { if (v[i] > a.v[5]) a.v[5] = v[i]; }
            else a.v[i] += v[i];
        }
    } else {
        for (int i = 0; i < got && i < a.n; ++i) a.v[i] += v[i];
    }
}

}  // namespace

int main(void) {
    mr::LineReader in(stdin);
    mr::Writer out(stdout);

    const char *line;
    size_t n;
    std::string key, cur;
    Acc acc;
    long long groups = 0, records = 0, unknown = 0;

    while (in.next(line, n)) {
        if (n == 0) continue;
        const void *t = std::memchr(line, '\t', n);
        if (!t) { ++unknown; continue; }
        const size_t klen = (size_t)((const char *)t - line);
        const char *val = line + klen + 1;
        const size_t vlen = n - klen - 1;
        ++records;

        key.assign(line, klen);
        if (!acc.open || key != cur) {
            if (acc.open) { emit(out, cur, acc); ++groups; }
            cur = key;
            start(acc, klen > 0 ? line[0] : '?');
        }
        if (acc.n == 0) { ++unknown; continue; }

        long long v[mr::G_FIELDS];
        const int got = mr::read_fields(val, vlen, v, acc.n);
        if (got > 0) fold(acc, v, got);
    }
    if (acc.open) { emit(out, cur, acc); ++groups; }
    out.flush();

    if (unknown)
        std::fprintf(stderr, "mr_reduce: %lld record(s) with an unrecognised key\n",
                     unknown);
    mr::counter("loganalytics", "reduce_in_records", records);
    mr::counter("loganalytics", "reduce_out_groups", groups);
    return 0;
}

// Key encoding and buffered I/O shared by the Hadoop Streaming programs.
//
// Keys:   G:0 (global scalars)   S:<server>   E:<endpoint>   I:<60-second interval>
// Ids are zero-padded (10 digits, 15 for intervals) because the shuffle sorts keys as
// bytes: unpadded, "10" sorts before "9" and one key's records would not be adjacent.
// Values are space-separated int64s; response times are in microseconds.

#ifndef MR_COMMON_HPP
#define MR_COMMON_HPP

#include "loganalytics.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mr {

static const int ID_WIDTH = 10;
static const int IV_WIDTH = 15;

// Number of int64 fields in a G record:
// total successful failed rt_sum rt_min rt_max bytes s2 s3 s4 s5
static const int G_FIELDS = 11;

// Buffered line reader; std::getline is too slow for tens of millions of lines.

class LineReader {
public:
    explicit LineReader(std::FILE *f, size_t cap = (8u << 20))
        : f_(f), buf_(cap), len_(0), pos_(0), eof_(false) {}

    bool next(const char *&line, size_t &n) {
        cur_.clear();
        for (;;) {
            if (pos_ >= len_) {
                if (eof_) break;
                len_ = std::fread(buf_.data(), 1, buf_.size(), f_);
                pos_ = 0;
                if (len_ == 0) { eof_ = true; break; }
            }
            const char *base = buf_.data();
            const void *nl = std::memchr(base + pos_, '\n', len_ - pos_);
            if (nl) {
                const size_t j = (size_t)((const char *)nl - base);
                if (cur_.empty()) {
                    line = base + pos_;
                    n = j - pos_;
                    pos_ = j + 1;
                    return true;
                }
                cur_.append(base + pos_, j - pos_);
                pos_ = j + 1;
                line = cur_.data();
                n = cur_.size();
                return true;
            }
            cur_.append(base + pos_, len_ - pos_);
            pos_ = len_;
        }
        if (!cur_.empty()) { line = cur_.data(); n = cur_.size(); cur_.clear(); return true; }
        return false;
    }

private:
    std::FILE *f_;
    std::vector<char> buf_;
    std::string cur_;
    size_t len_, pos_;
    bool eof_;
};

class Writer {
public:
    explicit Writer(std::FILE *f, size_t cap = (4u << 20)) : f_(f), cap_(cap) {
        buf_.reserve(cap);
    }
    ~Writer() { flush(); }

    void put(const char *p, size_t n) {
        if (buf_.size() + n > cap_) flush();
        if (n > cap_) { std::fwrite(p, 1, n, f_); return; }
        buf_.append(p, n);
    }
    void put(char c) { if (buf_.size() + 1 > cap_) flush(); buf_.push_back(c); }
    void put(const char *s) { put(s, std::strlen(s)); }
    void put(const std::string &s) { put(s.data(), s.size()); }
    void put_ll(long long v) {
        char t[24];
        put(t, (size_t)std::snprintf(t, sizeof(t), "%lld", v));
    }
    void flush() {
        if (!buf_.empty()) { std::fwrite(buf_.data(), 1, buf_.size(), f_); buf_.clear(); }
    }

private:
    std::FILE *f_;
    std::string buf_;
    size_t cap_;
};

inline void write_key(Writer &w, char type, long long id, int width) {
    char t[32];
    const int n = std::snprintf(t, sizeof(t), "%c:%0*lld", type, width, id);
    w.put(t, (size_t)n);
}

// Splits "T:<id>" into its type letter and numeric id.
inline bool parse_key(const char *p, size_t n, char &type, long long &id) {
    if (n < 3 || p[1] != ':') return false;
    type = p[0];
    const char *q = p + 2, *end = p + n;
    return loga::read_ll(q, end, id);
}

// Reads up to `max` space-separated int64s; returns how many were found.
inline int read_fields(const char *p, size_t n, long long *out, int max) {
    const char *end = p + n;
    int i = 0;
    while (i < max && loga::read_ll(p, end, out[i])) ++i;
    return i;
}

inline void write_fields(Writer &w, const long long *v, int n) {
    for (int i = 0; i < n; ++i) {
        if (i) w.put(' ');
        w.put_ll(v[i]);
    }
}

// Hadoop Streaming turns these stderr lines into job counters; elsewhere they are log lines.

inline void counter(const char *group, const char *name, long long n) {
    std::fprintf(stderr, "reporter:counter:%s,%s,%lld\n", group, name, n);
}

inline void status(const char *msg) {
    std::fprintf(stderr, "reporter:status:%s\n", msg);
}

}  // namespace mr

#endif

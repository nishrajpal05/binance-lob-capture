#include "fixed_point.h"

namespace fp {

static constexpr int64_t SCALE = 100'000'000LL;
static constexpr int SCALE_DIGITS = 8;

int64_t parse_decimal(const char* str, size_t len) {
    if (len == 0) return 0;

    bool negative = false;
    size_t pos = 0;

    if (str[pos] == '-') {
        negative = true;
        ++pos;
    }

    int64_t integer_part = 0;
    while (pos < len && str[pos] != '.') {
        integer_part = integer_part * 10 + (str[pos] - '0');
        ++pos;
    }

    int64_t frac_part = 0;
    int frac_count = 0;

    if (pos < len && str[pos] == '.') {
        ++pos;
        while (pos < len && frac_count < SCALE_DIGITS) {
            frac_part = frac_part * 10 + (str[pos] - '0');
            ++frac_count;
            ++pos;
        }
    }

    while (frac_count < SCALE_DIGITS) {
        frac_part *= 10;
        ++frac_count;
    }

    int64_t result = integer_part * SCALE + frac_part;
    return negative ? -result : result;
}

size_t int64_to_str(int64_t value, char* buf) {
    if (value == 0) {
        buf[0] = '0';
        return 1;
    }

    char tmp[24];
    size_t pos = 0;
    bool negative = false;

    uint64_t abs_val;
    if (value < 0) {
        negative = true;
        abs_val = static_cast<uint64_t>(-(value + 1)) + 1;
    } else {
        abs_val = static_cast<uint64_t>(value);
    }

    while (abs_val > 0) {
        tmp[pos++] = '0' + static_cast<char>(abs_val % 10);
        abs_val /= 10;
    }

    size_t written = 0;
    if (negative) {
        buf[written++] = '-';
    }

    for (size_t i = pos; i > 0; --i) {
        buf[written++] = tmp[i - 1];
    }

    return written;
}

}

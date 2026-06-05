#pragma once

#include <cstdint>
#include <cstddef>

namespace fp {

int64_t parse_decimal(const char* str, size_t len);
size_t  int64_to_str(int64_t value, char* buf);

}

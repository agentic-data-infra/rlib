#pragma once

#include <cstdio>

// RLIB_LOG("format", args...) prints a line to stderr when rlib is built
// with -DRLIB_ENABLE_LOG, and compiles to nothing otherwise (but still
// type checks its arguments).
#ifdef RLIB_ENABLE_LOG
#define RLIB_LOG(format, ...) \
  std::fprintf(stderr, "rlib: " format "\n" __VA_OPT__(, ) __VA_ARGS__)
#else
#define RLIB_LOG(format, ...)                                       \
  do {                                                              \
    if (false) {                                                    \
      std::fprintf(stderr, format "\n" __VA_OPT__(, ) __VA_ARGS__); \
    }                                                               \
  } while (false)
#endif

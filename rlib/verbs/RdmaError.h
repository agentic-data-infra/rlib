#pragma once

#include <cerrno>
#include <string>
#include <system_error>

namespace rlib {

// What rlib's control path (connection setup, resource allocation) throws
// when a libibverbs or librdmacm call fails: the call's errno and a message
// naming the call. code().value() is the errno.
class RdmaError : public std::system_error {
 public:
  RdmaError(int error, const std::string& what)
      : std::system_error{error, std::generic_category(), what} {}
};

// Throws RdmaError for the failed call 'what', with the error 'error'
// (librdmacm calls and those ibv_* calls that return a pointer set errno;
// other ibv_* calls return the error instead).
[[noreturn]] void throwRdmaError(const char* what, int error = errno);

} // namespace rlib

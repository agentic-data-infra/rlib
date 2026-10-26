#include "rlib/verbs/RdmaError.h"

namespace rlib {

void throwRdmaError(const char* what, int error) {
  throw RdmaError{error, what};
}

} // namespace rlib

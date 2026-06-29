#ifndef DEBUG_H
#define DEBUG_H

#include <errno.h>
#include <string.h>

#define DEBUG_Fmt "%s:%d %s() [%s] %s"
#define DEBUG_Arg __FILE__, __LINE__, __FUNCTION__, strerrorname_np(errno), strerror(errno)

#endif /* end of include guard: DEBUG_H */

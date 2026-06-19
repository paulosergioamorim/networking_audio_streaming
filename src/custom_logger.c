#include "custom_logger.h"

#define ERRNONAME_SAFE_TO_USE_ARRAY
#include "errnoname.h"

void custom_logger(Log_Level level, const char *fmt, va_list args) {
    if (level < nob_minimal_log_level)
        return;

    int error = errno;

    switch (level) {
    case NOB_INFO:
        fprintf(stderr, "[INFO] ");
        break;
    case NOB_WARNING:
        fprintf(stderr, "[WARNING] ");
        break;
    case NOB_ERROR:
        fprintf(stderr, "[ERROR] ");
        break;
    case NOB_NO_LOGS:
        return;
    default:
        NOB_UNREACHABLE("Nob_Log_Level");
    }

    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timer_buffer[100];
    strftime(timer_buffer, sizeof(timer_buffer), "%d-%m-%Y %H:%M:%S ", t);
    fputs(timer_buffer, stderr);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");

    if (level == NOB_ERROR && error != 0) {
        fprintf(stderr, "\t[%s] %s\n", errnoname(error), strerror(error));
    }
}

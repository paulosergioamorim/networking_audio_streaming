#include "signals.h"
#include "debug.h"
#include "nob.h"
#include <signal.h>
#include <stddef.h>

volatile sig_atomic_t signaled;

void sigint_handler(int signal) {
    NOB_UNUSED(signal);
    signaled = 1;
}

int signals_sigint_sigaction() {
    struct sigaction sa = {0};
    sa.sa_flags = 0;
    sa.sa_handler = &sigint_handler;

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }

    return 1;
}

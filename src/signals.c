#include "signals.h"
#include "nob.h"
#include "utils.h"
#include <signal.h>
#include <stddef.h>

volatile sig_atomic_t signaled;

void SIGINT_HANDLER(int signal) {
    NOB_UNUSED(signal);
    signaled = 1;
}

int signals_sigint_sigaction() {
    struct sigaction sa = {0};
    sa.sa_flags = 0;
    sa.sa_handler = &SIGINT_HANDLER;

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }

    return 1;
}

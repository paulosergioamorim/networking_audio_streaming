#include "signals.h"
#include "logger.h"
#include <signal.h>

volatile sig_atomic_t signaled;

void SIGINT_HANDLER(int signal) {
    signaled = 1;
}

int signals_sigint_sigaction() {
    struct sigaction sa = {0};
    sa.sa_flags = 0;
    sa.sa_handler = &SIGINT_HANDLER;

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        LOG_ERROR("sigaction");
        return -1;
    }

    return 0;
}

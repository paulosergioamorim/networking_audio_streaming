#include "signals.h"
#include <signal.h>
#include <stdio.h>

volatile sig_atomic_t signaled;

void SIGINT_HANDLER(int signal) {
    signaled = 1;
}

int signals_sigint_sigaction() {
    struct sigaction sa = {0};
    sa.sa_flags = 0;
    sa.sa_handler = &SIGINT_HANDLER;

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("sigaction");
        return -1;
    }

    return 0;
}

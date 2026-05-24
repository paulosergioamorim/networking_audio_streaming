#ifndef SIGNALS_H
#define SIGNALS_H

#include <signal.h>

extern volatile sig_atomic_t signaled;

void SIGINT_HANDLER(int signal);

int signals_sigint_sigaction();

#endif

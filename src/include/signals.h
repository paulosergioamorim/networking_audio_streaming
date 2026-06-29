#ifndef SIGNALS_H
#define SIGNALS_H

#include <signal.h>

extern volatile sig_atomic_t signaled;

void sigint_handler(int signal);

int signals_sigint_sigaction();

#endif /* end of include guard: SIGNALS_H */

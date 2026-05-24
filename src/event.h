#ifndef EVENT_H
#define EVENT_H

#include <sys/time.h>

typedef enum { EVENT_COMMAND, EVENT_STREAM, EVENT_MESSAGE } Event_Kind;

typedef struct {
    Event_Kind kind;
    char buf[512];
    struct timeval tv;
} Event;

#endif

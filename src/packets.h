#ifndef PACKETS_H
#define PACKETS_H

#include <stddef.h>
#include <sys/time.h>

#define MESSAGE_SIZE 4096

typedef enum {
    _,
    REQ_LIST,
    REQ_START,
    REQ_STOP,
    REQ_RESUME,
    RES_LIST_CONTINUE,
    RES_LIST_END,
    RES_START_OK,
    RES_START_NO_FILE,
    RES_STOP,
    RES_RESUME,
    RES_STREAM
} Message_Kind;

typedef struct {
    Message_Kind kind;
    struct timeval tv;
    size_t len;
    char buf[MESSAGE_SIZE];
} Message;

#endif

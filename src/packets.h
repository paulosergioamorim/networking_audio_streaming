#ifndef EVENT_H
#define EVENT_H

#include <sys/time.h>

#define MESSAGE_SIZE 512
#define AUDIO_STREAM_SIZE 256

typedef enum { MESSAGE_DISPLAY, MESSAGE_INTERNAL } Message_Kind;

typedef struct {
    Message_Kind kind;
    char buf[MESSAGE_SIZE];
} Message;

typedef struct {
    long seq;
    struct timeval tv;
    unsigned char buf[AUDIO_STREAM_SIZE];
} Audio_Stream;

#endif

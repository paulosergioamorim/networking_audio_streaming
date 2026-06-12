#ifndef PACKETS_H
#define PACKETS_H

#include <linux/limits.h>
#include <stddef.h>
#include <sys/time.h>

typedef enum {
    KIND_NONE,
    KIND_LIST,
    KIND_START,
    KIND_STOP,
    KIND_RESUME,
    KIND_STREAM,
    // this commands is client only
    KIND_HELP,
    KIND_EXIT,
    KIND_STATS,
    KIND_RESET
} Message_Kind;

typedef enum { STATUS_NONE, STATUS_OK, STATUS_LIST_CONTINUE, STATUS_LIST_END, STATUS_ERR_NO_FILE } Status_Code;

typedef struct {
    Message_Kind kind;
} Request_Header;

typedef struct {
    Request_Header header;
    // only KIND_START messages use this. buf is the audio index + 1. Because it's small, all messages send it
    size_t buf;
} Request;

typedef struct {
    Message_Kind kind;
    Status_Code code;
    struct timeval tv;
    size_t len; // the lenght of 'buf'
} Response_Header;

#define RESPONSE_MAX 4096

typedef struct {
    Response_Header header;
    char buf[RESPONSE_MAX]; // only KIND_LIST and KIND_STREAM use this
} Response;

#endif

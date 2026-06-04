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
    KIND_EXIT,
    KIND_RESUME,
    KIND_STREAM,
} Message_Kind;

typedef enum { STATUS_NONE, STATUS_OK, STATUS_LIST_CONTINUE, STATUS_LIST_END, STATUS_ERR_NO_FILE } Status_Code;

typedef struct {
    Message_Kind kind;
    size_t len; // the lenght of 'buf'
} Request_Header;

typedef struct {
    Request_Header header;
    char buf[NAME_MAX]; // only KIND_START messages use this
} Request;

typedef struct {
    Message_Kind kind;
    Status_Code code;
    struct timeval tv;
    size_t len; // the lenght of 'buf'
} Response_Header;

#define RESPONSE_MAX (1 << 16) // 64KB

typedef struct {
    Response_Header header;
    char buf[RESPONSE_MAX]; // only KIND_LIST and KIND_STREAM use this
} Response;

#endif

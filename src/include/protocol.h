#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <arpa/inet.h>
#include <linux/limits.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>

typedef enum {
    KIND_NONE,
    KIND_LIST,
    KIND_START,
    KIND_STOP,
    KIND_RESUME,
    KIND_STREAM,
    // these commands are client only
    KIND_HELP,
    KIND_EXIT,
    KIND_STATS,
    KIND_RESET,
} Message_Kind;

typedef enum {
    STATUS_NONE,
    STATUS_OK,
    STATUS_LIST_CONTINUE,
    STATUS_LIST_END,
    STATUS_ERR_NO_FILE,
} Status_Code;

typedef struct {
    int8_t kind;
} Request_Header;

typedef struct {
    Request_Header header;
    // only KIND_START messages use this. buf is the audio index + 1. Because it's small, all messages send it
    int32_t buf;
} Request;

typedef struct {
    int8_t kind;
    int8_t code;
    struct timeval tv;
    uint32_t len;
} Response_Header;

#define RESPONSE_MAX 1600

typedef struct {
    Response_Header header;
    uint8_t data[RESPONSE_MAX]; // only KIND_LIST and KIND_STREAM use this
} Response;

static inline Response_Header response_header_build(Message_Kind kind, Status_Code code, uint32_t len) {
    struct timeval tv = {0};
    gettimeofday(&tv, NULL);
    return (Response_Header){
        .kind = kind,
        .code = code,
        .tv = tv,
        .len = htonl(len),
    };
}

#endif /* end of include guard: PROTOCOL_H */

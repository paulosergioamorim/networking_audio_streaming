/// @file queue.c
/// @author paulosergioamorim

#include "queue.h"
#include "nob.h"
#include "utils.h"
#include <pthread.h>
#include <stdlib.h>
#include <sys/uio.h>

int queue_init(Queue *q, size_t capacity) {
    *q = (Queue){0};

    if (capacity == 0) {
        nob_log(ERROR, "capacity equals to 0");
        goto err_capacity;
    }

    if (pthread_mutex_init(&q->mu, NULL) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err_mutext_init;
    }

    if (pthread_cond_init(&q->cond_empty, NULL) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err_cond_empty_init;
    }

    if (pthread_cond_init(&q->cond_full, NULL) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err_cond_full_init;
    }

    q->capacity = capacity;
    q->items = malloc(capacity * sizeof(*q->items));

    if (!q->items) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err_malloc;
    }

    q->is_active = 1;
    return 1;

err_malloc:
    pthread_cond_destroy(&q->cond_empty);
err_cond_full_init:
    pthread_mutex_destroy(&q->mu);
err_cond_empty_init:
    pthread_cond_destroy(&q->cond_full);
err_capacity:
err_mutext_init:
    return 0;
}

void queue_enqueue(Queue *q, unsigned char *src, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count + len > q->capacity && q->is_active)
        pthread_cond_wait(&q->cond_full, &q->mu);

    if (q->is_active) {
        for (size_t i = 0; i < len; i++) {
            q->items[q->head] = src[i];
            q->head = (q->head + 1) % q->capacity;
            q->count++;
        }
    }

    pthread_cond_broadcast(&q->cond_empty);
    pthread_mutex_unlock(&q->mu);
}

size_t queue_dequeue(Queue *q, unsigned char *dest, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count == 0 && q->is_active)
        pthread_cond_wait(&q->cond_empty, &q->mu);

    size_t i = 0;
    if (q->is_active) {
        for (i = 0; i < len && q->count > 0; i++) {
            dest[i] = q->items[q->tail];
            q->tail = (q->tail + 1) % q->capacity;
            q->count--;
        }
    }

    pthread_cond_broadcast(&q->cond_full);
    pthread_mutex_unlock(&q->mu);
    return i;
}

void queue_clear(Queue *q) {
    pthread_mutex_lock(&q->mu);
    q->head = 0;
    q->tail = 0;
    pthread_mutex_unlock(&q->mu);
}

void queue_abort(Queue *q) {
    pthread_mutex_lock(&q->mu);
    q->is_active = 0;
    pthread_cond_broadcast(&q->cond_empty);
    pthread_cond_broadcast(&q->cond_full);
    pthread_mutex_unlock(&q->mu);
}

void queue_destroy(Queue *q) {
    pthread_mutex_lock(&q->mu);
    free(q->items);
    pthread_mutex_unlock(&q->mu);
    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->cond_empty);
    pthread_cond_destroy(&q->cond_full);
    *q = (Queue){0};
}

void queue_enqueue2(Queue *q, int fd, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count + len > q->capacity && q->is_active)
        pthread_cond_wait(&q->cond_full, &q->mu);

    ssize_t bytes_readed = 0;
    if (q->is_active) {
        for (size_t i = 0; i < len; i += bytes_readed, len -= i) {
            struct iovec vec[2];
            size_t vec_count = 1;
            size_t rest_bytes = (q->head + len) % q->capacity;
            vec[0].iov_base = q->items + q->head;
            vec[0].iov_len = len;
            if (rest_bytes != q->head + len) {
                vec[0].iov_len = len - rest_bytes;
                vec[1].iov_base = q->items;
                vec[1].iov_len = rest_bytes;
                vec_count = 2;
            }
            bytes_readed = readv(fd, vec, vec_count);
            if (bytes_readed == -1) {
                bytes_readed = 0;
                if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                    nob_log(ERROR, TRACE_FMT, TRACE_ARG);
                }
            }
            q->head = (q->head + bytes_readed) % q->capacity;
            q->count += bytes_readed;
        }
    }

    pthread_cond_broadcast(&q->cond_empty);
    pthread_mutex_unlock(&q->mu);
}

size_t queue_dequeue2(Queue *q, unsigned char *dest, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count == 0 && q->is_active)
        pthread_cond_wait(&q->cond_empty, &q->mu);

    size_t to_read = 0;
    if (q->is_active) {
        to_read = min(q->count, len);
        size_t rest_bytes = (q->tail + to_read) % q->capacity;
        if (rest_bytes == q->tail + to_read) {
            memcpy(dest, q->items + q->tail, to_read);
        } else {
            size_t first_bytes = to_read - rest_bytes;
            memcpy(dest, q->items + q->tail, first_bytes);
            memcpy(dest + first_bytes, q->items, rest_bytes);
        }
        q->tail = rest_bytes;
        q->count -= to_read;
    }

    pthread_cond_broadcast(&q->cond_full);
    pthread_mutex_unlock(&q->mu);
    return to_read;
}

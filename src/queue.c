#include "queue.h"
#include "logger.h"
#include <pthread.h>
#include <stdlib.h>

int queue_init(Queue *q, size_t cap) {
    *q = (Queue){0};

    if (cap == 0) {
        LOG_ERROR("capacity equals to 0");
        return 1;
    }

    if (pthread_mutex_init(&q->mu, NULL) == -1) {
        LOG_CUSTOM_ERRNO("pthread_mutex_init");
        return 0;
    }

    if (pthread_cond_init(&q->cond_empty, NULL) == -1) {
        LOG_CUSTOM_ERRNO("pthread_cond_init");
        pthread_mutex_destroy(&q->mu);
        return 0;
    }

    if (pthread_cond_init(&q->cond_full, NULL) == -1) {
        LOG_CUSTOM_ERRNO("pthread_cond_init");
        pthread_mutex_destroy(&q->mu);
        pthread_cond_destroy(&q->cond_empty);
        return 0;
    }

    q->is_active = 1;
    q->cap = cap;
    q->buf = malloc(cap * sizeof(*q->buf));

    if (!q->buf) {
        LOG_CUSTOM_ERRNO("malloc");
        pthread_mutex_destroy(&q->mu);
        pthread_cond_destroy(&q->cond_empty);
        pthread_cond_destroy(&q->cond_full);
        return 0;
    }

    return 1;
}

void queue_enqueue(Queue *q, unsigned char *src, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count + len > q->cap && q->is_active) {
        LOG_DEBUG("Blocking thread on 'cond_full'");
        pthread_cond_wait(&q->cond_full, &q->mu);
    }

    if (!q->is_active) {
        pthread_mutex_unlock(&q->mu);
        return;
    }

    for (size_t i = 0; i < len; i++) {
        size_t idx = (q->head + q->count) % q->cap;
        q->count++;
        q->buf[idx] = src[i];
    }

    pthread_cond_broadcast(&q->cond_empty);
    pthread_mutex_unlock(&q->mu);
}

size_t queue_dequeue(Queue *q, unsigned char *dest, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->count == 0 && q->is_active) {
        LOG_DEBUG("Blocking thread on 'cond_empty'");
        pthread_cond_wait(&q->cond_empty, &q->mu);
    }

    if (!q->is_active) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }

    size_t to_read = len < q->count ? len : q->count;

    for (size_t i = 0; i < to_read; i++) {
        dest[i] = q->buf[q->head];
        q->head = (q->head + 1) % q->cap;
        q->count--;
    }

    pthread_cond_broadcast(&q->cond_full);
    pthread_mutex_unlock(&q->mu);

    return to_read;
}

void queue_clear(Queue *q) {
    pthread_mutex_lock(&q->mu);
    q->head = 0;
    q->count = 0;
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

    if (q->buf) {
        free(q->buf);
    }

    pthread_mutex_unlock(&q->mu);

    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->cond_empty);
    pthread_cond_destroy(&q->cond_full);

    *q = (Queue){0};
}

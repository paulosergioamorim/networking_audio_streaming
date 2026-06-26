#include "queue.h"
#include "nob.h"
#include <pthread.h>
#include <stdlib.h>

int queue_init(Queue *q, size_t capacity) {
    *q = (Queue){0};

    if (capacity == 0) {
        nob_log(ERROR, "capacity equals to 0");
        goto err_cap;
    }

    if (pthread_mutex_init(&q->mu, NULL) == -1) {
        nob_log(ERROR, "pthread_mutex_init");
        goto err_mu;
    }

    if (pthread_cond_init(&q->cond_empty, NULL) == -1) {
        nob_log(ERROR, "pthread_cond_init");
        goto err_empty;
    }

    if (pthread_cond_init(&q->cond_full, NULL) == -1) {
        nob_log(ERROR, "pthread_cond_init");
        goto err_full;
    }

    q->capacity = capacity;
    q->items = malloc(capacity * sizeof(*q->items));

    if (!q->items) {
        nob_log(ERROR, "malloc");
        goto err_malloc;
    }

    q->is_active = 1;
    return 1;

err_malloc:
    pthread_cond_destroy(&q->cond_empty);
err_full:
    pthread_mutex_destroy(&q->mu);
err_empty:
    pthread_cond_destroy(&q->cond_full);
err_cap:
err_mu:
    return 0;
}

size_t queue_count(Queue *q) {
    size_t count = q->head - q->tail;
    if (q->head < q->tail)
        count += q->capacity;
    return count;
}

void queue_enqueue(Queue *q, unsigned char *src, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (queue_count(q) + len > q->capacity && q->is_active)
        pthread_cond_wait(&q->cond_full, &q->mu);

    if (q->is_active) {
        for (size_t i = 0; i < len; i++) {
            q->items[q->head] = src[i];
            q->head = (q->head + 1) % q->capacity;
        }
    }

    pthread_cond_broadcast(&q->cond_empty);
    pthread_mutex_unlock(&q->mu);
}

size_t queue_dequeue(Queue *q, unsigned char *dest, size_t len) {
    pthread_mutex_lock(&q->mu);

    while (q->head == q->tail && q->is_active)
        pthread_cond_wait(&q->cond_empty, &q->mu);

    size_t i = 0;
    if (q->is_active) {
        for (i = 0; i < len && q->head != q->tail; i++) {
            dest[i] = q->items[q->tail];
            q->tail = (q->tail + 1) % q->capacity;
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

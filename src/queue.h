#ifndef QUEUE_H
#define QUEUE_H

#include <pthread.h>
#include <stddef.h>

typedef struct {
    unsigned char *buf;
    pthread_mutex_t mu;
    pthread_cond_t cond_empty;
    pthread_cond_t cond_full;
    int is_active;
    size_t head;
    size_t cap;
    size_t count;
} Queue;

/**
 * @brief Init <q> with non-growable <cap> bytes of capacity
 * @return 1 if success. 0 if <cap> is 0, if <malloc>, <mutex>, <cond> init's failed
 */
int queue_init(Queue *q, size_t cap);

/**
 * @brief Enqueue <len> bytes of <src> in <q>. Blocks if <q> is full
 */
void queue_enqueue(Queue *q, unsigned char *src, size_t len);

/**
 * @brief Dequeue at most <len> bytes of <q> to <dest>. Blocks if <q> is empty
 */
size_t queue_dequeue(Queue *q, unsigned char *dest, size_t len);

/**
 * @brief Clear <q>. Set <head> and <count> to 0
 */
void queue_clear(Queue *q);

void queue_abort(Queue *q);

/**
 * @brief Destroy <q>. Free resources and set <q> to {0}
 */
void queue_destroy(Queue *q);

#endif

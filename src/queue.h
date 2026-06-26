/// @file queue.h
/// @author paulosergioamorim

#ifndef QUEUE_H
#define QUEUE_H

#include <pthread.h>
#include <stddef.h>

/**
 * @brief Blocking circular buffer of bytes.
 */
typedef struct {
    unsigned char *items;
    pthread_mutex_t mu;
    pthread_cond_t cond_empty;
    pthread_cond_t cond_full;
    int is_active;
    size_t head;
    size_t tail;
    size_t capacity;
} Queue;

/**
 * @brief Init queue with non-growable capacity bytes
 * @param q Not null pointer to the queue
 * @param capacity Non zero capacity
 * @return 1 if success. 0 if capacity is 0, if alloc, mutex, cond init's failed
 */
int queue_init(Queue *q, size_t capacity);

/**
 * @brief Enqueue len bytes of src in queue. Blocks if queue should be overflows with more len bytes enqueued
 * @param q Not null pointer to the queue
 * @param src Memory region to enqueue with size of at least len bytes
 * @param len Amount of bytes to enqueue
 */
void queue_enqueue(Queue *q, unsigned char *src, size_t len);

/**
 * @brief Dequeue at most len bytes of queue to dest. Blocks if queue is empty
 * @param q Not null pointer to the queue
 * @param dest Memory region to dequeue with size of at least len bytes
 * @param len Amount of bytes to dequeue
 * @return The amount of bytes dequeued (if the count of bytes of queue is less then len)
 */
size_t queue_dequeue(Queue *q, unsigned char *dest, size_t len);

/**
 * @brief Clear queue. Set head and tail to 0
 * @param q Not null pointer to the queue
 */
void queue_clear(Queue *q);

/**
 * @brief Set is_active to 0 and signal all threads.
 * @param q Not null pointer to the queue
 */
void queue_abort(Queue *q);

/**
 * @brief Destroy queue. Free resources
 * @param q Not null pointer to the queue
 */
void queue_destroy(Queue *q);

#endif

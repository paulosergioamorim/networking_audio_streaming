#ifndef IO_H
#define IO_H

#include <stdint.h>
#include <time.h>

int timer_realtime_create(time_t tv_sec, time_t tv_nsec);

int socket_create_server(const char *addr, int port, int backlog);

int socket_create_client(const char *addr, int port);

int epoll_add_fd(int epollfd, int fd, uint32_t events);

int epoll_del_fd(int epollfd, int fd);

int epoll_mod_fd(int epollfd, int fd, uint32_t events);

#endif /* end of include guard: IO_H */

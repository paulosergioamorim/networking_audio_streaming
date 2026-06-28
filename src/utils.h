#ifndef UTILS_H
#define UTILS_H

#include <errno.h>
#include <stdint.h>
#include <string.h>

#define TRACE_FMT "%s:%d %s() [%s] %s"
#define TRACE_ARG __FILE__, __LINE__, __FUNCTION__, strerrorname_np(errno), strerror(errno)
#define min(a, b) (((a) < (b)) ? a : b)

int timer_realtime_create();

int socket_create_server(const char *addr, int port, int backlog);

int socket_create_client(const char *addr, int port);

int socket_accept(int sockfd);

int epoll_add_fd(int epollfd, int fd, uint32_t events);

int epoll_del_fd(int epollfd, int fd);

int epoll_mod_fd(int epollfd, int fd, uint32_t events);

#endif /* ifndef UTILS_H */

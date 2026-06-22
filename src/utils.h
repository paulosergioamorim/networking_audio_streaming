#ifndef SOCKETS_H
#define SOCKETS_H

#include "errnoname.h"
#include <errno.h>
#include <stdint.h>

#define TRACE_FMT "%s:%d %s() [%s] %s"
#define TRACE_ARG __FILE__, __LINE__, __FUNCTION__, errnoname(errno), strerror(errno)

typedef int Epoll_Fd;
typedef int Sock_Fd;

int timer_realtime_create();

Sock_Fd socket_create_server(const char *addr, int port, int backlog);

Sock_Fd socket_create_client(const char *addr, int port);

Sock_Fd socket_accept(Sock_Fd socket);

int fd_set_nonblocking(int fd);

int epoll_add_fd(Epoll_Fd epoll, int fd, uint32_t events);

int epoll_del_fd(Epoll_Fd epoll, int fd);

int epoll_mod_fd(Epoll_Fd epoll, int fd, uint32_t events);

#endif /* ifndef SOCKETS_H */

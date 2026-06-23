#include "utils.h"
#include "nob.h"
#include "utils.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>

Sock_Fd socket_create_server(const char *addr, int port, int backlog) {
    struct sockaddr_in sockaddr = {0};
    /* Step1: create a TCP socket */
    Sock_Fd sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    int opt = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    /* Initialize the socket address structure */
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    /* Convert string address to numeric address */
    int ok = inet_pton(AF_INET, addr, &sockaddr.sin_addr.s_addr);

    if (ok <= 0) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    /* Step2: bind the socket to port <port> on the local host */
    if (bind(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) != 0) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    /* Step3: listen for incoming connections */
    if (listen(sock, backlog) != 0) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    return sock;

err:
    if (sock > 0)
        close(sock);
    return 0;
}

Sock_Fd socket_create_client(const char *addr, int port) {
    Sock_Fd sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err_socket;
    }

    struct sockaddr_in server_sockaddr = {0};
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(port);

    int ok = inet_pton(AF_INET, addr, &server_sockaddr.sin_addr.s_addr);

    if (ok <= 0) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    if (connect(sock, (struct sockaddr *)&server_sockaddr, sizeof(server_sockaddr)) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    return sock;

err:
    close(sock);
err_socket:
    return 0;
}

Sock_Fd socket_accept(Fd socket) {
    Sock_Fd sock = accept4(socket, NULL, NULL, SOCK_NONBLOCK);

    if (sock == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }

        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return -1;
    }

    return sock;
}

int fd_set_nonblocking(Fd fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }
    flags = flags | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }
    return 1;
}

int epoll_add_fd(Fd epoll, int fd, uint32_t events) {
    struct epoll_event ev = {
        .data.fd = fd,
        .events = events,
    };
    if (epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &ev) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }
    return 1;
}

int epoll_del_fd(Fd epoll, int fd) {
    if (epoll_ctl(epoll, EPOLL_CTL_DEL, fd, NULL) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }
    return 1;
}

int epoll_mod_fd(Fd epoll, int fd, uint32_t events) {
    struct epoll_event ev = {
        .data.fd = fd,
        .events = events,
    };
    if (epoll_ctl(epoll, EPOLL_CTL_MOD, fd, &ev) == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return 0;
    }
    return 1;
}

int timer_realtime_create() {
    // timer for send streaming packets
    struct itimerspec tspec = {0};
    int fd = timerfd_create(CLOCK_REALTIME, 0);

    if (fd == -1) {
        nob_log(ERROR, "timerfd_create");
        goto err;
    }

    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        nob_log(ERROR, "clock_gettime");
        goto err;
    }

    tspec.it_interval.tv_sec = 0;
    tspec.it_interval.tv_nsec = 100000000;
    tspec.it_value.tv_sec = now.tv_sec;
    tspec.it_value.tv_nsec = now.tv_nsec;

    if (timerfd_settime(fd, TFD_TIMER_ABSTIME, &tspec, NULL) == -1) {
        nob_log(ERROR, "timerfd_settime");
        goto err;
    }

    return fd;

err:
    if (fd > 0)
        close(fd);
    return 0;
}

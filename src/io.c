#include "io.h"
#include "debug.h"
#include "nob.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>

int fd_set_nonblocking(int fd);

int socket_create_server(const char *addr, int port, int backlog) {
    struct sockaddr_in sockaddr = {0};
    int sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    int ok = 0, opt = 0;

    if (sock == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_socket;
    }

    opt = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    ok = inet_pton(AF_INET, addr, &sockaddr.sin_addr.s_addr);

    if (ok <= 0) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    if (bind(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) != 0) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    if (listen(sock, backlog) != 0) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    return sock;

err:
    close(sock);
err_socket:
    return 0;
}

int socket_create_client(const char *addr, int port) {
    struct sockaddr_in sockaddr = {0};
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    int ok = 0;

    if (sock == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_socket;
    }

    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);

    ok = inet_pton(AF_INET, addr, &sockaddr.sin_addr.s_addr);

    if (ok <= 0) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    if (connect(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    if (fd_set_nonblocking(sock) == 0)
        goto err;

    return sock;

err:
    close(sock);
err_socket:
    return 0;
}

int fd_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }
    flags = flags | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }
    return 1;
}

int epoll_add_fd(int epoll, int fd, uint32_t events) {
    struct epoll_event ev = {0};
    ev.data.fd = fd;
    ev.events = events;
    if (epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &ev) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }
    return 1;
}

int epoll_del_fd(int epoll, int fd) {
    if (epoll_ctl(epoll, EPOLL_CTL_DEL, fd, NULL) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }
    return 1;
}

int epoll_mod_fd(int epoll, int fd, uint32_t events) {
    struct epoll_event ev = {0};
    ev.data.fd = fd;
    ev.events = events;
    if (epoll_ctl(epoll, EPOLL_CTL_MOD, fd, &ev) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return 0;
    }
    return 1;
}

int timer_realtime_create(time_t tv_sec, time_t tv_nsec) {
    struct itimerspec tspec = {0};
    int timer = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK);

    if (timer == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_timerfd_create;
    }

    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    tspec.it_interval.tv_sec = tv_sec;
    tspec.it_interval.tv_nsec = tv_nsec;
    tspec.it_value.tv_sec = now.tv_sec;
    tspec.it_value.tv_nsec = now.tv_nsec;

    if (timerfd_settime(timer, TFD_TIMER_ABSTIME, &tspec, NULL) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err;
    }

    return timer;

err:
    close(timer);
err_timerfd_create:
    return 0;
}

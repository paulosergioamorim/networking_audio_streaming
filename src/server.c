#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#define AUDIODIR "./audios"

typedef struct {
    struct sockaddr_in addr;
    socklen_t addrlen;
    size_t offset;
    int playing; // is streaming?
    int fd;
} Connection_State;

typedef struct {
    int key; // the connection socket descriptor
    Connection_State value;
} Connections_State;

int server_setup(int *listen_sock, int *epollfd);

void server_destroy(Connections_State *conns);

int create_tcp_server_socket();

int create_udp_server_socket();

#define MAX_EVENTS 10

int sockudp;

int main(int argc, char **argv) {
    int tcpsock = 0;
    int epollfd = 0;
    Connections_State *conns = NULL;

    if (signals_sigint_sigaction() == -1) {
        return 1;
    }

    if (server_setup(&tcpsock, &epollfd) == -1) {
        return 1;
    }

    hmput(conns, tcpsock, (Connection_State){0});
    hmput(conns, epollfd, (Connection_State){0});

    int nfds = 0;
    struct epoll_event events[MAX_EVENTS];
    struct epoll_event ev;
    struct sockaddr_in new_addr = {0};
    socklen_t addrlen = 0;
    Message req;
    Message res;

    while (!signaled) {
        nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1);
        if (nfds == -1) {
            perror("epoll_wait");
            server_destroy(conns);
            return 1;
        }

        for (int i = 0; i < nfds; i++) {
            int sockfd = events[i].data.fd;
            if (sockfd == tcpsock) {
                int conn_sock = accept(tcpsock, (struct sockaddr *)&new_addr, &addrlen);
                printf("Accept connection on fd %d\n", conn_sock);
                if (conn_sock == -1) {
                    perror("accept");
                    server_destroy(conns);
                    return 1;
                }
                ev.events = EPOLLIN;
                ev.data.fd = conn_sock;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
                    perror("epoll_ctl: conn_sock");
                    server_destroy(conns);
                    return 1;
                }
                hmput(conns, conn_sock, (Connection_State){0});
                continue;
            }

            memset(&req, 0, sizeof(req));
            memset(&res, 0, sizeof(res));
            int ok = recv(sockfd, &req, sizeof(req), 0);
            if (ok == -1) {
                perror("recv");
                break;
            }
            if (ok == 0) {
                printf("Closing connection fd %d\n", sockfd);
                if (epoll_ctl(epollfd, EPOLL_CTL_DEL, sockfd, NULL) == -1) {
                    perror("epoll_ctl: conn");
                    server_destroy(conns);
                    return 1;
                }
                close(sockfd);
                hmdel(conns, sockfd);
                continue;
            }
            if (strcmp(req.buf, "/list") == 0) {
                DIR *dir = opendir(AUDIODIR);
                struct dirent *de;
                while ((de = readdir(dir)) != NULL) {
                    if (de->d_type != 'd' && suffix_is_audio(de->d_name)) {
                        strcat(res.buf, de->d_name);
                        strcat(res.buf, "\n");
                    }
                }
                res.kind = MESSAGE_DISPLAY;
                send(sockfd, &res, sizeof(res), 0);
                closedir(dir);
            } else if (strncmp(req.buf, "/start", 6) == 0) {
                char *filename = req.buf + sizeof("/start ") - 1;
                char fullname[255] = AUDIODIR "/";
                strncat(fullname, filename, sizeof(fullname) - sizeof(AUDIODIR "/"));
                struct stat st;
                int fd = open(fullname, O_RDONLY);
                if (ok == -1 && errno == ENOENT) {
                    strcat(res.buf, "No file\n");
                    send(sockfd, &res, sizeof(res), 0);
                }
                Connections_State *state = hmgetp(conns, sockfd);
                state->value.fd = fd;
                res.kind = MESSAGE_INTERNAL;
                strcat(res.buf, "START OK");
                send(sockfd, &res, sizeof(res), 0);
            } else if (strcmp(req.buf, "/stop") == 0) {
            } else if (strcmp(req.buf, "/resume") == 0) {
            } else {
            }
        }
    }

    server_destroy(conns);
    return 0;
}

int create_tcp_server_socket() {
    struct sockaddr_in saddr;
    int fd, ret_val;

    /* Step1: create a TCP socket */
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        perror("socket");
        return -1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
        perror("setsockopt");
        return -1;
    }

    /* Initialize the socket address structure */
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(8000);
    saddr.sin_addr.s_addr = INADDR_ANY;

    /* Step2: bind the socket to port 8000 on the local host */
    ret_val = bind(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val != 0) {
        fprintf(stderr, "bind failed [%s]\n", strerror(errno));
        return -1;
    }

    /* Step3: listen for incoming connections */
    ret_val = listen(fd, 5);
    if (ret_val != 0) {
        fprintf(stderr, "listen failed [%s]\n", strerror(errno));
        return -1;
    }

    return fd;
}

int server_setup(int *listenfd_ptr, int *epollfd_ptr) {
    struct epoll_event ev;
    int fd, epollfd;

    fd = create_tcp_server_socket();

    if (fd == -1) {
        return -1;
    }

    epollfd = epoll_create1(0);
    if (epollfd == -1) {
        perror("epoll_create1");
        return -1;
    }

    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        perror("epoll_ctl: listen_sock");
        return -1;
    }

    *listenfd_ptr = fd;
    *epollfd_ptr = epollfd;
    return 0;
}

void server_destroy(Connections_State *connections) {
    for (int i = 0; i < hmlen(connections); i++) {
        Connections_State item = connections[i];
        close(item.key);
        if (item.value.fd > 0) {
            close(item.value.fd);
        }
    }
    hmfree(connections);
}

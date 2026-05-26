#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
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

typedef struct {
    int tcp_sock;
    int udp_sock;
    int epoll_fd;
    Connections_State *conns;
} Audio_Server;

#define MAX_EVENTS 10

int audio_server_create_tcp_socket(const char *addr, int port);

int audio_server_create_udp_socket(const char *addr, int port);

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port, int udp_port);

void audio_server_destroy(Audio_Server *s);

int main(int argc, char **argv) {
    Audio_Server s;
    int tcp_port = atoi(argv[2]);
    int udp_port = atoi(argv[3]);

    if (tcp_port == 0 || udp_port == 0) {
        fprintf(stderr, "Args Error!\nCommand help: ./server <ip-address> <tcp_port> <udp_port>\n");
        return 1;
    }

    int ok = audio_server_init(&s, argv[1], tcp_port, udp_port);

    if (!ok) {
        audio_server_destroy(&s);
        return 1;
    }

    int nfds = 0;
    struct epoll_event events[MAX_EVENTS];
    struct epoll_event ev;
    struct sockaddr_in new_addr = {0};
    socklen_t addrlen = 0;
    Message req;
    Message res;

    while (!signaled) {
        nfds = epoll_wait(s.epoll_fd, events, MAX_EVENTS, -1);

        if (nfds == -1) {
            perror("epoll_wait");
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < nfds; i++) {
            int sockfd = events[i].data.fd;

            if (sockfd == s.tcp_sock) {
                int conn_sock = accept(s.tcp_sock, (struct sockaddr *)&new_addr, &addrlen);
                printf("Accept connection on fd %d\n", conn_sock);

                if (conn_sock == -1) {
                    perror("accept");
                    audio_server_destroy(&s);
                    return 1;
                }

                ev.events = EPOLLIN;
                ev.data.fd = conn_sock;

                if (epoll_ctl(s.epoll_fd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
                    perror("epoll_ctl: conn_sock");
                    audio_server_destroy(&s);
                    return 1;
                }

                hmput(s.conns, conn_sock, (Connection_State){0});
                continue;
            } // the client has connected in the tcp socket

            memset(&req, 0, sizeof(req));
            memset(&res, 0, sizeof(res));
            int ok = recv(sockfd, &req, sizeof(req), 0);

            if (ok == -1) {
                perror("recv");
                break;
            }

            if (ok == 0) {
                printf("Closing connection fd %d\n", sockfd);

                if (epoll_ctl(s.epoll_fd, EPOLL_CTL_DEL, sockfd, NULL) == -1) {
                    perror("epoll_ctl: conn");
                    audio_server_destroy(&s);
                    return 1;
                }

                close(sockfd);
                hmdel(s.conns, sockfd);
                continue;
            } // the client has closed the tcp socket

            if (req.kind == REQ_LIST) {
                DIR *dir = opendir(AUDIODIR);
                struct dirent *de;

                while ((de = readdir(dir)) != NULL) {
                    if (de->d_type != 'd' && suffix_is_audio(de->d_name)) {
                        res.kind = RES_LIST_CONTINUE;
                        strcpy(res.buf, de->d_name);
                        send(sockfd, &res, sizeof(res), 0);
                    }
                }

                res.kind = RES_LIST_END;
                memset(res.buf, 0, sizeof(res.buf));
                send(sockfd, &res, sizeof(res), 0);
                closedir(dir);
                continue;
            }
            if (req.kind == REQ_START) {
                char *filename = req.buf + sizeof("/start ") - 1;
                char fullname[255] = AUDIODIR "/";
                strncat(fullname, filename, sizeof(fullname) - sizeof(AUDIODIR "/"));
                int fd = open(fullname, O_RDONLY);

                if (fd == -1 && errno == ENOENT) {
                    res.kind = RES_START_NO_FILE;
                    send(sockfd, &res, sizeof(res), 0);
                }

                Connections_State *state = hmgetp(s.conns, sockfd);
                state->value.fd = fd;
                res.kind = RES_START_OK;
                strcat(res.buf, "START OK");
                send(sockfd, &res, sizeof(res), 0);
                continue;
            }
            if (req.kind == REQ_STOP) {
                Connections_State *state = hmgetp(s.conns, sockfd);
                state->value.playing = 0;
                continue;
            }
            if (req.kind == REQ_RESUME) {
                Connections_State *state = hmgetp(s.conns, sockfd);
                state->value.playing = 1;
                continue;
            }
        }
    }

    audio_server_destroy(&s);
    return 0;
}

int audio_server_create_tcp_socket(const char *addr, int port) {
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
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = inet_addr(addr);

    /* Step2: bind the socket to port <port> on the local host */
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

int audio_server_create_udp_socket(const char *addr, int port) {
    struct sockaddr_in saddr;
    int fd, ret_val;

    /* Step1: create a UDP socket */
    fd = socket(AF_INET, SOCK_DGRAM, 0);

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
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = inet_addr(addr);

    /* Step2: bind the socket to port <port> on the local host */
    ret_val = bind(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));

    if (ret_val != 0) {
        fprintf(stderr, "bind failed [%s]\n", strerror(errno));
        return -1;
    }

    return fd;
}

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port, int udp_port) {
    s->conns = NULL;

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    s->tcp_sock = audio_server_create_tcp_socket(addr, tcp_port);

    if (s->tcp_sock == -1) {
        return 0;
    }

    s->udp_sock = audio_server_create_udp_socket(addr, udp_port);

    if (s->udp_sock == -1) {
        return 0;
    }

    struct epoll_event ev;
    s->epoll_fd = epoll_create1(0);

    if (s->epoll_fd == -1) {
        perror("epoll_create1");
        return 0;
    }

    ev.events = EPOLLIN;
    ev.data.fd = s->tcp_sock;

    if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->tcp_sock, &ev) == -1) {
        perror("epoll_ctl: listen_sock");
        return 0;
    }

    return 1;
}

void audio_server_destroy(Audio_Server *s) {
    for (int i = 0; i < hmlen(s->conns); i++) {
        Connections_State item = s->conns[i];
        close(item.key);

        if (item.value.fd > 0) {
            close(item.value.fd);
        }
    }

    hmfree(s->conns);

    if (s->udp_sock > 0) {
        close(s->udp_sock);
    }
    if (s->tcp_sock > 0) {
        close(s->tcp_sock);
    }
    if (s->epoll_fd > 0) {
        close(s->epoll_fd);
    }
}

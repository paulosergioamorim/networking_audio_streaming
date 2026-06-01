#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#define AUDIODIR "./audios"

typedef struct {
    size_t offset;
    int playing; // is streaming?
    int fd;
    int sock;
} Connection_State;

typedef struct {
    int key; // the connection socket descriptor
    Connection_State value;
} Connections_State;

typedef struct {
    int tcp_sock;
    int epoll_fd;
    Connections_State *conns;
    pthread_mutex_t conns_mutex;
    pthread_t streaming_thread;
} Audio_Server;

void audio_server_transmit_packet(Connection_State *connection_state);

void *audio_server_streaming_thread(void *connections_state);

int audio_server_create_tcp_socket(const char *addr, int port);

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port);

void audio_server_destroy(Audio_Server *s);

int main(int argc, char **argv) {
    Audio_Server s;

    if (argc < 3) {
        fprintf(stderr, "Args Error!\nCommand help: ./server <ip-address> <port>\n");
        return 1;
    }

    const char *ip_addr = argv[1];
    int port = atoi(argv[2]);

    if (port == 0) {
        fprintf(stderr, "Args Error!\nCommand help: ./server <ip-address> <port>\n");
        return 1;
    }

    int ok = audio_server_init(&s, ip_addr, port);

    if (!ok) {
        audio_server_destroy(&s);
        return 1;
    }

    int N = 0;
    const int MAX_EVENTS = 10;
    struct epoll_event events[MAX_EVENTS];
    struct epoll_event ev;

    pthread_create(&s.streaming_thread, NULL, audio_server_streaming_thread, &s);

    while (!signaled) {
        N = epoll_wait(s.epoll_fd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            perror("epoll_wait");
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            int event_sock = events[i].data.fd;

            if (event_sock == s.tcp_sock) {
                int conn_sock = accept(s.tcp_sock, NULL, NULL);

                if (conn_sock == -1) {
                    continue;
                }

                ev.events = EPOLLIN;
                ev.data.fd = conn_sock;
                epoll_ctl(s.epoll_fd, EPOLL_CTL_ADD, conn_sock, &ev);

                Connection_State state = {0};
                state.sock = conn_sock;

                pthread_mutex_lock(&s.conns_mutex);
                hmput(s.conns, conn_sock, state);
                pthread_mutex_unlock(&s.conns_mutex);
                continue;
            }

            Message req = {0};
            Message res = {0};
            ssize_t bytes_readed = recv(event_sock, &req, sizeof(req), MSG_WAITALL);

            if (bytes_readed == -1) {
                perror("recv");
                break;
            } else if (bytes_readed == 0) {
                pthread_mutex_lock(&s.conns_mutex);
                Connections_State *state = hmgetp(s.conns, event_sock);
                if (state != NULL) {
                    if (state->value.fd > 0) {
                        close(state->value.fd);
                    }
                }
                hmdel(s.conns, event_sock);
                pthread_mutex_unlock(&s.conns_mutex);
                epoll_ctl(s.epoll_fd, EPOLL_CTL_DEL, event_sock, &ev);
                close(event_sock);
            }

            if (req.kind == REQ_LIST) {
                DIR *dir = opendir(AUDIODIR);
                if (dir == NULL) {
                    res.kind = RES_LIST_END;
                    memset(res.buf, 0, sizeof(res.buf));
                    send(event_sock, &res, sizeof(res), 0);
                    closedir(dir);
                    continue;
                }

                struct dirent *de;

                while ((de = readdir(dir)) != NULL) {
                    if (de->d_type != 'd' && suffix_is_audio(de->d_name)) {
                        res.kind = RES_LIST_CONTINUE;
                        strcpy(res.buf, de->d_name);
                        send(event_sock, &res, sizeof(res), 0);
                    }
                }

                res.kind = RES_LIST_END;
                memset(res.buf, 0, sizeof(res.buf));
                send(event_sock, &res, sizeof(res), 0);
                closedir(dir);
                continue;
            }
            if (req.kind == REQ_START) {
                char *filename = req.buf;
                char fullname[255] = AUDIODIR "/";
                strncat(fullname, filename, sizeof(fullname) - sizeof(AUDIODIR "/"));
                int fd = open(fullname, O_RDONLY);

                if (fd == -1) {
                    res.kind = RES_START_NO_FILE;
                    send(event_sock, &res, sizeof(res), 0);
                    continue;
                }

                pthread_mutex_lock(&s.conns_mutex);
                Connections_State *state = hmgetp(s.conns, event_sock);
                if (state != NULL) {
                    state->value.offset = 0;
                    if (state->value.fd > 0) {
                        close(state->value.fd);
                    }
                    state->value.fd = fd;
                    state->value.playing = 1;
                }
                pthread_mutex_unlock(&s.conns_mutex);

                res.kind = RES_START_OK;
                send(event_sock, &res, sizeof(res), 0);

                continue;
            }
            if (req.kind == REQ_STOP) {
                pthread_mutex_lock(&s.conns_mutex);
                Connections_State *state = hmgetp(s.conns, event_sock);
                if (state != NULL) {
                    state->value.playing = 0;
                }
                pthread_mutex_unlock(&s.conns_mutex);
                res.kind = RES_STOP;
                send(event_sock, &res, sizeof(res), 0);
                continue;
            }
            if (req.kind == REQ_RESUME) {
                pthread_mutex_lock(&s.conns_mutex);
                Connections_State *state = hmgetp(s.conns, event_sock);
                if (state != NULL) {
                    state->value.playing = 1;
                }
                pthread_mutex_unlock(&s.conns_mutex);
                res.kind = RES_RESUME;
                send(event_sock, &res, sizeof(res), 0);
                continue;
            }
        }
    }

    audio_server_destroy(&s);
    return 0;
}

void audio_server_transmit_packet(Connection_State *c) {
    Message res = {0};
    res.kind = RES_STREAM;

    if (lseek(c->fd, c->offset, SEEK_SET) < 0) {
        return;
    }

    gettimeofday(&res.tv, NULL);
    ssize_t bytes_read = read(c->fd, &res.buf, sizeof(res.buf));

    if (bytes_read <= 0) {
        c->playing = 0;
        c->offset = 0;
        return;
    }

    res.len = bytes_read;
    ssize_t sent = send(c->sock, &res, sizeof(Message), MSG_NOSIGNAL | MSG_DONTWAIT);

    if (sent == sizeof(Message)) {
        c->offset += bytes_read;
    } else if (sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    } else {
        c->playing = 0;
    }
}

void *audio_server_streaming_thread(void *audio_server) {
    Audio_Server *s = (Audio_Server *)audio_server;

    while (1) {
        if (!s->conns) {
            return NULL;
        }

        int n_conns = hmlen(s->conns);
        for (int i = 0; i < n_conns; i++) {
            pthread_mutex_lock(&s->conns_mutex);

            Connection_State *c = &s->conns[i].value;
            if (c->playing == 1) {
                audio_server_transmit_packet(c);
            }
            pthread_mutex_unlock(&s->conns_mutex);
        }
        usleep(3000);
    }

    return NULL;
}

int audio_server_create_tcp_socket(const char *addr, int port) {
    struct sockaddr_in sockaddr;
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
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    sockaddr.sin_addr.s_addr = inet_addr(addr);

    /* Step2: bind the socket to port <port> on the local host */
    ret_val = bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr));

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

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port) {
    *s = (Audio_Server){0};

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    pthread_mutex_init(&s->conns_mutex, NULL);

    s->tcp_sock = audio_server_create_tcp_socket(addr, tcp_port);

    if (s->tcp_sock == -1) {
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
    pthread_mutex_lock(&s->conns_mutex);
    for (int i = 0; i < hmlen(s->conns); i++) {
        Connections_State item = s->conns[i];
        close(item.key);
        if (item.value.fd > 0) {
            close(item.value.fd);
        }
    }
    hmfree(s->conns); // make streaming thread able to return
    pthread_mutex_unlock(&s->conns_mutex);
    pthread_join(s->streaming_thread, NULL); // collect thread (free resources)
    pthread_mutex_destroy(&s->conns_mutex);
    if (s->tcp_sock > 0) {
        close(s->tcp_sock);
    }
    if (s->epoll_fd > 0) {
        close(s->epoll_fd);
    }
}

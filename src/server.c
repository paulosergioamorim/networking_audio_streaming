#include "logger.h"
#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <pthread.h>
#include <stddef.h>
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
#define BACKLOG 5

typedef struct {
    size_t offset;
    int playing; // is streaming?
    int fd;
    int sockfd;
} Client_State;

typedef struct {
    int key; // the connection socket descriptor
    Client_State value;
} Clients_State;

typedef struct {
    char *key;         // basename
    const char *value; // path
} Audio;

typedef struct {
    int sockfd;
    int epollfd;
    Clients_State *clients;
    Audio *audios;
    pthread_mutex_t mu;
    pthread_t streaming_thread;
} Audio_Server;

void audio_server_transmit_packet(Client_State *c);

void *audio_server_streaming_thread(void *arg);

int audio_server_create_tcp_socket(const char *addr, int port);

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port);

void audio_server_load_audios(Audio_Server *s);

void audio_server_destroy(Audio_Server *s);

int main(int argc, char **argv) {
    Audio_Server s;

    logger_initConsoleLogger(stdout);
    logger_setLevel(LogLevel_DEBUG);

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
    LOG_INFO("Server listening on %s:%d", ip_addr, port);

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
        N = epoll_wait(s.epollfd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            LOG_ERROR("epoll_wait(): %s", strerror(errno));
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            int event_sock = events[i].data.fd;

            if (event_sock == s.sockfd) {
                int conn_sock = accept(s.sockfd, NULL, NULL);

                if (conn_sock == -1) {
                    LOG_ERROR("accept(): %s", strerror(errno));
                    continue;
                }

                ev.events = EPOLLIN;
                ev.data.fd = conn_sock;

                if (epoll_ctl(s.epollfd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
                    LOG_ERROR("epoll_ctl() failed: %s", strerror(errno));
                    close(conn_sock);
                    break;
                }

                Client_State state = {0};
                state.sockfd = conn_sock;

                pthread_mutex_lock(&s.mu);
                hmput(s.clients, conn_sock, state);
                pthread_mutex_unlock(&s.mu);
                LOG_INFO("Client connected");
                continue;
            }

            Message req = {0};
            Message res = {0};
            ssize_t bytes_readed = recv(event_sock, &req, sizeof(req), MSG_NOSIGNAL);

            if (bytes_readed == -1) {
                LOG_ERROR("recv() failed: %s", strerror(errno));
                break;
            } else if (bytes_readed == 0) {
                close(event_sock); // this call although removes event_sock from epoll
                LOG_WARN("Client socket closed without sending REQ_EXIT packet");
            }

            if (req.kind == REQ_EXIT) {
                pthread_mutex_lock(&s.mu);
                ptrdiff_t idx = hmgeti(s.clients, event_sock);
                if (idx != -1) {
                    Client_State *state = &s.clients[idx].value;
                    if (state->fd > 0) {
                        close(state->fd);
                    }
                    hmdel(s.clients, event_sock);
                    res.kind = RES_EXIT;
                    ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                    if (ok == -1) {
                        LOG_ERROR("send() failed: %s", strerror(errno));
                    }
                    close(event_sock); // this call although removes event_sock from epoll
                }
                pthread_mutex_unlock(&s.mu);
                LOG_INFO("Client disconnected");
            }

            if (req.kind == REQ_LIST) {
                res.kind = RES_LIST_CONTINUE;
                for (int i = 0; i < shlen(s.audios); i++) {
                    strcpy(res.buf, s.audios[i].key);
                    ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                    if (ok == -1) {
                        LOG_ERROR("send() failed: %s", strerror(errno));
                    }
                }
                res.kind = RES_LIST_END;
                ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                if (ok == -1) {
                    LOG_ERROR("send() failed: %s", strerror(errno));
                }
                continue;
            }
            if (req.kind == REQ_START) {
                char *basename = req.buf;
                ptrdiff_t idx = shgeti(s.audios, basename);

                if (idx == -1) {
                    res.kind = RES_START_NO_FILE;
                    ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                    if (ok == -1) {
                        LOG_ERROR("send() failed: %s", strerror(errno));
                    }
                    continue;
                }

                const char *path = s.audios[idx].value;
                int fd = open(path, O_RDONLY);

                if (fd == -1) {
                    LOG_ERROR("Failed to open indexed file. Maybe has been deleted");
                    res.kind = RES_START_NO_FILE;
                    ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                    if (ok == -1) {
                        LOG_ERROR("send() failed: %s", strerror(errno));
                    }
                }

                pthread_mutex_lock(&s.mu);
                Clients_State *state = hmgetp(s.clients, event_sock);
                if (state != NULL) {
                    state->value.offset = 0;
                    if (state->value.fd > 0) {
                        close(state->value.fd);
                    }
                    state->value.fd = fd;
                    state->value.playing = 1;
                }
                pthread_mutex_unlock(&s.mu);

                res.kind = RES_START_OK;
                ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                if (ok == -1) {
                    LOG_ERROR("send() failed: %s", strerror(errno));
                }

                continue;
            }
            if (req.kind == REQ_STOP) {
                pthread_mutex_lock(&s.mu);
                Clients_State *state = hmgetp(s.clients, event_sock);
                if (state != NULL) {
                    state->value.playing = 0;
                }
                pthread_mutex_unlock(&s.mu);
                res.kind = RES_STOP;
                ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                if (ok == -1) {
                    LOG_ERROR("send() failed: %s", strerror(errno));
                }
                continue;
            }
            if (req.kind == REQ_RESUME) {
                pthread_mutex_lock(&s.mu);
                Clients_State *state = hmgetp(s.clients, event_sock);
                if (state != NULL) {
                    state->value.playing = 1;
                }
                pthread_mutex_unlock(&s.mu);
                res.kind = RES_RESUME;
                ssize_t ok = send(event_sock, &res, sizeof(res), 0);
                if (ok == -1) {
                    LOG_ERROR("send() failed: %s", strerror(errno));
                }
                continue;
            }
        }
    }

    LOG_INFO("Closing server");
    audio_server_destroy(&s);
    return 0;
}

void audio_server_transmit_packet(Client_State *c) {
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
    ssize_t ok = send(c->sockfd, &res, sizeof(res), MSG_NOSIGNAL | MSG_DONTWAIT);

    if (ok == -1) {
        LOG_ERROR("send() failed: %s", strerror(errno));
        return;
    }
    if (ok == 0) {
        c->playing = 0;
        return;
    }
    c->offset += bytes_read; // MSG_DONTWAIT garantees that ok = res.len
}

void *audio_server_streaming_thread(void *audio_server) {
    Audio_Server *s = (Audio_Server *)audio_server;

    while (!signaled) {
        int n_conns = hmlen(s->clients);
        for (int i = 0; i < n_conns; i++) {
            pthread_mutex_lock(&s->mu);

            Client_State *c = &s->clients[i].value;
            if (c->playing == 1) {
                audio_server_transmit_packet(c);
            }
            pthread_mutex_unlock(&s->mu);
        }
        usleep(2000);
    }

    return NULL;
}

int audio_server_create_tcp_socket(const char *addr, int port) {
    struct sockaddr_in sockaddr;
    int fd, ret_val;

    /* Step1: create a TCP socket */
    fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        LOG_ERROR("socket() failed: %s", strerror(errno));
        return -1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
        LOG_ERROR("setsockopt() failed: %s", strerror(errno));
        return -1;
    }

    /* Initialize the socket address structure */
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    sockaddr.sin_addr.s_addr = inet_addr(addr);

    /* Step2: bind the socket to port <port> on the local host */
    ret_val = bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr));

    if (ret_val != 0) {
        LOG_ERROR("bind() failed: %s", strerror(errno));
        return -1;
    }

    /* Step3: listen for incoming connections */
    ret_val = listen(fd, BACKLOG);

    if (ret_val != 0) {
        LOG_ERROR("listen() failed: %s", strerror(errno));
        return -1;
    }

    return fd;
}

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port) {
    *s = (Audio_Server){0};

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    pthread_mutex_init(&s->mu, NULL);

    s->sockfd = audio_server_create_tcp_socket(addr, tcp_port);

    if (s->sockfd == -1) {
        return 0;
    }

    struct epoll_event ev;
    s->epollfd = epoll_create1(0);

    if (s->epollfd == -1) {
        LOG_ERROR("epoll_create1() failed: %s", strerror(errno));
        return 0;
    }

    ev.events = EPOLLIN;
    ev.data.fd = s->sockfd;

    if (epoll_ctl(s->epollfd, EPOLL_CTL_ADD, s->sockfd, &ev) == -1) {
        LOG_ERROR("epoll_ctl() failed: %s", strerror(errno));
        return 0;
    }

    audio_server_load_audios(s);

    return 1;
}

void audio_server_destroy(Audio_Server *s) {
    pthread_mutex_lock(&s->mu);
    for (int i = 0; i < hmlen(s->clients); i++) {
        Clients_State item = s->clients[i];
        close(item.key);
        if (item.value.fd > 0) {
            close(item.value.fd);
        }
    }
    hmfree(s->clients); // make streaming thread able to return
    pthread_mutex_unlock(&s->mu);
    pthread_join(s->streaming_thread, NULL); // collect thread (free resources)
    pthread_mutex_destroy(&s->mu);
    if (s->sockfd > 0) {
        close(s->sockfd);
    }
    if (s->epollfd > 0) {
        close(s->epollfd);
    }
    shfree(s->audios); // free keys and values too
}

void audio_server_load_audios(Audio_Server *s) {
    sh_new_strdup(s->audios); // auto strdup key
    DIR *dir = opendir(AUDIODIR);

    if (!dir) {
        LOG_ERROR("Error to load " AUDIODIR " directory: %s", strerror(errno));
        return;
    }

    struct dirent *de;

    while ((de = readdir(dir)) != NULL) {
        if (de->d_type != 'd' && suffix_is_audio(de->d_name)) {
            char path[NAME_MAX];
            strcpy(path, AUDIODIR "/");
            strcat(path, de->d_name);
            shput(s->audios, de->d_name, path);
        }
    }

    if (closedir(dir) == -1) {
        LOG_ERROR("Error to close " AUDIODIR " directory: %s", strerror(errno));
    }

    LOG_INFO("Loaded audios");
}

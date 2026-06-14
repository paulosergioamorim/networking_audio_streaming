#include "logger.h"
#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <unistd.h>

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#define FLAG_IMPLEMENTATION
#include "flag.h"

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
} Empty;

typedef struct {
    int key;
    Empty value;
} Active_Clients;

typedef struct {
    const char *path; // relative path from pwd
    int len;          // strlen(key) + 1
} Audio;

typedef struct {
    char *key; // idx + basename
    Audio value;
} Audios;

typedef struct {
    int sockfd;
    int epollfd;
    int timerfd;
    Clients_State *clients;
    Active_Clients *active_clients;
    Audios *audios;
} Audio_Server;

void audio_server_transmit_packet(Client_State *c);

void *audio_server_streaming_thread(void *arg);

int audio_server_create_tcp_socket(const char *addr, int port);

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port);

void audio_server_load_audios(Audio_Server *s);

void audio_server_destroy(Audio_Server *s);

void audio_server_handle_accept(Audio_Server *s);

void audio_server_handle_exit(Audio_Server *s, int event_sock);

void audio_server_handle_list(Audio_Server *s, int event_sock, Request *req, Response *res);

void audio_server_handle_start(Audio_Server *s, int event_sock, Request *req, Response *res);

void audio_server_handle_stop(Audio_Server *s, int event_sock, Request *req, Response *res);

void audio_server_handle_resume(Audio_Server *s, int event_sock, Request *req, Response *res);

void audio_server_client_set_streaming(Audio_Server *s, int key, int fd);

void audio_server_client_unset_streaming(Audio_Server *s, int key);

void audio_server_display_usage(FILE *fp) {
    fprintf(fp, "USAGE: ./server [OPTIONS]\n");
    fprintf(fp, "OPTIONS:\n");
    flag_print_options(fp);
}

int main(int argc, char **argv) {
    Audio_Server s;

    logger_initConsoleLogger(stdout);
    logger_setLevel(LogLevel_INFO);

    bool *help = flag_bool("help", false, "Print this help");
    char **ipaddr = flag_str("ipaddr", "0.0.0.0", "Provide the serving IP Address");
    uint64_t *port = flag_uint64("port", 8000, "Provide the serving PORT");
    bool *debug = flag_bool("debug", false, "Print debug levels");

    if (!flag_parse(argc, argv)) {
        audio_server_display_usage(stderr);
        return 1;
    }

    if (*help) {
        audio_server_display_usage(stdout);
        return 0;
    }

    if (*debug) {
        logger_setLevel(LogLevel_DEBUG);
    }

    if (!audio_server_init(&s, *ipaddr, *port)) {
        audio_server_destroy(&s);
        return 1;
    }

    LOG_INFO("Server listening on %s:%d", *ipaddr, *port);

    int N = 0;
    const int MAX_EVENTS = 10;
    struct epoll_event events[MAX_EVENTS];

    while (!signaled) {
        N = epoll_wait(s.epollfd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            LOG_CUSTOM_ERRNO("epoll_wait");
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            uint32_t event_mask = events[i].events;
            int eventfd = events[i].data.fd;

            if (eventfd == s.sockfd) {
                audio_server_handle_accept(&s);
                continue;
            }

            if (eventfd == s.timerfd) {
                uint64_t expdir;
                ssize_t bytes_readed = read(s.timerfd, &expdir, sizeof(expdir));

                if (bytes_readed == -1) {
                    LOG_CUSTOM_ERRNO("read");
                }

                for (size_t i = 0; i < hmlen(s.active_clients); i++) {
                    int key = s.active_clients[i].key;
                    ptrdiff_t idx = hmgeti(s.clients, key);
                    if (idx == -1) {
                        continue;
                    }
                    Client_State *c = &s.clients[idx].value;
                    LOG_DEBUG("Enviando pacotes de stream para o cliente %d\n", key);
                    audio_server_transmit_packet(c);
                }

                continue;
            }

            if (event_mask & EPOLLRDHUP) {
                audio_server_handle_exit(&s, eventfd);
                continue;
            }

            if (event_mask & EPOLLIN) {
                Request req = {0};
                Response res = {0};
                ssize_t bytes_readed = recv(eventfd, &req, sizeof(req), MSG_NOSIGNAL);

                if (bytes_readed == -1) {
                    LOG_CUSTOM_ERRNO("recv");
                    break;
                }

                switch (req.header.kind) {
                case KIND_LIST:
                    audio_server_handle_list(&s, eventfd, &req, &res);
                    break;
                case KIND_START:
                    audio_server_handle_start(&s, eventfd, &req, &res);
                    break;
                case KIND_STOP:
                    audio_server_handle_stop(&s, eventfd, &req, &res);
                    break;
                case KIND_RESUME:
                    audio_server_handle_resume(&s, eventfd, &req, &res);
                    break;
                default:
                    LOG_ERROR("Invalid request");
                    break;
                }
            }
        }
    }

    LOG_INFO("Closing server");
    audio_server_destroy(&s);
    return 0;
}

void audio_server_transmit_packet(Client_State *c) {
    Response res = {0};
    int sockfd = c->sockfd;
    res.header.kind = KIND_STREAM;

    gettimeofday(&res.header.tv, NULL);
    ssize_t bytes_read = pread(c->fd, res.buf, sizeof(res.buf), c->offset);

    if (bytes_read == -1) {
        LOG_CUSTOM_ERRNO("pread");
        return;
    }

    if (bytes_read == 0) {
        c->playing = 0;
        c->offset = 0;
        return;
    }

    res.header.len = bytes_read;
    ssize_t bytes_written = send(sockfd, &res.header, sizeof(res.header), MSG_NOSIGNAL | MSG_MORE | MSG_DONTWAIT);

    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
        return;
    }

    bytes_written = send(sockfd, res.buf, res.header.len, MSG_NOSIGNAL | MSG_DONTWAIT);

    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
        return;
    }
    if (bytes_written == 0) {
        c->playing = 0;
        return;
    }
    c->offset += bytes_written;
}

int audio_server_create_tcp_socket(const char *addr, int port) {
    struct sockaddr_in sockaddr;
    int fd, ret_val;

    /* Step1: create a TCP socket */
    fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        LOG_CUSTOM_ERRNO("socket");
        return -1;
    }

    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        LOG_CUSTOM_ERRNO("setsockopt");
        return -1;
    }

    /* Initialize the socket address structure */
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);

    int ok = inet_pton(AF_INET, addr, &sockaddr.sin_addr.s_addr);

    if (ok == 0) {
        LOG_ERROR("Invalid -ipaddr format");
        return -1;
    }

    if (ok == -1) {
        LOG_CUSTOM_ERRNO("inet_pton");
        return -1;
    }

    /* Step2: bind the socket to port <port> on the local host */
    ret_val = bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr));

    if (ret_val != 0) {
        LOG_CUSTOM_ERRNO("bind");
        return -1;
    }

    /* Step3: listen for incoming connections */
    ret_val = listen(fd, BACKLOG);

    if (ret_val != 0) {
        LOG_CUSTOM_ERRNO("listen");
        return -1;
    }

    return fd;
}

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port) {
    *s = (Audio_Server){0};

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    s->sockfd = audio_server_create_tcp_socket(addr, tcp_port);

    if (s->sockfd == -1) {
        return 0;
    }

    struct epoll_event ev;
    s->epollfd = epoll_create1(0);

    if (s->epollfd == -1) {
        LOG_CUSTOM_ERRNO("epoll_create1");
        return 0;
    }

    ev.events = EPOLLIN;
    ev.data.fd = s->sockfd;

    if (epoll_ctl(s->epollfd, EPOLL_CTL_ADD, s->sockfd, &ev) == -1) {
        LOG_CUSTOM_ERRNO("epoll_ctl");
        return 0;
    }

    // timer for send streaming packets
    struct itimerspec tspec = {0};
    s->timerfd = timerfd_create(CLOCK_REALTIME, 0);

    if (s->timerfd == -1) {
        LOG_CUSTOM_ERRNO("timerfd_create");
        return 0;
    }

    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        LOG_CUSTOM_ERRNO("clock_gettime");
        return 0;
    }

    tspec.it_interval.tv_sec = 0;
    tspec.it_interval.tv_nsec = 100000000;
    tspec.it_value.tv_sec = now.tv_sec;
    tspec.it_value.tv_nsec = now.tv_nsec;

    if (timerfd_settime(s->timerfd, TFD_TIMER_ABSTIME, &tspec, NULL) == -1) {
        LOG_CUSTOM_ERRNO("timerfd_settime");
        return 0;
    }

    audio_server_load_audios(s);

    return 1;
}

void audio_server_destroy(Audio_Server *s) {
    for (int i = 0; i < hmlen(s->clients); i++) {
        Clients_State item = s->clients[i];
        close(item.key);
        if (item.value.fd > 0) {
            close(item.value.fd);
        }
    }
    hmfree(s->clients);
    hmfree(s->active_clients);
    if (s->timerfd) {
        close(s->timerfd);
    }
    if (s->sockfd > 0) {
        shutdown(s->sockfd, SHUT_RDWR);
        close(s->sockfd);
    }
    if (s->epollfd > 0) {
        close(s->epollfd);
    }
    for (int i = 0; i < shlen(s->audios); i++) {
        free((void *)s->audios[i].value.path);
    }
    shfree(s->audios); // free keys too
}

void audio_server_load_audios(Audio_Server *s) {
    sh_new_strdup(s->audios); // auto strdup key
    DIR *dir = opendir(AUDIODIR);

    if (!dir) {
        LOG_CUSTOM_ERRNO("Error to load " AUDIODIR " directory");
        return;
    }

    struct dirent *de;

    size_t i = 1;
    while ((de = readdir(dir)) != NULL) {
        if (de->d_type != 'd' && suffix_is_audio(de->d_name)) {
            char path[NAME_MAX];
            char *name = de->d_name;
            char key[279]; // the compiler told me that :)
            snprintf(key, sizeof(key), "%ld - %s", i, name);
            strcpy(path, AUDIODIR "/");
            strcat(path, name);
            Audio audio = {.path = strdup(path), .len = strlen(key) + 1};
            shput(s->audios, key, audio);
            i++;
        }
    }

    if (closedir(dir) == -1) {
        LOG_CUSTOM_ERRNO("Error to close " AUDIODIR " directory");
    }

    LOG_INFO("Loaded audios");
}

void audio_server_handle_accept(Audio_Server *s) {
    struct epoll_event ev = {0};
    int fd = accept(s->sockfd, NULL, NULL);

    if (fd == -1) {
        LOG_CUSTOM_ERRNO("accept");
        return;
    }

    Client_State c = {0};
    c.sockfd = fd;
    ev.events = EPOLLRDHUP | EPOLLIN;
    ev.data.fd = fd;

    if (epoll_ctl(s->epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        LOG_CUSTOM_ERRNO("epoll_ctl");
        close(fd);
        return;
    }

    hmput(s->clients, fd, c);
    LOG_INFO("Client connected");
}

void audio_server_handle_exit(Audio_Server *s, int event_sock) {
    ptrdiff_t idx = hmgeti(s->clients, event_sock);
    if (idx != -1) {
        Client_State *c = &s->clients[idx].value;
        if (c->fd > 0) {
            close(c->fd);
        }
        audio_server_client_unset_streaming(s, event_sock);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
        hmdel(s->clients, event_sock);
#pragma GCC diagnostic pop
        if (epoll_ctl(s->epollfd, EPOLL_CTL_DEL, event_sock, NULL) == -1) {
            LOG_CUSTOM_ERRNO("epoll_ctl");
        }
        close(event_sock);
    }
    LOG_INFO("Client disconnected");
}

void audio_server_handle_list(Audio_Server *s, int event_sock, Request *req, Response *res) {
    LOG_INFO("Client request /list");
    res->header.kind = KIND_LIST;
    res->header.code = STATUS_LIST_CONTINUE;
    for (int i = 0; i < shlen(s->audios); i++) {
        res->header.len = s->audios[i].value.len;
        strcpy(res->buf, s->audios[i].key);
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL | MSG_MORE);
        if (bytes_written == -1) {
            LOG_CUSTOM_ERRNO("send");
        }
        bytes_written = send(event_sock, res->buf, res->header.len, MSG_NOSIGNAL | MSG_MORE);
        if (bytes_written == -1) {
            LOG_CUSTOM_ERRNO("send");
        }
    }
    res->header.code = STATUS_LIST_END;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
    }
}

void audio_server_handle_start(Audio_Server *s, int event_sock, Request *req, Response *res) {
    LOG_INFO("Client request /start");
    res->header.kind = KIND_START;
    size_t idx = req->buf - 1;

    if (!(0 <= idx && idx < shlen(s->audios))) {
        res->header.code = STATUS_ERR_NO_FILE;
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
        if (bytes_written == -1) {
            LOG_CUSTOM_ERRNO("send");
        }
        return;
    }

    const char *path = s->audios[idx].value.path;
    int fd = open(path, O_RDONLY);

    if (fd == -1) {
        LOG_ERROR("Failed to open indexed file. Maybe has been deleted.");
        LOG_CUSTOM_ERRNO("open");
        res->header.code = STATUS_ERR_NO_FILE;
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
        if (bytes_written == -1) {
            LOG_CUSTOM_ERRNO("send");
        }
    }

    audio_server_client_set_streaming(s, event_sock, fd);

    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
    }
}

void audio_server_handle_stop(Audio_Server *s, int event_sock, Request *req, Response *res) {
    LOG_INFO("Client request /stop");
    audio_server_client_unset_streaming(s, event_sock);
    res->header.kind = KIND_STOP;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
    }
}

void audio_server_handle_resume(Audio_Server *s, int event_sock, Request *req, Response *res) {
    LOG_INFO("Client request /resume");
    audio_server_client_set_streaming(s, event_sock, 0);
    res->header.kind = KIND_RESUME;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
    }
}

void audio_server_client_set_streaming(Audio_Server *s, int key, int fd) {
    ptrdiff_t idx = hmgeti(s->active_clients, key);

    if (idx == -1) {
        hmput(s->active_clients, key, (Empty){});

        if (hmlen(s->active_clients) == 1) {
            struct epoll_event ev = {0};
            ev.events = EPOLLIN;
            ev.data.fd = s->timerfd;

            if (epoll_ctl(s->epollfd, EPOLL_CTL_ADD, s->timerfd, &ev) == -1) {
                LOG_CUSTOM_ERRNO("epoll_ctl");
            }
        }
    }

    idx = hmgeti(s->clients, key);

    if (idx != -1) {
        Client_State *c = &s->clients[idx].value;
        if (fd > 0) {
            if (c->fd > 0) {
                close(c->fd);
            }
            c->fd = fd;
        }
        c->playing = 1;
    }
}

void audio_server_client_unset_streaming(Audio_Server *s, int key) {
    ptrdiff_t idx = hmgeti(s->active_clients, key);

    if (idx >= 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
        hmdel(s->active_clients, key);
#pragma GCC diagnostic pop

        if (hmlen(s->active_clients) == 0 && epoll_ctl(s->epollfd, EPOLL_CTL_DEL, s->timerfd, NULL) == -1) {
            LOG_CUSTOM_ERRNO("epoll_ctl");
        }
    }

    idx = hmgeti(s->clients, key);
    if (idx != -1) {
        Client_State *c = &s->clients[idx].value;
        c->playing = 0;
    }
}

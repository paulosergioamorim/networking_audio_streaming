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
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
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
    int playing;
    ptrdiff_t audio_idx;
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
    char display_name[279]; // [idx] basename
    int display_name_size;  // strlen(display_name) + 1
    void *buf;              // memory mapping of the file backed by file descriptor fd
    size_t file_size;       // the file size
    int fd;                 // file descriptor
} Audio2;

typedef struct {
    int sockfd;
    int epollfd;
    int timerfd;
    Clients_State *clients;
    Active_Clients *active_clients;
    Audio2 *audios;
} Audio_Server;

void audio_server_transmit_packet(Audio_Server *s, Client_State *c);

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

void audio_server_client_set_streaming(Audio_Server *s, int key, int file_idx);

void audio_server_client_unset_streaming(Audio_Server *s, int key);

void audio2_destroy(Audio2 *a);

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
                    audio_server_transmit_packet(&s, c);
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

#define min(a, b) (((a) < (b)) ? a : b)

void audio_server_transmit_packet(Audio_Server *s, Client_State *c) {
    Response res = {0};
    int sockfd = c->sockfd;
    res.header.kind = KIND_STREAM;
    gettimeofday(&res.header.tv, NULL);
    Audio2 *audio = &s->audios[c->audio_idx];
    size_t nbytes = min(sizeof(res.buf), audio->file_size - c->offset);

    if (nbytes == 0) {
        LOG_INFO("Client %d end streaming", sockfd);
        audio_server_client_unset_streaming(s, sockfd);
        c->audio_idx = -1;
        c->offset = 0;
        return;
    }

    memcpy(res.buf, audio->buf + c->offset, nbytes);
    res.header.code = STATUS_OK;
    res.header.len = nbytes;
    ssize_t bytes_written = send(sockfd, &res.header, sizeof(res.header), MSG_NOSIGNAL | MSG_MORE | MSG_DONTWAIT);

    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
        return;
    }

    bytes_written = send(sockfd, res.buf, nbytes, MSG_NOSIGNAL | MSG_DONTWAIT);

    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
        return;
    }

    c->offset += bytes_written;
    LOG_DEBUG("Enviados %ld bytes de stream para o cliente %d", bytes_written, sockfd);
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
    for (int i = 0; i < arrlen(s->audios); i++) {
        audio2_destroy(&s->audios[i]);
    }
    arrfree(s->audios);
}

void audio_server_load_audios(Audio_Server *s) {
    DIR *dir = opendir(AUDIODIR);

    if (!dir) {
        LOG_CUSTOM_ERRNO("Error to load " AUDIODIR " directory");
        return;
    }

    struct dirent *de;

    size_t i = 1;
    while ((de = readdir(dir)) != NULL) {
        if (!(de->d_type == DT_REG && suffix_is_audio(de->d_name))) {
            continue;
        }

        Audio2 audio = {0};
        char *name = de->d_name;
        const char *display_name_format = "[%ld] %s";
        audio.display_name_size =
            1 + snprintf(audio.display_name, sizeof(audio.display_name), display_name_format, i, name);
        char path[PATH_MAX];
        strcpy(path, AUDIODIR "/");
        strcat(path, name);
        int fd = open(path, O_RDONLY);

        if (fd == -1) {
            LOG_CUSTOM_ERRNO("open");
            continue;
        }

        struct stat st = {0};

        if (fstat(fd, &st) == -1) {
            LOG_CUSTOM_ERRNO("fstat");
            close(fd);
            continue;
        }

        void *buf = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

        if (buf == MAP_FAILED) {
            LOG_CUSTOM_ERRNO("mmap");
            close(fd);
            continue;
        }

        audio.buf = buf;
        audio.fd = fd;
        audio.file_size = st.st_size;
        arrput(s->audios, audio);
        i++;
    }

    if (closedir(dir) == -1) {
        LOG_CUSTOM_ERRNO("Error to close " AUDIODIR " directory");
        return;
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

    if (idx == -1) {
        LOG_WARN("Invalid client index");
        return;
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

    LOG_INFO("Client disconnected");
}

void audio_server_handle_list(Audio_Server *s, int event_sock, Request *req, Response *res) {
    LOG_INFO("Client request /list");
    res->header.kind = KIND_LIST;
    res->header.code = STATUS_LIST_CONTINUE;

    for (int i = 0; i < arrlen(s->audios); i++) {
        res->header.len = s->audios[i].display_name_size;
        strncpy(res->buf, s->audios[i].display_name, sizeof(res->buf));
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

    if (!(0 <= idx && idx < arrlen(s->audios))) {
        res->header.code = STATUS_ERR_NO_FILE;
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
        if (bytes_written == -1) {
            LOG_CUSTOM_ERRNO("send");
        }
        return;
    }

    audio_server_client_set_streaming(s, event_sock, idx);

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
    audio_server_client_set_streaming(s, event_sock, -1);
    res->header.kind = KIND_RESUME;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), MSG_NOSIGNAL);
    if (bytes_written == -1) {
        LOG_CUSTOM_ERRNO("send");
    }
}

void audio_server_client_set_streaming(Audio_Server *s, int key, int audio_idx) {
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

    if (idx == -1) {
        LOG_WARN("Invalid client index");
        return;
    }

    Client_State *c = &s->clients[idx].value;

    if (audio_idx != -1) {
        c->audio_idx = audio_idx;
        c->offset = 0;
    }

    c->playing = 1;
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

    if (idx == -1) {
        LOG_WARN("Invalid client index");
        return;
    }

    Client_State *c = &s->clients[idx].value;
    c->playing = 0;
}

void audio2_destroy(Audio2 *a) {
    if (munmap(a->buf, a->file_size) == -1) {
        LOG_CUSTOM_ERRNO("munmap");
    }
    if (close(a->fd) == -1) {
        LOG_CUSTOM_ERRNO("close");
    }
}

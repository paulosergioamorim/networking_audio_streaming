#include "packets.h"
#include "signals.h"
#include "suffix.h"
#include "utils.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"
#define FLAG_IMPLEMENTATION
#include "flag.h"
#define NOB_IMPLEMENTATION
#include "nob.h"

#define AUDIODIR "./audios"
#define BACKLOG 1024
#define min(a, b) (((a) < (b)) ? a : b)

typedef struct {
    size_t offset;
    ptrdiff_t audio_idx;
    int sockfd;
} Client_State;

typedef struct {
    int key; // the connection socket descriptor
    Client_State value;
} Clients_State;

typedef struct {
} EmptyStruct;

typedef struct {
    int key;
    EmptyStruct value;
} Active_Clients;

typedef struct {
    char display_name[279]; // [idx] basename
    int display_name_size;  // strlen(display_name) + 1
    void *buf;              // memory mapping of the file backed by file descriptor fd
    size_t file_size;       // the file size
    int fd;                 // file descriptor
} Audio2;

typedef struct {
    Sock_Fd sock;
    Epoll_Fd epoll;
    Fd timer;
    Clients_State *clients;
    Active_Clients *active_clients;
    Audio2 *audios;
} Audio_Server;

void audio_server_transmit_packet(Audio_Server *s, Client_State *c);

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

void audio_server_handle_request(Audio_Server *s, int event_sock);

void audio_server_handle_timer(Audio_Server *s, int timerfd);

void audio2_destroy(Audio2 *a);

void audio_server_display_usage(FILE *fp) {
    fprintf(fp, "USAGE: ./server [OPTIONS]\n");
    fprintf(fp, "OPTIONS:\n");
    flag_print_options(fp);
}

int main(int argc, char **argv) {
    Audio_Server s;

    bool *help = flag_bool("help", false, "Print this help");
    char **ipaddr = flag_str("ipaddr", "0.0.0.0", "Provide the serving IP Address");
    uint64_t *port = flag_uint64("port", 8000, "Provide the serving PORT");

    if (!flag_parse(argc, argv)) {
        audio_server_display_usage(stderr);
        return 1;
    }

    if (*help) {
        audio_server_display_usage(stdout);
        return 0;
    }

    if (!audio_server_init(&s, *ipaddr, *port)) {
        return 1;
    }

    nob_log(INFO, "Server listening on %s:%ld", *ipaddr, *port);

    int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];

    while (!signaled) {
        int N = epoll_wait(s.epoll, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            nob_log(ERROR, "epoll_wait");
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            uint32_t event_mask = events[i].events;
            int eventfd = events[i].data.fd;

            if (eventfd == s.sock && event_mask & EPOLLIN) {
                audio_server_handle_accept(&s);
            } else if (eventfd == s.timer && event_mask & EPOLLIN) {
                audio_server_handle_timer(&s, eventfd);
            } else if (event_mask & EPOLLRDHUP) {
                audio_server_handle_exit(&s, eventfd);
            } else if (event_mask & EPOLLIN) {
                audio_server_handle_request(&s, eventfd);
            }
        }
    }

    nob_log(INFO, "Closing server");
    audio_server_destroy(&s);
    return 0;
}

void audio_server_transmit_packet(Audio_Server *s, Client_State *c) {
    Response res = {0};
    int sockfd = c->sockfd;
    Audio2 *audio = &s->audios[c->audio_idx];
    size_t nbytes = min(sizeof(res.buf), audio->file_size - c->offset);

    if (nbytes == 0) {
        nob_log(INFO, "Client %d end streaming", sockfd);
        audio_server_client_unset_streaming(s, sockfd);
        c->audio_idx = -1;
        c->offset = 0;
        return;
    }

    res.header = (Response_Header){
        .kind = KIND_STREAM,
        .code = STATUS_OK,
        .len = nbytes,
    };
    gettimeofday(&res.header.tv, NULL);

    int n = 2;
    struct msghdr msg = {0};
    struct iovec vec[n];
    vec[0].iov_base = &res.header;
    vec[0].iov_len = sizeof(res.header);
    vec[1].iov_base = audio->buf + c->offset;
    vec[1].iov_len = nbytes;
    msg.msg_iov = vec;
    msg.msg_iovlen = n;

    ssize_t bytes_written = sendmsg(sockfd, &msg, 0);

    if (bytes_written == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        return;
    }

    if (bytes_written - sizeof(res.header) < res.header.len) {
        nob_log(WARNING, "Parcial write");
        return;
    }

    c->offset += -sizeof(res.header) + bytes_written;
}

int audio_server_init(Audio_Server *s, const char *addr, int tcp_port) {
    *s = (Audio_Server){0};

    if (signals_sigint_sigaction() == 0)
        goto err;

    s->sock = socket_create_server(addr, tcp_port, BACKLOG);
    if (s->sock == 0)
        goto err;

    if (fd_set_nonblocking(s->sock) == 0)
        goto err;

    s->timer = timer_realtime_create();
    if (s->timer == 0)
        goto err;

    if (fd_set_nonblocking(s->timer) == 0)
        goto err;

    s->epoll = epoll_create1(0);
    if (s->epoll == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        goto err;
    }

    if (epoll_add_fd(s->epoll, s->sock, EPOLLIN | EPOLLET) == 0)
        goto err;

    audio_server_load_audios(s);
    return 1;

err:
    if (s->sock > 0)
        close(s->sock);
    if (s->timer > 0)
        close(s->timer);
    if (s->epoll > 0)
        close(s->epoll);
    return 0;
}

void audio_server_destroy(Audio_Server *s) {
    for (int i = 0; i < hmlen(s->clients); i++) {
        Clients_State item = s->clients[i];
        close(item.key);
    }
    hmfree(s->clients);
    hmfree(s->active_clients);
    close(s->timer);
    close(s->sock);
    close(s->epoll);
    for (int i = 0; i < arrlen(s->audios); i++) {
        audio2_destroy(&s->audios[i]);
    }
    arrfree(s->audios);
}

void audio_server_load_audios(Audio_Server *s) {
    DIR *dir = opendir(AUDIODIR);

    if (!dir) {
        nob_log(ERROR, "Error to load " AUDIODIR " directory");
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
        audio.display_name_size = 1 + snprintf(audio.display_name, sizeof(audio.display_name), "[%ld] %s", i, name);
        char path[PATH_MAX];
        strcpy(path, AUDIODIR "/");
        strcat(path, name);
        int fd = open(path, O_RDONLY);

        if (fd == -1) {
            nob_log(ERROR, "open");
            continue;
        }

        struct stat st = {0};

        if (fstat(fd, &st) == -1) {
            nob_log(ERROR, "fstat");
            close(fd);
            continue;
        }

        void *buf = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

        if (buf == MAP_FAILED) {
            nob_log(ERROR, "mmap");
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
        nob_log(ERROR, "Error to close " AUDIODIR " directory");
        return;
    }

    nob_log(INFO, "Loaded audios");
}

void audio_server_handle_accept(Audio_Server *s) {
    while (true) {
        Sock_Fd sock = socket_accept(s->sock);

        if (sock <= 0)
            break;

        Client_State c = {
            .sockfd = sock,
        };

        epoll_add_fd(s->epoll, sock, EPOLLRDHUP | EPOLLIN | EPOLLET);
        hmput(s->clients, sock, c);
        nob_log(INFO, "Client connected");
    }
}

void audio_server_handle_exit(Audio_Server *s, int event_sock) {
    ptrdiff_t idx = hmgeti(s->clients, event_sock);

    if (idx == -1) {
        nob_log(WARNING, "Invalid client index");
        return;
    }

    audio_server_client_unset_streaming(s, event_sock);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
    hmdel(s->clients, event_sock);
#pragma GCC diagnostic pop
    epoll_del_fd(s->epoll, event_sock);
    close(event_sock);

    nob_log(INFO, "Client disconnected");
}

void audio_server_handle_list(Audio_Server *s, int event_sock, Request *req, Response *res) {
    nob_log(INFO, "Client request /list");

    Response_Header header_end = {
        .kind = KIND_LIST,
        .code = STATUS_LIST_END,
    };

    struct msghdr msg = {0};
    int audios_len = arrlen(s->audios);
    Response_Header headers[audios_len];
    int n = 2 * audios_len + 1;
    struct iovec vec[n];

    for (int i = 0; i < audios_len; i++) {
        headers[i] = (Response_Header){
            .kind = KIND_LIST,
            .code = STATUS_LIST_CONTINUE,
            .len = s->audios[i].display_name_size,
        };
        vec[2 * i].iov_base = &headers[i];
        vec[2 * i].iov_len = sizeof(headers[i]);
        vec[2 * i + 1].iov_base = s->audios[i].display_name;
        vec[2 * i + 1].iov_len = headers[i].len;
    }

    vec[n - 1].iov_base = &header_end;
    vec[n - 1].iov_len = sizeof(header_end);
    msg.msg_iov = vec;
    msg.msg_iovlen = n;

    ssize_t bytes_written = sendmsg(event_sock, &msg, 0);

    if (bytes_written == -1) {
        nob_log(ERROR, TRACE_FMT, TRACE_ARG);
    }
}

void audio_server_handle_start(Audio_Server *s, int event_sock, Request *req, Response *res) {
    nob_log(INFO, "Client request /start");
    res->header.kind = KIND_START;
    size_t idx = req->buf - 1;

    if (!(0 <= idx && idx < arrlen(s->audios))) {
        res->header.code = STATUS_ERR_NO_FILE;
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
        if (bytes_written == -1) {
            nob_log(ERROR, "send");
        }
        return;
    }

    audio_server_client_set_streaming(s, event_sock, idx);

    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, "send");
    }
}

void audio_server_handle_stop(Audio_Server *s, int event_sock, Request *req, Response *res) {
    nob_log(INFO, "Client request /stop");
    audio_server_client_unset_streaming(s, event_sock);
    res->header.kind = KIND_STOP;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, "send");
    }
}

void audio_server_handle_resume(Audio_Server *s, int event_sock, Request *req, Response *res) {
    nob_log(INFO, "Client request /resume");
    audio_server_client_set_streaming(s, event_sock, -1);
    res->header.kind = KIND_RESUME;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, "send");
    }
}

void audio_server_client_set_streaming(Audio_Server *s, int key, int audio_idx) {
    ptrdiff_t idx = hmgeti(s->active_clients, key);

    if (idx == -1) {
        hmput(s->active_clients, key, (EmptyStruct){});
        if (hmlen(s->active_clients) == 1)
            epoll_add_fd(s->epoll, s->timer, EPOLLIN | EPOLLET);
    }

    idx = hmgeti(s->clients, key);

    if (idx == -1) {
        nob_log(WARNING, "Invalid client index");
        return;
    }

    Client_State *c = &s->clients[idx].value;

    if (audio_idx != -1) {
        c->audio_idx = audio_idx;
        c->offset = 0;
    }
}

void audio_server_client_unset_streaming(Audio_Server *s, int key) {
    ptrdiff_t idx = hmgeti(s->active_clients, key);

    if (idx >= 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
        hmdel(s->active_clients, key);
#pragma GCC diagnostic pop
        if (hmlen(s->active_clients) == 0)
            epoll_del_fd(s->epoll, s->timer);
    }
}

void audio2_destroy(Audio2 *a) {
    if (munmap(a->buf, a->file_size) == -1)
        nob_log(ERROR, "munmap");
    if (close(a->fd) == -1)
        nob_log(ERROR, "close");
}

void audio_server_handle_request(Audio_Server *s, int event_sock) {
    while (true) {
        Request req = {0};
        Response res = {0};
        ssize_t bytes_readed = recv(event_sock, &req, sizeof(req), 0);

        if (bytes_readed == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        }

        if (bytes_readed < sizeof(req))
            nob_log(WARNING, "Parcial read");

        if (bytes_readed == 0) {
            audio_server_handle_exit(s, event_sock);
            break;
        }

        switch (req.header.kind) {
        case KIND_LIST:
            audio_server_handle_list(s, event_sock, &req, &res);
            break;
        case KIND_START:
            audio_server_handle_start(s, event_sock, &req, &res);
            break;
        case KIND_STOP:
            audio_server_handle_stop(s, event_sock, &req, &res);
            break;
        case KIND_RESUME:
            audio_server_handle_resume(s, event_sock, &req, &res);
            break;
        default:
            nob_log(ERROR, "Invalid request");
            break;
        }
    }
}

void audio_server_handle_timer(Audio_Server *s, int timerfd) {
    while (true) {
        uint64_t expdir;
        ssize_t bytes_readed = read(s->timer, &expdir, sizeof(expdir));

        if (bytes_readed == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        }

        for (size_t i = 0; i < hmlen(s->active_clients); i++) {
            int key = s->active_clients[i].key;
            ptrdiff_t idx = hmgeti(s->clients, key);
            if (idx == -1)
                continue;
            Client_State *c = &s->clients[idx].value;
            audio_server_transmit_packet(s, c);
        }
    }
}

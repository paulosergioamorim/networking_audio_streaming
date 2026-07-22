#include "debug.h"
#include "io.h"
#include "protocol.h"
#include "signals.h"
#include <dirent.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"
#define FLAG_IMPLEMENTATION
#include "flag.h"
#define NOB_IMPLEMENTATION
#include "nob.h"

#define AUDIODIR "./audios"
#define BACKLOG 1024

typedef struct {
    size_t offset;
    int audio_idx;
    int sock;
} Client;

typedef struct {
    int key;
    Client value;
} Client_Map;

typedef struct {
} None;

typedef struct {
    int key;
    None value;
} Active_Client_Map;

typedef struct {
    void *buf;              // memory mapping of the file backed by file descriptor fd
    int display_name_len;   // strlen(display_name)
    int fd;                 // file descriptor
    size_t file_size;       // the file size
    char display_name[279]; // idx + basename
} Audio2;

typedef struct {
    int sock;
    int epoll;
    int timer;
    Client_Map *clients;
    Active_Client_Map *active_clients;
    Audio2 *audios;
} Audio_Server;

void audio_server_transmit_packet(Audio_Server *s, Client *c);

int audio_server_init(Audio_Server *s, const char *addr, int port);

void audio_server_load_audios(Audio_Server *s);

void audio_server_destroy(Audio_Server *s);

void audio_server_handle_accept(Audio_Server *s);

void audio_server_handle_exit(Audio_Server *s, int event_sock);

void audio_server_handle_list(Audio_Server *s, int event_sock);

void audio_server_handle_start(Audio_Server *s, int event_sock, Request *req, Response *res);

void audio_server_handle_stop(Audio_Server *s, int event_sock, Response *res);

void audio_server_handle_resume(Audio_Server *s, int event_sock, Response *res);

void audio_server_client_set_streaming(Audio_Server *s, int key, int file_idx);

void audio_server_client_unset_streaming(Audio_Server *s, int key);

void audio_server_handle_request(Audio_Server *s, int event_sock);

void audio_server_handle_timer(Audio_Server *s);

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
    sigset_t mask = {0};
    sigfillset(&mask);
    sigdelset(&mask, SIGINT);

    while (1) {
        int N = epoll_pwait(s.epoll, events, MAX_EVENTS, -1, &mask);

        if (N == -1 && errno == EINTR) {
            break;
        }

        if (N == -1) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
            audio_server_destroy(&s);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            uint32_t event_mask = events[i].events;
            int eventfd = events[i].data.fd;

            if (eventfd == s.sock) {
                if (event_mask & EPOLLIN) {
                    audio_server_handle_accept(&s);
                }
            } else if (eventfd == s.timer) {
                if (event_mask & EPOLLIN) {
                    audio_server_handle_timer(&s);
                }
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

void audio_server_transmit_packet(Audio_Server *s, Client *c) {
    Response res = {0};
    int sock = c->sock;
    Audio2 *audio = &s->audios[c->audio_idx];
    size_t nbytes = audio->file_size - c->offset;
    if (nbytes > sizeof(res.data)) {
        nbytes = sizeof(res.data);
    }

    if (nbytes == 0) {
        nob_log(INFO, "Client %d end streaming", sock);
        audio_server_client_unset_streaming(s, sock);
        c->audio_idx = -1;
        c->offset = 0;
        return;
    }

    res.header = response_header_build(KIND_STREAM, STATUS_OK, nbytes);
    int n = 2;
    struct msghdr msg = {0};
    struct iovec vec[n];
    vec[0].iov_base = &res.header;
    vec[0].iov_len = sizeof(res.header);
    vec[1].iov_base = audio->buf + c->offset;
    vec[1].iov_len = nbytes;
    msg.msg_iov = vec;
    msg.msg_iovlen = n;

    ssize_t bytes_written = sendmsg(sock, &msg, 0);

    if (bytes_written == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        return;
    }

    if (bytes_written - sizeof(res.header) < nbytes) {
        nob_log(WARNING, "Partial write");
        return;
    }

    c->offset += -sizeof(res.header) + bytes_written;
}

int audio_server_init(Audio_Server *s, const char *addr, int port) {
    *s = (Audio_Server){0};

    struct sigaction sa = {
        .sa_handler = &sigint_handler,
    };

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        goto err_sigaction;
    }

    s->sock = socket_create_server(addr, port, BACKLOG);
    if (s->sock == 0) {
        goto err_sock;
    }

    s->timer = timer_realtime_create(0, 100000000);
    if (s->timer == 0) {
        goto err_timer;
    }

    s->epoll = epoll_create1(0);
    if (s->epoll == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_epoll;
    }

    if (epoll_add_fd(s->epoll, s->sock, EPOLLIN | EPOLLET) == 0) {
        goto err_epoll_add;
    }

    audio_server_load_audios(s);
    return 1;

err_epoll_add:
    close(s->epoll);
err_epoll:
    close(s->timer);
err_timer:
    close(s->sock);
err_sock:
err_sigaction:
    return 0;
}

void audio_server_destroy(Audio_Server *s) {
    for (int i = 0; i < hmlen(s->clients); i++) {
        Client *item = &s->clients[i].value;
        close(item->sock);
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

    struct dirent *de = NULL;
    int i = 1;
    while ((de = readdir(dir)) != NULL) {
        String_View mp3_ext = sv_from_cstr(".mp3");
        String_View file_name = sv_from_cstr(de->d_name);

        if (de->d_type != DT_REG || !sv_ends_with(file_name, mp3_ext)) {
            continue;
        }

        Audio2 audio = {0};
        char *name = de->d_name;
        audio.display_name_len = snprintf(audio.display_name, sizeof(audio.display_name), "[%d] %s", i, name);
        char path[PATH_MAX];
        strcpy(path, AUDIODIR "/");
        strcat(path, name);
        int fd = open(path, O_RDONLY);

        if (fd == -1) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
            continue;
        }

        struct stat st = {0};

        if (fstat(fd, &st) == -1) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
            close(fd);
            continue;
        }

        void *buf = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

        if (buf == MAP_FAILED) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
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
    while (1) {
        Client c = {0};
        c.sock = accept4(s->sock, NULL, NULL, SOCK_NONBLOCK);

        if (c.sock == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        }

        epoll_add_fd(s->epoll, c.sock, EPOLLRDHUP | EPOLLIN | EPOLLET);
        hmput(s->clients, c.sock, c);
        nob_log(INFO, "Client connected");
    }
}

void audio_server_handle_exit(Audio_Server *s, int event_sock) {
    int idx = hmgeti(s->clients, event_sock);

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

void audio_server_handle_list(Audio_Server *s, int event_sock) {
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
            .len = s->audios[i].display_name_len,
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
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
}

void audio_server_handle_start(Audio_Server *s, int event_sock, Request *req, Response *res) {
    nob_log(INFO, "Client request /start");
    res->header.kind = KIND_START;
    int idx = ntohl(req->buf) - 1;

    if (idx >= arrlen(s->audios)) {
        res->header.code = STATUS_ERR_NO_FILE;
        ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
        if (bytes_written == -1) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        }
        return;
    }

    audio_server_client_set_streaming(s, event_sock, idx);

    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
}

void audio_server_handle_stop(Audio_Server *s, int event_sock, Response *res) {
    nob_log(INFO, "Client request /stop");
    audio_server_client_unset_streaming(s, event_sock);
    res->header.kind = KIND_STOP;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
}

void audio_server_handle_resume(Audio_Server *s, int event_sock, Response *res) {
    nob_log(INFO, "Client request /resume");
    audio_server_client_set_streaming(s, event_sock, -1);
    res->header.kind = KIND_RESUME;
    res->header.code = STATUS_OK;
    ssize_t bytes_written = send(event_sock, &res->header, sizeof(res->header), 0);
    if (bytes_written == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
}

void audio_server_client_set_streaming(Audio_Server *s, int key, int audio_idx) {
    int idx = hmgeti(s->active_clients, key);

    if (idx == -1) {
        hmput(s->active_clients, key, (None){});
        if (hmlen(s->active_clients) == 1) {
            epoll_add_fd(s->epoll, s->timer, EPOLLIN | EPOLLET);
        }
    }

    idx = hmgeti(s->clients, key);

    if (idx == -1) {
        nob_log(WARNING, "Invalid client index");
        return;
    }

    Client *c = &s->clients[idx].value;

    if (audio_idx != -1) {
        c->audio_idx = audio_idx;
        c->offset = 0;
    }
}

void audio_server_client_unset_streaming(Audio_Server *s, int key) {
    int idx = hmgeti(s->active_clients, key);

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
    if (munmap(a->buf, a->file_size) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
    if (close(a->fd) == -1) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
    }
}

void audio_server_handle_request(Audio_Server *s, int event_sock) {
    while (1) {
        Request req = {0};
        Response res = {0};
        ssize_t bytes_read = recv(event_sock, &req, sizeof(req), 0);

        if (bytes_read == 0) {
            req.header.kind = KIND_EXIT;
        } else if (bytes_read == -1) {
            if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
            }
            return;
        } else if (bytes_read < (ssize_t)sizeof(req)) {
            nob_log(WARNING, "Partial read"); // TODO: make this better :)
        }

#define REQUEST_HANDLERS                                                                                               \
    X(KIND_EXIT, audio_server_handle_exit, s, event_sock)                                                              \
    X(KIND_LIST, audio_server_handle_list, s, event_sock)                                                              \
    X(KIND_START, audio_server_handle_start, s, event_sock, &req, &res)                                                \
    X(KIND_STOP, audio_server_handle_stop, s, event_sock, &res)                                                        \
    X(KIND_RESUME, audio_server_handle_resume, s, event_sock, &res)
        switch (req.header.kind) {
#define X(kind, handler, ...)                                                                                          \
    case kind:                                                                                                         \
        handler(__VA_ARGS__);                                                                                          \
        break;
            REQUEST_HANDLERS
#undef X
        default:
            nob_log(ERROR, "Invalid request");
            break;
        }
    }
}

void audio_server_handle_timer(Audio_Server *s) {
    while (1) {
        uint64_t expdir;
        ssize_t bytes_read = read(s->timer, &expdir, sizeof(expdir));

        if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        }

        for (int i = 0; i < hmlen(s->active_clients); i++) {
            int key = s->active_clients[i].key;
            int idx = hmgeti(s->clients, key);
            if (idx == -1) {
                continue;
            }
            Client *c = &s->clients[idx].value;
            audio_server_transmit_packet(s, c);
        }
    }
}

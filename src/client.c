#include "packets.h"
#include "queue.h"
#include "signals.h"
#include "utils.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/vlc.h>

#define FLAG_IMPLEMENTATION
#include "flag.h"

#define NOB_IMPLEMENTATION
#include "nob.h"

#define KB(x) ((1 << 10) * x)
#define HELP_MSG                                                                                                       \
    ("|=======================================|\n"                                                                     \
     "| /help         -> more info            |\n"                                                                     \
     "| /list         -> list avaliable songs |\n"                                                                     \
     "| /start <idx>  -> start streaming file |\n"                                                                     \
     "| /stats        -> show metrics         |\n"                                                                     \
     "| /stop         -> stop streaming       |\n"                                                                     \
     "| /reset        -> reset metrics        |\n"                                                                     \
     "| /resume       -> resume streaming     |\n"                                                                     \
     "| /exit or ^C   -> to exit              |\n"                                                                     \
     "|=======================================|\n")
#define LIST_LINE_HORIZONTAL ("|----------------------------------------------------------------------------------|\n")

typedef struct {
    unsigned long min_us;
    unsigned long max_us;
    unsigned long sum_us;
    unsigned long count;
} Delay_Stats;

typedef struct {
    libvlc_instance_t *vlc_instance;
    libvlc_media_player_t *vlc_mp;
    Delay_Stats stats;
    Queue queue;
    int sock;
    bool kind_list_start;
    bool is_playing;
    bool has_playered;
} Audio_Client;

int open_cb(void *opaque, void **datap, uint64_t *sizep);

ssize_t read_cb(void *opaque, unsigned char *buf, size_t len);

int seek_cb(void *opaque, uint64_t offset);

void close_cb(void *opaque);

void audio_client_stats_reset(Audio_Client *s);

void audio_client_stats_update(Audio_Client *s, Response *res);

void audio_client_stats_print(const Delay_Stats *s);

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port);

void audio_client_destroy(Audio_Client *c);

void audio_client_handle_start(Audio_Client *c);

void audio_client_handle_exit(Audio_Client *c);

void audio_client_handle_response(Audio_Client *c);

ssize_t recv_exact(int fd, void *buf, int n, int flags);

Message_Kind audio_client_parse_str_to_enum(const char *str);

void audio_client_display_usage(FILE *fp) {
    fprintf(fp, "USAGE: ./client [OPTIONS]\n");
    fprintf(fp, "OPTIONS:\n");
    flag_print_options(fp);
}

int main(int argc, char **argv) {
    Audio_Client c;

    bool *help = flag_bool("help", false, "Print this help");
    char **ipaddr = flag_str("ipaddr", "0.0.0.0", "Provide the server IP Address");
    int *port = (int *)flag_uint64("port", 8000, "Provide the server PORT");

    if (!flag_parse(argc, argv)) {
        audio_client_display_usage(stderr);
        return 1;
    }

    if (*help) {
        audio_client_display_usage(stdout);
        return 0;
    }

    if (!audio_client_init(&c, *ipaddr, *port)) {
        return 1;
    }

    printf("/help for more info\n");

    int nfds = 2;
    struct pollfd pollfds[] = {
        (struct pollfd){.fd = STDIN_FILENO, .events = POLLIN},
        (struct pollfd){.fd = c.sock, .events = POLLIN | POLLRDHUP},
    };

    while (!signaled) {
        // use poll for suport regular files as standard input
        if (poll(pollfds, nfds, -1) == -1 && errno != EINTR) {
            nob_log(ERROR, TRACE_FMT, TRACE_ARG);
            audio_client_destroy(&c);
            return 1;
        }

        for (int i = 0; i < nfds; i++) {
            int revents = pollfds[i].revents;
            if (!revents)
                continue;
            int fd = pollfds[i].fd;

            if (fd == STDIN_FILENO && revents & POLLIN) {
                char prompt[NAME_MAX] = {0};
                fgets(prompt, sizeof(prompt), stdin);
                prompt[strcspn(prompt, "\n")] = '\0';
                if (*prompt == '\0')
                    continue;

                Message_Kind kind = audio_client_parse_str_to_enum(prompt);

                switch (kind) {
                case KIND_NONE:
                    printf("Invalid command\n");
                    break;
                case KIND_EXIT:
                    signaled = 1;
                    break;
                case KIND_HELP:
                    printf(HELP_MSG);
                    break;
                case KIND_STATS:
                    audio_client_stats_print(&c.stats);
                    break;
                case KIND_RESET:
                    audio_client_stats_reset(&c);
                    break;
                default:
                    break;
                }

                Request req = {0};
                req.header.kind = kind;

                if (kind == KIND_START) {
                    char *idx_str = prompt + sizeof("/start ") - 1;
                    long idx = atol(idx_str);
                    if (idx <= 0) {
                        printf("Invalid audio index\n");
                        continue;
                    }
                    req.buf = idx;
                }

                if ((kind == KIND_STOP && c.is_playing == 0) ||
                    (kind == KIND_RESUME && (c.has_playered == 0 || c.is_playing == 1))) {
                    break;
                }

                ssize_t bytes_written = send(c.sock, &req, sizeof(req), 0);
                if (bytes_written == -1) {
                    nob_log(ERROR, TRACE_FMT, TRACE_ARG);
                    continue;
                }
            }

            if (fd == c.sock) {
                if (revents & POLLRDHUP) {
                    nob_log(INFO, "Server has been closed. Exiting...");
                    signaled = 1;
                    break;
                }

                if (revents & POLLIN)
                    audio_client_handle_response(&c);
            }
        }
    }

    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port) {
    *c = (Audio_Client){0};

    c->sock = socket_create_client(server_addr, server_tcp_port);
    if (c->sock == 0)
        goto err;

    if (queue_init(&c->queue, KB(32)) == 0)
        goto err;

    const char *args[] = {"--quiet"};
    c->vlc_instance = libvlc_new(1, args);

    if (!c->vlc_instance) {
        nob_log(ERROR, "libvlc_new");
        goto err;
    }

    libvlc_media_t *vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);

    if (!vlc_media) {
        nob_log(ERROR, "libvlc_media_new_callbacks");
        goto err;
    }

    c->vlc_mp = libvlc_media_player_new_from_media(vlc_media);

    if (!c->vlc_mp) {
        nob_log(ERROR, "libvlc_media_player_new_from_media");
        goto err;
    }

    libvlc_media_release(vlc_media);

    if (signals_sigint_sigaction() == 0)
        goto err;

    return 1;

err:
    if (c->sock > 0)
        close(c->sock);
    if (c->vlc_instance)
        libvlc_release(c->vlc_instance);
    if (c->vlc_mp)
        libvlc_media_player_release(c->vlc_mp);
    if (c->queue.is_active)
        queue_destroy(&c->queue);
    return 0;
}

void audio_client_destroy(Audio_Client *c) {
    queue_abort(&c->queue);
    libvlc_media_player_stop(c->vlc_mp);
    libvlc_media_player_release(c->vlc_mp);
    libvlc_release(c->vlc_instance);
    audio_client_handle_exit(c);
    c->is_playing = 0;
    queue_destroy(&c->queue);
}

void audio_client_handle_exit(Audio_Client *c) {
    close(c->sock);
}

Message_Kind audio_client_parse_str_to_enum(const char *str) {
    if (strcmp(str, "/list") == 0) {
        return KIND_LIST;
    }
    if (strncmp(str, "/start", 6) == 0) {
        return KIND_START;
    }
    if (strcmp(str, "/stop") == 0) {
        return KIND_STOP;
    }
    if (strcmp(str, "/resume") == 0) {
        return KIND_RESUME;
    }
    if (strcmp(str, "/exit") == 0) {
        return KIND_EXIT;
    }
    if (strcmp(str, "/help") == 0) {
        return KIND_HELP;
    }
    if (strcmp(str, "/stats") == 0) {
        return KIND_STATS;
    }
    if (strcmp(str, "/reset") == 0) {
        return KIND_RESET;
    }
    return KIND_NONE;
}

void audio_client_handle_start(Audio_Client *c) {
    c->is_playing = 0;
    c->has_playered = 1;

    // free all libvlc threads
    pthread_mutex_lock(&c->queue.mu);
    c->queue.is_active = 0;
    pthread_cond_broadcast(&c->queue.cond_empty);
    pthread_mutex_unlock(&c->queue.mu);

    // stop the libvlc player
    libvlc_media_player_stop(c->vlc_mp);

    // reset the circular queue
    queue_clear(&c->queue);

    // activate the queue
    pthread_mutex_lock(&c->queue.mu);
    c->queue.is_active = 1;
    pthread_mutex_unlock(&c->queue.mu);

    c->is_playing = 1;
    libvlc_media_player_play(c->vlc_mp);
}

void audio_client_stats_reset(Audio_Client *s) {
    s->stats.min_us = ULONG_MAX;
    s->stats.max_us = 0;
    s->stats.sum_us = 0;
    s->stats.count = 0;
}

void audio_client_stats_update(Audio_Client *s, Response *res) {
    struct timeval now;
    gettimeofday(&now, NULL);
    unsigned long delay_us =
        (1000000 * now.tv_sec + now.tv_usec) - (1000000 * res->header.tv.tv_sec + res->header.tv.tv_usec);
    if (delay_us < s->stats.min_us)
        s->stats.min_us = delay_us;
    if (delay_us > s->stats.max_us)
        s->stats.max_us = delay_us;
    s->stats.sum_us += delay_us;
    s->stats.count++;
}

void audio_client_stats_print(const Delay_Stats *s) {
    if (s->count == 0) {
        printf("|=======================================|\n"
               "|          No packets received          |\n"
               "|=======================================|\n");
        return;
    }
    printf("|=====================================|\n"
           "| packets : %-5lu                     |\n"
           "| min     : us %-5lu                  |\n"
           "| max     : us %-5lu                  |\n"
           "| avg     : us %-5lu                  |\n"
           "|=====================================|\n",
           s->count, s->min_us, s->max_us, s->sum_us / s->count);
}

void audio_client_handle_response(Audio_Client *c) {
    while (true) {
        Response res = {0};
        ssize_t bytes_readed = recv(c->sock, &res.header, sizeof(res.header), 0);
        Message_Kind kind = res.header.kind;

        if (bytes_readed == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            nob_log(ERROR, TRACE_FMT, TRACE_ARG);
        }

        if (bytes_readed == 0)
            break;

        if (kind == KIND_LIST) {
            if (res.header.code == STATUS_LIST_END) {
                printf(LIST_LINE_HORIZONTAL);
                c->kind_list_start = 0;
                continue;
            }
            bytes_readed = recv(c->sock, res.buf, res.header.len, 0);

            if (bytes_readed == -1) {
                nob_log(ERROR, "recv");
                continue;
            }

            if (!c->kind_list_start) {
                printf(LIST_LINE_HORIZONTAL);
                c->kind_list_start = 1;
            }

            printf("| %s %*s |\n", res.buf, 80 - (int)res.header.len, " ");
            continue;
        }

        if (kind == KIND_START) {
            if (res.header.code == STATUS_ERR_NO_FILE) {
                printf("No audio file\n");
                break;
            }
            audio_client_handle_start(c);
            printf("Start audio streaming...\n");
            continue;
        }

        if (kind == KIND_STOP) {
            c->is_playing = 0;
            libvlc_media_player_pause(c->vlc_mp);
            printf("Stop audio streaming...\n");
            continue;
        }

        if (kind == KIND_RESUME) {
            c->is_playing = 1;
            libvlc_media_player_play(c->vlc_mp);
            printf("Resume audio streaming...\n");
            continue;
        }

        if (kind == KIND_STREAM) {
            ssize_t steps = recv_exact(c->sock, res.buf, res.header.len, 0);
            if (steps > 1)
                nob_log(WARNING, "Parcial read in %ld steps", steps);
            audio_client_stats_update(c, &res);
            queue_enqueue(&c->queue, (unsigned char *)res.buf, res.header.len);
            continue;
        }

        nob_log(WARNING, "Invalid response");
    }
}

ssize_t recv_exact(int fd, void *buf, int n, int flags) {
    size_t steps = 0, bytes_readed = 0;
    for (size_t i = 0; i < n; i += bytes_readed, steps++) {
        bytes_readed = recv(fd, buf + i, n - i, flags);
        if (bytes_readed == -1) {
            if (!(errno == EAGAIN || errno == EWOULDBLOCK))
                nob_log(ERROR, TRACE_FMT, TRACE_ARG);
            bytes_readed = 0;
        }
    }
    return steps;
}

int open_cb(void *opaque, void **datap, uint64_t *sizep) {
    *datap = opaque;
    *sizep = UINT64_MAX;
    return 0;
}

ssize_t read_cb(void *opaque, unsigned char *buf, size_t len) {
    Audio_Client *c = (Audio_Client *)opaque;
    Queue *q = &c->queue;
    return queue_dequeue(q, buf, len);
}

int seek_cb(void *opaque, uint64_t offset) {
    return -1;
}

void close_cb(void *opaque) {
}

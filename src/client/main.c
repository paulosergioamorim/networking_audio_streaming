#include "debug.h"
#include "io.h"
#include "protocol.h"
#include "queue.h"
#include "signals.h"
#include <errno.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <time.h>
#include <vlc/vlc.h>

#define FLAG_IMPLEMENTATION
#include "flag.h"

#define NOB_IMPLEMENTATION
#include "nob.h"

#define KB(x) ((1 << 10) * x)
#define HELP_MSG                                                                                                       \
    ("|=======================================|\n"                                                                     \
     "| /help         -> more info            |\n"                                                                     \
     "| /list         -> list available songs |\n"                                                                     \
     "| /start <idx>  -> start streaming file |\n"                                                                     \
     "| /stats        -> show metrics         |\n"                                                                     \
     "| /stop         -> stop streaming       |\n"                                                                     \
     "| /reset        -> reset metrics        |\n"                                                                     \
     "| /resume       -> resume streaming     |\n"                                                                     \
     "| /exit or ^C   -> to exit              |\n"                                                                     \
     "|=======================================|\n")
#define LIST_LINE_HORIZONTAL ("|===================================================================================|\n")

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
    int kind_list_start;
    int is_playing;
    int has_playered;
} Audio_Client;

int open_cb(void *opaque, void **datap, uint64_t *sizep);

ssize_t read_cb(void *opaque, unsigned char *buf, size_t len);

int seek_cb(void *opaque, uint64_t offset);

void close_cb(void *opaque);

void audio_client_stats_reset(Audio_Client *s);

void audio_client_stats_update(Audio_Client *s, Response *res);

void audio_client_stats_print(Audio_Client *c);

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port);

void audio_client_destroy(Audio_Client *c);

void audio_client_handle_start(Audio_Client *c);

void audio_client_handle_exit(Audio_Client *c);

void audio_client_handle_response(Audio_Client *c);

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
    uint64_t *port = flag_uint64("port", 8000, "Provide the server PORT");

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

    sigset_t mask = {0};
    sigfillset(&mask);
    sigdelset(&mask, SIGINT);

    while (1) {
        // use poll for suport regular files as standard input
        int readyfds = ppoll(pollfds, nfds, NULL, &mask);

        if (readyfds == -1 && errno == EINTR) {
            break;
        }

        if (readyfds == -1) {
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
            audio_client_destroy(&c);
            return 1;
        }

        for (int i = 0; i < nfds; i++) {
            short revents = pollfds[i].revents;
            if (!revents) {
                continue;
            }
            int fd = pollfds[i].fd;

            if (fd == STDIN_FILENO && revents & POLLIN) {
                char prompt[NAME_MAX] = {0};
                fgets(prompt, sizeof(prompt), stdin);
                prompt[strcspn(prompt, "\n")] = '\0';
                if (*prompt == '\0') {
                    continue;
                }

                Message_Kind kind = audio_client_parse_str_to_enum(prompt);

                switch (kind) {
                case KIND_NONE:
                    printf("Invalid command\n");
                    break;
                case KIND_EXIT:
                    goto exit;
                case KIND_HELP:
                    printf(HELP_MSG);
                    break;
                case KIND_STATS:
                    audio_client_stats_print(&c);
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
                    int idx = 0;
                    if (sscanf(prompt, "/start %d", &idx) != 1 || idx <= 0) {
                        printf("Invalid audio index\n");
                        continue;
                    }
                    req.buf = htonl(idx);
                }

                if ((kind == KIND_STOP && c.is_playing == 0) ||
                    (kind == KIND_RESUME && (c.has_playered == 0 || c.is_playing == 1))) {
                    break;
                }

                ssize_t bytes_written = send(c.sock, &req, sizeof(req), 0);
                if (bytes_written == -1) {
                    nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
                    continue;
                }
            }

            if (fd == c.sock) {
                if (revents & POLLRDHUP) {
                    nob_log(INFO, "Server has been closed. Exiting...");
                    goto exit;
                }
                if (revents & POLLIN) {
                    audio_client_handle_response(&c);
                }
            }
        }
    }

exit:
    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port) {
    libvlc_media_t *vlc_media = NULL;
    const char *args[] = {"--quiet"};
    *c = (Audio_Client){0};

    struct sigaction sa = {0};
    sa.sa_handler = &sigint_handler;

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        goto err_sigaction;
    }

    c->sock = socket_create_client(server_addr, server_tcp_port);
    if (c->sock == 0) {
        goto err_sock;
    }

    if (queue_init(&c->queue, KB(32)) == 0) {
        goto err_queue;
    }

    c->vlc_instance = libvlc_new(1, args);

    if (!c->vlc_instance) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_vlc_instance;
    }

    vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);

    if (!vlc_media) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_vlc_media;
    }

    c->vlc_mp = libvlc_media_player_new_from_media(vlc_media);
    libvlc_media_release(vlc_media);

    if (!c->vlc_mp) {
        nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        goto err_vlc_mp;
    }

    return 1;

err_vlc_mp:
    libvlc_media_player_release(c->vlc_mp);
err_vlc_media:
    libvlc_release(c->vlc_instance);
err_vlc_instance:
    queue_destroy(&c->queue);
err_queue:
    close(c->sock);
err_sock:
err_sigaction:
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
#define COMMANDS                                                                                                       \
    X(KIND_LIST, "/list", sizeof("/list"))                                                                             \
    X(KIND_START, "/start ", sizeof("/start"))                                                                         \
    X(KIND_STOP, "/stop", sizeof("/stop"))                                                                             \
    X(KIND_RESUME, "/resume", sizeof("/resume"))                                                                       \
    X(KIND_EXIT, "/exit", sizeof("/exit"))                                                                             \
    X(KIND_HELP, "/help", sizeof("/help"))                                                                             \
    X(KIND_STATS, "/stats", sizeof("/stats"))                                                                          \
    X(KIND_RESET, "/reset", sizeof("/reset"))
#define X(kind, match, len)                                                                                            \
    if (strncmp(str, match, len) == 0) {                                                                               \
        return kind;                                                                                                   \
    }
    COMMANDS
#undef X
    return KIND_NONE;
}

void audio_client_handle_start(Audio_Client *c) {
    c->is_playing = 0;
    c->has_playered = 1;
    // free all libvlc threads
    queue_abort(&c->queue);
    // stop the libvlc player
    libvlc_media_player_stop(c->vlc_mp);
    // reset the circular queue
    queue_clear(&c->queue);
    // activate the queue
    c->queue.is_active = 1;
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

void audio_client_stats_print(Audio_Client *c) {
    Delay_Stats *s = &c->stats;
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
    while (1) {
        Response res = {0};
        ssize_t bytes_read = recv(c->sock, &res.header, sizeof(res.header), 0);
        Message_Kind kind = res.header.kind;

        if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
        }

        if (bytes_read == 0) {
            break;
        }

        if (kind == KIND_LIST) {
            if (res.header.code == STATUS_LIST_END) {
                printf(LIST_LINE_HORIZONTAL);
                c->kind_list_start = 0;
                continue;
            }
            bytes_read = recv(c->sock, res.data, res.header.len, 0);

            if (bytes_read == -1) {
                nob_log(ERROR, DEBUG_Fmt, DEBUG_Arg);
                continue;
            }

            if (!c->kind_list_start) {
                printf(LIST_LINE_HORIZONTAL);
                c->kind_list_start = 1;
            }

            printf("| %.*s %*s |\n", res.header.len, res.data, 80 - res.header.len, " ");
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
            queue_enqueue2(&c->queue, c->sock, ntohl(res.header.len));
            audio_client_stats_update(c, &res);
            continue;
        }

        nob_log(WARNING, "Invalid response");
    }
}

int open_cb(void *opaque, void **datap, uint64_t *sizep) {
    *datap = opaque;
    *sizep = UINT64_MAX;
    return 0;
}

ssize_t read_cb(void *opaque, unsigned char *buf, size_t len) {
    Audio_Client *c = (Audio_Client *)opaque;
    Queue *q = &c->queue;
    return queue_dequeue2(q, buf, len);
}

int seek_cb(void *opaque, uint64_t offset) {
    NOB_UNUSED(opaque);
    NOB_UNUSED(offset);
    return -1;
}

void close_cb(void *opaque) {
    NOB_UNUSED(opaque);
}

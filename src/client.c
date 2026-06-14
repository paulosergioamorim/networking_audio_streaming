#include "logger.h"
#include "packets.h"
#include "queue.h"
#include "signals.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <limits.h>
#include <linux/limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/vlc.h>

#define FLAG_IMPLEMENTATION
#include "flag.h"

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
    int epollfd;
    int sockfd;
    libvlc_instance_t *vlc_instance;
    libvlc_media_player_t *vlc_mp;
    Queue queue;
    int is_playing;
    int has_playered;
    Delay_Stats stats;
} Audio_Client;

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

void audio_client_stats_reset(Delay_Stats *s);

void audio_client_stats_update(Delay_Stats *s, unsigned long delay_us);

void audio_client_stats_print(const Delay_Stats *s);

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port);

int audio_client_create_tcp_socket(const char *server_addr, int port);

void audio_client_destroy(Audio_Client *c);

void audio_client_handle_start(Audio_Client *c);

void audio_client_handle_exit(Audio_Client *c);

Message_Kind audio_client_parse_str_to_enum(const char *str);

void audio_client_display_usage(FILE *fp) {
    fprintf(fp, "USAGE: ./client [OPTIONS]\n");
    fprintf(fp, "OPTIONS:\n");
    flag_print_options(fp);
}

int main(int argc, char **argv) {
    Audio_Client c;

    logger_initConsoleLogger(stdout);

    bool *help = flag_bool("help", false, "Print this help");
    char **ipaddr = flag_str("ipaddr", "0.0.0.0", "Provide the server IP Address");
    int *port = (int *)flag_uint64("port", 8000, "Provide the server PORT");
    bool *debug = flag_bool("debug", false, "Print debug levels");

    if (!flag_parse(argc, argv)) {
        audio_client_display_usage(stderr);
        return 1;
    }

    if (*help) {
        audio_client_display_usage(stdout);
        return 0;
    }

    if (*debug) {
        logger_setLevel(LogLevel_DEBUG);
    }

    if (!audio_client_init(&c, *ipaddr, *port)) {
        audio_client_destroy(&c);
        return 1;
    }

    printf("/help for more info\n");

    const int MAX_EVENTS = 10;
    int N = 0;
    struct epoll_event events[MAX_EVENTS];
    int kind_list_start = 0;

    while (!signaled) {
        N = epoll_wait(c.epollfd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            LOG_CUSTOM_ERRNO("epoll_wait");
            audio_client_destroy(&c);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            uint32_t event_mask = events[i].events;
            int event_sock = events[i].data.fd;

            if (event_sock == STDIN_FILENO && event_mask & EPOLLIN) {
                char prompt[NAME_MAX] = {0};
                read(STDIN_FILENO, prompt, sizeof(prompt));
                char *ptr = strchr(prompt, '\n');

                if (ptr) {
                    *ptr = '\0';
                }
                if (*prompt == '\0') {
                    continue;
                }

                Message_Kind kind = audio_client_parse_str_to_enum(prompt);

                if (kind == KIND_NONE) {
                    printf("Invalid command\n");
                    continue;
                }

                if (kind == KIND_EXIT) {
                    signaled = 1;
                    break;
                }

                if (kind == KIND_HELP) {
                    printf(HELP_MSG);
                    continue;
                }

                if (kind == KIND_STATS) {
                    audio_client_stats_print(&c.stats);
                    continue;
                }

                if (kind == KIND_RESET) {
                    audio_client_stats_reset(&c.stats);
                    continue;
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

                ssize_t bytes_written = send(c.sockfd, &req, sizeof(req), MSG_NOSIGNAL);
                if (bytes_written == -1) {
                    LOG_CUSTOM_ERRNO("send");
                    continue;
                }
            }

            if (event_sock == c.sockfd) {
                if (event_mask & EPOLLRDHUP) {
                    LOG_INFO("Server has been closed. Exiting...");
                    signaled = 1;
                    break;
                }

                if (event_mask & EPOLLIN) {
                    Response res = {0};
                    ssize_t bytes_readed = recv(c.sockfd, &res.header, sizeof(res.header), MSG_NOSIGNAL);
                    Message_Kind kind = res.header.kind;

                    if (bytes_readed == -1) {
                        LOG_CUSTOM_ERRNO("recv");
                        break;
                    }

                    if (kind == KIND_LIST) {
                        if (res.header.code == STATUS_LIST_END) {
                            printf(LIST_LINE_HORIZONTAL);
                            kind_list_start = 0;
                            continue;
                        }
                        bytes_readed = recv(c.sockfd, res.buf, res.header.len, MSG_NOSIGNAL);

                        if (bytes_readed == -1) {
                            LOG_CUSTOM_ERRNO("recv");
                            continue;
                        }

                        if (!kind_list_start) {
                            printf(LIST_LINE_HORIZONTAL);
                            kind_list_start = 1;
                        }

                        printf("| %s %*s |\n", res.buf, 80 - (int)res.header.len, " ");
                        continue;
                    }

                    if (kind == KIND_START) {
                        if (res.header.code == STATUS_ERR_NO_FILE) {
                            printf("No audio file\n");
                            break;
                        }
                        audio_client_handle_start(&c);
                        printf("Start audio streaming...\n");
                        continue;
                    }

                    if (kind == KIND_STOP) {
                        c.is_playing = 0;
                        libvlc_media_player_pause(c.vlc_mp);
                        printf("Stop audio streaming...\n");
                        continue;
                    }

                    if (kind == KIND_RESUME) {
                        c.is_playing = 1;
                        libvlc_media_player_play(c.vlc_mp);
                        printf("Resume audio streaming...\n");
                        continue;
                    }

                    if (kind == KIND_STREAM) {
                        bytes_readed = recv(c.sockfd, res.buf, res.header.len, MSG_NOSIGNAL | MSG_DONTWAIT);

                        if (bytes_readed == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                            LOG_WARN("Would block");
                            break;
                        }

                        if (bytes_readed == -1) {
                            LOG_CUSTOM_ERRNO("recv");
                            break;
                        }

                        struct timeval now;
                        gettimeofday(&now, NULL);
                        unsigned long latency = (1000000 * now.tv_sec + now.tv_usec) -
                                                (1000000 * res.header.tv.tv_sec + res.header.tv.tv_usec);
                        audio_client_stats_update(&c.stats, latency);
                        queue_enqueue(&c.queue, (unsigned char *)res.buf, bytes_readed);
                        continue;
                    }

                    LOG_DEBUG("Invalid response");
                }
            }
        }
    }

    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port) {
    *c = (Audio_Client){0};

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);

    if (pthread_sigmask(SIG_BLOCK, &mask, NULL) == -1) {
        LOG_CUSTOM_ERRNO("pthread_sigmask");
        return 0;
    }

    c->sockfd = audio_client_create_tcp_socket(server_addr, server_tcp_port);

    if (c->sockfd == -1) {
        return 0;
    }

    c->epollfd = epoll_create1(0);

    if (c->epollfd == -1) {
        LOG_CUSTOM_ERRNO("epoll_create1");
        return 0;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = STDIN_FILENO;

    if (epoll_ctl(c->epollfd, EPOLL_CTL_ADD, STDIN_FILENO, &ev) == -1) {
        LOG_CUSTOM_ERRNO("epoll_ctl");
        return 0;
    }

    ev.events = EPOLLRDHUP | EPOLLIN;
    ev.data.fd = c->sockfd;

    if (epoll_ctl(c->epollfd, EPOLL_CTL_ADD, c->sockfd, &ev) == -1) {
        LOG_CUSTOM_ERRNO("epoll_ctl");
        return 0;
    }

    queue_init(&c->queue, KB(32));

    const char *args[] = {"--quiet"};
    c->vlc_instance = libvlc_new(1, args);

    if (c->vlc_instance == NULL) {
        LOG_CUSTOM_ERRNO("libvlc_new");
        return 0;
    }

    libvlc_media_t *vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);

    if (!vlc_media) {
        LOG_CUSTOM_ERRNO("libvlc_media_new_callbacks");
        return 0;
    }

    c->vlc_mp = libvlc_media_player_new_from_media(vlc_media);

    if (!c->vlc_mp) {
        LOG_CUSTOM_ERRNO("libvlc_media_player_new_from_media");
        return 0;
    }

    libvlc_media_release(vlc_media);

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) == -1) {
        LOG_CUSTOM_ERRNO("pthread_sigmask");
        return 0;
    }

    return 1;
}

int audio_client_create_tcp_socket(const char *server_addr, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        LOG_CUSTOM_ERRNO("socket");
        return -1;
    }

    struct sockaddr_in srv_addr = {0};
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(port);

    int ok = inet_pton(AF_INET, server_addr, &srv_addr.sin_addr.s_addr);

    if (ok == 0) {
        LOG_ERROR("Invalid -ipaddr format");
        return -1;
    }

    if (ok == -1) {
        LOG_CUSTOM_ERRNO("inet_pton");
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) == -1) {
        LOG_CUSTOM_ERRNO("connect");
        return -1;
    }

    return fd;
}

void audio_client_destroy(Audio_Client *c) {
    queue_abort(&c->queue);

    if (c->vlc_mp) {
        libvlc_media_player_stop(c->vlc_mp);
        libvlc_media_player_release(c->vlc_mp);
    }
    if (c->vlc_instance) {
        libvlc_release(c->vlc_instance);
    }

    audio_client_handle_exit(c);
    c->is_playing = 0;
    queue_destroy(&c->queue);
}

void audio_client_handle_exit(Audio_Client *c) {
    if (c->sockfd <= 0) {
        return;
    }

    shutdown(c->sockfd, SHUT_RDWR);
    close(c->sockfd);

    if (c->epollfd > 0) {
        close(c->epollfd);
    }
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

void audio_client_stats_reset(Delay_Stats *s) {
    s->min_us = ULONG_MAX;
    s->max_us = 0;
    s->sum_us = 0;
    s->count = 0;
}

void audio_client_stats_update(Delay_Stats *s, unsigned long delay_us) {
    if (delay_us < s->min_us) {
        s->min_us = delay_us;
    }
    if (delay_us > s->max_us) {
        s->max_us = delay_us;
    }
    s->sum_us += delay_us;
    s->count++;
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

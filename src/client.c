#include "logger.h"
#include "packets.h"
#include "queue.h"
#include "signals.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/vlc.h>

#define MB(x) ((1 << 20) * x) // 1MB
#define HELP_MSG                                                                                                       \
    ("|=======================================|\n"                                                                     \
     "| /help         -> more info            |\n"                                                                     \
     "| /list         -> list avaliable songs |\n"                                                                     \
     "| /start <file> -> start streaming file |\n"                                                                     \
     "| /stats        -> show metrics         |\n"                                                                     \
     "| /stop         -> stop streaming       |\n"                                                                     \
     "| /reset        -> reset metrics        |\n"                                                                     \
     "| /resume       -> resume streaming     |\n"                                                                     \
     "| /exit or ^C   -> to exit              |\n"                                                                     \
     "|=======================================|\n")

// Registrar só pacotes de streaming?
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

int main(int argc, char **argv) {
    Audio_Client c;

    logger_initConsoleLogger(stdout);
    logger_setLevel(LogLevel_DEBUG);

    if (argc < 3) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <server-ip-address> <server-port>\n");
        return 1;
    }

    const char *server_ip_addr = argv[1];
    int server_port = atoi(argv[2]);

    if (server_port == 0) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <server-ip-address> <server-port>\n");
        return 1;
    }

    int ok = audio_client_init(&c, server_ip_addr, server_port);

    if (!ok) {
        audio_client_destroy(&c);
        return 1;
    }

    printf("/help for more info\n");

    const int MAX_EVENTS = 10;
    int N = 0;
    struct epoll_event events[MAX_EVENTS];

    while (!signaled) {
        N = epoll_wait(c.epollfd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            LOG_ERROR("epoll_wait");
            audio_client_destroy(&c);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            uint32_t event_mask = events[i].events;
            int event_sock = events[i].data.fd;

            if (event_sock == STDIN_FILENO && event_mask & EPOLLIN) {
                Request req = {0};
                char prompt[NAME_MAX] = {0};
                read(STDIN_FILENO, prompt, sizeof(prompt));
                char *ptr = strchr(prompt, '\n');

                if (ptr) {
                    *ptr = '\0';
                }
                if (*prompt == '\0') {
                    continue;
                }
                if (strcmp(prompt, "/exit") == 0) {
                    signaled = 1;
                    break;
                }
                if (strcmp(prompt, "/help") == 0) {
                    printf(HELP_MSG);
                    continue;
                }
                if (strcmp(prompt, "/stats") == 0) {
                    audio_client_stats_print(&c.stats);
                    continue;
                }
                if (strcmp(prompt, "/reset") == 0) {
                    audio_client_stats_reset(&c.stats);
                    printf("Metrics reset.\n");
                    continue;
                }

                req.header.kind = audio_client_parse_str_to_enum(prompt);

                if (req.header.kind == KIND_NONE) {
                    break;
                }

                if (req.header.kind == KIND_START) {
                    char *filename = prompt + sizeof("/start ") - 1;
                    strcpy(req.buf, filename);
                    req.header.len = strlen(filename) + 1;
                }

                if ((req.header.kind == KIND_STOP && c.is_playing == 0) ||
                    (req.header.kind == KIND_RESUME && (c.has_playered == 0 || c.is_playing == 1))) {
                    break;
                }

                ssize_t ok = send(c.sockfd, &req.header, sizeof(req.header),
                                  MSG_NOSIGNAL | (req.header.kind == KIND_START ? MSG_MORE : 0));
                if (ok == -1) {
                    LOG_ERROR("send");
                    break;
                }

                if (req.header.kind != KIND_START) {
                    break;
                }

                ok = send(c.sockfd, req.buf, req.header.len, MSG_NOSIGNAL);
                if (ok == -1) {
                    LOG_ERROR("send");
                    break;
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
                    ssize_t ok = recv(c.sockfd, &res.header, sizeof(res.header), MSG_NOSIGNAL);

                    if (ok == -1) {
                        LOG_ERROR("recv");
                        break;
                    }

                    switch (res.header.kind) {
                    case KIND_LIST:
                        if (res.header.code == STATUS_LIST_END) {
                            printf("End of list\n");
                            break;
                        }
                        ok = recv(c.sockfd, res.buf, res.header.len, MSG_NOSIGNAL);

                        if (ok == -1) {
                            LOG_ERROR("recv");
                            break;
                        }
                        printf("| %s |\n", res.buf);
                        break;
                    case KIND_START:
                        if (res.header.code == STATUS_ERR_NO_FILE) {
                            printf("No audio file\n");
                            break;
                        }
                        audio_client_handle_start(&c);
                        break;
                    case KIND_STOP:
                        c.is_playing = 0;
                        libvlc_media_player_pause(c.vlc_mp);
                        break;
                    case KIND_RESUME:
                        c.is_playing = 1;
                        libvlc_media_player_play(c.vlc_mp);
                        break;
                    case KIND_STREAM:
                        ok = recv(c.sockfd, res.buf, res.header.len, MSG_NOSIGNAL | MSG_DONTWAIT);

                        if (ok == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                            LOG_ERROR("recv");
                            break;
                        }

                        if (ok == -1) {
                            LOG_ERROR("recv");
                            break;
                        }

                        struct timeval now;
                        gettimeofday(&now, NULL);
                        unsigned long latency = (1000000 * now.tv_sec + now.tv_usec) -
                                                (1000000 * res.header.tv.tv_sec + res.header.tv.tv_usec);
                        audio_client_stats_update(&c.stats, latency);

                        queue_enqueue(&c.queue, (unsigned char *)res.buf, ok);
                        break;
                    default:
                        break;
                    }
                }
            }
        }
    }

    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port) {
    *c = (Audio_Client){0};

    audio_client_stats_reset(&c->stats);

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    c->sockfd = audio_client_create_tcp_socket(server_addr, server_tcp_port);

    if (c->sockfd == -1) {
        return 0;
    }

    c->epollfd = epoll_create1(0);

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = STDIN_FILENO;

    epoll_ctl(c->epollfd, EPOLL_CTL_ADD, STDIN_FILENO, &ev);
    ev.events = EPOLLRDHUP | EPOLLIN;
    ev.data.fd = c->sockfd;
    epoll_ctl(c->epollfd, EPOLL_CTL_ADD, c->sockfd, &ev);

    queue_init(&c->queue, MB(1));

    const char *args[] = {"--quiet"};
    c->vlc_instance = libvlc_new(1, args);

    if (c->vlc_instance == NULL) {
        fprintf(stderr, "Failed to init vlc: %s\n", strerror(errno));
        return 0;
    }

    libvlc_media_t *vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);
    c->vlc_mp = libvlc_media_player_new_from_media(vlc_media);
    libvlc_media_release(vlc_media);

    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    pthread_sigmask(SIG_UNBLOCK, &mask, NULL);

    return 1;
}

int audio_client_create_tcp_socket(const char *server_addr, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return -1;
    }

    struct sockaddr_in srv_addr = {0};
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_addr.s_addr = inet_addr(server_addr);
    srv_addr.sin_port = htons(port);

    if (connect(fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) == -1) {
        fprintf(stderr, "Failed to connect socket: %s\n", strerror(errno));
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

// Metrics
void audio_client_stats_reset(Delay_Stats *s) {
    s->min_us = ULONG_MAX;
    s->max_us = 0;
    s->sum_us = 0;
    s->count = 0;
}

void audio_client_stats_update(Delay_Stats *s, unsigned long delay_us) {
    if (delay_us < s->min_us)
        s->min_us = delay_us;
    if (delay_us > s->max_us)
        s->max_us = delay_us;
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
    printf("|=======================================|\n"
           "| packets : %lu                         |\n"
           "| min     : %lu us                      |\n"
           "| max     : %lu us                      |\n"
           "| avg     : %lu us                      |\n"
           "|=======================================|\n",
           s->count, s->min_us, s->max_us, s->sum_us / s->count);
}

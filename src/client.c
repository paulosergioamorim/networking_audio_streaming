#include "packets.h"
#include "signals.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/vlc.h>

#define MB(x) ((1 << 20) * x) // 1MB

typedef struct {
    char *buf;
    size_t count;
    size_t head;
    size_t tail;
} Queue;

typedef struct {
    int tcp_sock;
    int udp_sock;
    libvlc_instance_t *vlc_instance;
    libvlc_media_player_t *vlc_mp;
    libvlc_media_t *vlc_media;
    Queue queue; // circular buffer
    pthread_t recv_thread;
    int is_playing;
    int is_exiting;
    pthread_mutex_t mu;
    pthread_cond_t cond_empty_buf;
    pthread_cond_t cond_not_playing;
} Audio_Client;

int open_cb(void *opaque, void **datap, uint64_t *sizep) {
    *datap = opaque;
    *sizep = UINT64_MAX;
    return 0;
}

ssize_t read_cb(void *opaque, unsigned char *buf, size_t len) {
    Audio_Client *c = (Audio_Client *)opaque;
    Queue *q = &c->queue;
    size_t to_read = 0;

    pthread_mutex_lock(&c->mu);

    while (q->count == 0 && c->is_playing) {
        pthread_cond_wait(&c->cond_empty_buf, &c->mu);
    }

    if (c->is_exiting) {
        return -1;
    }

    if (!c->is_playing && q->count == 0) {
        pthread_mutex_unlock(&c->mu);
        return 0;
    }

    to_read = len < q->count ? len : q->count;
    size_t first_part = MB(1) - q->head;

    if (to_read <= first_part) {
        memcpy(buf, q->buf + q->head, to_read);
    } else {
        memcpy(buf, q->buf + q->head, first_part);
        memcpy(buf + first_part, q->buf, to_read - first_part);
    }

    q->head = (q->head + to_read) % MB(1);
    q->count -= to_read;

    pthread_mutex_unlock(&c->mu);
    return to_read;
}

int seek_cb(void *opaque, uint64_t offset) {
    return -1;
}

void close_cb(void *opaque) {
}

void *udp_receiver_thread(void *arg) {
    Audio_Client *c = (Audio_Client *)arg;
    Queue *q = &c->queue;
    Audio_Stream pkt;

    while (1) {
        pthread_mutex_lock(&c->mu);
        while (!c->is_playing) {
            if (c->is_exiting) {
                pthread_mutex_unlock(&c->mu);
                return NULL;
            }
            pthread_cond_wait(&c->cond_not_playing, &c->mu);
        }
        pthread_mutex_unlock(&c->mu);

        ssize_t n = recv(c->udp_sock, &pkt, sizeof(pkt), 0);

        if (n != sizeof(pkt)) {
            continue;
        }

        pthread_mutex_lock(&c->mu);
        size_t space_left = MB(1) - q->count;
        if (space_left >= AUDIO_STREAM_SIZE) {
            size_t first_part = MB(1) - q->tail;

            if (AUDIO_STREAM_SIZE <= first_part) {
                memcpy(q->buf + q->tail, pkt.buf, AUDIO_STREAM_SIZE);
            } else {
                memcpy(q->buf + q->tail, pkt.buf, first_part);
                memcpy(q->buf, pkt.buf + first_part, AUDIO_STREAM_SIZE - first_part);
            }

            q->tail = (q->tail + AUDIO_STREAM_SIZE) % MB(1);
            q->count += AUDIO_STREAM_SIZE;

            pthread_cond_signal(&c->cond_empty_buf);
        }

        pthread_mutex_unlock(&c->mu);
    }

    return NULL;
}

int audio_client_init(Audio_Client *c, const char *client_addr, int client_udp_port, const char *server_addr,
                      int server_tcp_port, int server_udp_port);

int audio_client_create_tcp_socket(const char *server_addr, int port);

int audio_client_create_udp_socket(const char *server_addr, int port);

void audio_client_destroy(Audio_Client *c);

int main(int argc, char **argv) {
    Audio_Client c;

    if (argc < 5) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <client-ip-address> <client-udp-port> <server-ip-address> "
                        "<server-tcp_port> <server-udp_port>\n");
        return 1;
    }

    const char *client_ip_addr = argv[1];
    int client_udp_port = atoi(argv[2]);
    const char *server_ip_addr = argv[3];
    int server_tcp_port = atoi(argv[4]);
    int server_udp_port = atoi(argv[5]);

    if (client_udp_port == 0 || server_tcp_port == 0 || server_udp_port == 0) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <client-ip-address> <client-udp-port> <server-ip-address> "
                        "<server-tcp_port> <server-udp_port>\n");
        return 1;
    }

    int ok = audio_client_init(&c, client_ip_addr, client_udp_port, server_ip_addr, server_tcp_port, server_udp_port);

    if (!ok) {
        audio_client_destroy(&c);
        return 1;
    }

    printf("/help for more info\n");

    while (!signaled) {
        Message req = {0};
        Message res = {0};
        char prompt[256] = {0};
        printf(">>> ");
        fgets(prompt, sizeof(prompt), stdin);
        char *ptr = strchr(prompt, '\n');

        if (ptr) {
            *ptr = '\0';
        }

        if (*prompt == '\0') {
            continue;
        }

        if (strcmp(prompt, "/exit") == 0) {
            break;
        }
        if (strcmp(prompt, "/help") == 0) {
            printf("|=======================================|\n"
                   "| /help         -> more info            |\n"
                   "| /list         -> list avaliable songs |\n"
                   "| /start <file> -> start streaming file |\n"
                   "| /stop         -> stop streaming       |\n"
                   "| /resume       -> resume streaming     |\n"
                   "| /exit or ^C   -> to exit              |\n"
                   "|=======================================|\n");
            continue;
        }

        if (strcmp(prompt, "/list") == 0) {
            req.kind = REQ_LIST;
        } else if (strncmp(prompt, "/start", 6) == 0) {
            req.kind = REQ_START;
            char *filename = prompt + sizeof("/start ") - 1;
            strcpy(req.buf, filename);
        } else {
            printf("Invalid input\n");
            continue;
        }

        int wr = send(c.tcp_sock, &req, sizeof(req), MSG_NOSIGNAL);
        if (wr == -1) {
            perror("send");
            break;
        }

        int br = recv(c.tcp_sock, &res, sizeof(res), 0);

        if (br == -1) {
            perror("recv");
            break;
        }

        if (res.kind == RES_LIST_END) {
            printf("No audio files\n");
            continue;
        }

        if (res.kind == RES_LIST_CONTINUE) {
            do {
                printf("| %s |\n", res.buf);
                int br = recv(c.tcp_sock, &res, sizeof(res), 0);

                if (br == -1) {
                    perror("recv");
                    break;
                }
            } while (res.kind != RES_LIST_END);
            continue;
        }

        if (res.kind == RES_START_NO_FILE) {
            printf("No audio file\n");
            continue;
        }

        if (res.kind == RES_START_OK) {
            pthread_mutex_lock(&c.mu);
            c.is_playing = 1;
            libvlc_media_player_play(c.vlc_mp);
            pthread_cond_signal(&c.cond_not_playing);
            pthread_mutex_unlock(&c.mu);
            continue;
        }
    }

    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *client_addr, int client_udp_port, const char *server_addr,
                      int server_tcp_port, int server_udp_port) {
    *c = (Audio_Client){0};

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    c->udp_sock = audio_client_create_udp_socket(server_addr, server_udp_port);

    if (c->udp_sock == -1) {
        return 0;
    }

    struct sockaddr_in sockaddr = {0}; // udp socket address for binding
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr.s_addr = inet_addr(client_addr);
    sockaddr.sin_port = htons(client_udp_port);

    if (bind(c->udp_sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
        fprintf(stderr, "Failed to bind socket: %s\n", strerror(errno));
        return 0;
    }

    c->tcp_sock = audio_client_create_tcp_socket(server_addr, server_tcp_port);

    if (c->tcp_sock == -1) {
        return 0;
    }

    send(c->tcp_sock, &sockaddr, sizeof(sockaddr), 0);

    c->queue.buf = malloc(MB(1) * sizeof(*c->queue.buf));

    if (c->queue.buf == NULL) {
        fprintf(stderr, "Failed to malloc: %s\n", strerror(errno));
        return 0;
    }

    const char *args[] = {"--quiet"};
    c->vlc_instance = libvlc_new(1, args);

    if (c->vlc_instance == NULL) {
        fprintf(stderr, "Failed to init vlc: %s\n", strerror(errno));
        return 0;
    }

    c->vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);
    c->vlc_mp = libvlc_media_player_new_from_media(c->vlc_media);
    libvlc_media_release(c->vlc_media);
    pthread_create(&c->recv_thread, NULL, udp_receiver_thread, c);
    pthread_mutex_init(&c->mu, NULL);
    pthread_cond_init(&c->cond_empty_buf, NULL);
    pthread_cond_init(&c->cond_not_playing, NULL);

    if (signals_sigint_sigaction() == -1) {
        return 0;
    } // after libvlc init

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

int audio_client_create_udp_socket(const char *server_addr, int port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (fd == -1) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return -1;
    }

    return fd;
}

void audio_client_destroy(Audio_Client *c) {
    pthread_mutex_lock(&c->mu);
    c->is_playing = 0;
    c->is_exiting = 1;
    pthread_cond_broadcast(&c->cond_not_playing);
    pthread_cond_broadcast(&c->cond_empty_buf);
    pthread_mutex_unlock(&c->mu);
    if (c->tcp_sock > 0) {
        close(c->tcp_sock);
    }
    if (c->udp_sock > 0) {
        close(c->udp_sock);
    }
    libvlc_media_player_stop(c->vlc_mp);
    libvlc_media_player_release(c->vlc_mp);
    libvlc_release(c->vlc_instance);
    if (c->queue.buf != NULL) {
        free(c->queue.buf);
    }
    pthread_cond_destroy(&c->cond_empty_buf);
    pthread_cond_destroy(&c->cond_not_playing);
    pthread_mutex_destroy(&c->mu);
}

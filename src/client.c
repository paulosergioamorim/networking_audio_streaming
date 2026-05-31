#include "packets.h"
#include "queue.h"
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
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/vlc.h>

#define MB(x) ((1 << 20) * x) // 1MB

typedef struct {
    int epollfd;
    int sock;
    libvlc_instance_t *vlc_instance;
    libvlc_media_player_t *vlc_mp;
    libvlc_media_t *vlc_media;
    Queue queue;
    int is_playing;
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

int audio_client_init(Audio_Client *c, const char *server_addr, int server_tcp_port);

int audio_client_create_tcp_socket(const char *server_addr, int port);

int audio_client_create_udp_socket(const char *server_addr, int port);

void audio_client_destroy(Audio_Client *c);

int main(int argc, char **argv) {
    Audio_Client c;

    if (argc < 3) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <server-ip-address> <server-tcp-port>\n");
        return 1;
    }

    const char *server_ip_addr = argv[1];
    int server_control_port = atoi(argv[2]);

    if (server_control_port == 0) {
        fprintf(stderr, "Args Error!\nCommand help: ./client <server-ip-address> <server-tcp-port>\n");
        return 1;
    }

    int ok = audio_client_init(&c, server_ip_addr, server_control_port);

    if (!ok) {
        audio_client_destroy(&c);
        return 1;
    }

    printf("/help for more info\n");

    const int MAX_EVENTS = 10;
    int N = 0;
    struct epoll_event events[MAX_EVENTS];
    struct epoll_event ev;

    while (!signaled) {
        N = epoll_wait(c.epollfd, events, MAX_EVENTS, -1);

        if (N & EINTR) {
            continue;
        }

        if (N == -1) {
            perror("epoll_wait");
            audio_client_destroy(&c);
            return 1;
        }

        for (int i = 0; i < N; i++) {
            int event_sock = events[i].data.fd;

            if (event_sock == STDIN_FILENO) {
                Message req = {0};
                char prompt[256] = {0};
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
                } else if (strcmp(prompt, "/stop") == 0) {
                    req.kind = REQ_STOP;
                } else if (strcmp(prompt, "/resume") == 0) {
                    req.kind = REQ_RESUME;
                } else {
                    printf("Invalid input\n");
                    continue;
                }

                int wr = send(c.sock, &req, sizeof(req), MSG_NOSIGNAL);
                if (wr == -1) {
                    perror("send");
                    break;
                }
            }

            if (event_sock == c.sock) {
                Message res = {0};
                int br = recv(c.sock, &res, sizeof(res), MSG_WAITALL);

                if (br == -1) {
                    perror("recv");
                    break;
                }

                if (res.kind == RES_LIST_END) {
                    printf("End of list\n");
                    continue;
                }

                if (res.kind == RES_LIST_CONTINUE) {
                    printf("| %s |\n", res.buf);
                    continue;
                }

                if (res.kind == RES_START_NO_FILE) {
                    printf("No audio file\n");
                    continue;
                }

                if (res.kind == RES_START_OK) {
                    c.is_playing = 1;
                    libvlc_media_player_play(c.vlc_mp);
                    continue;
                }

                if (res.kind == RES_STREAM) {
                    queue_enqueue(&c.queue, (unsigned char *)res.buf, res.len);
                    continue;
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
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    c->sock = audio_client_create_tcp_socket(server_addr, server_tcp_port);

    if (c->sock == -1) {
        return 0;
    }

    c->epollfd = epoll_create1(0);

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = STDIN_FILENO;

    epoll_ctl(c->epollfd, EPOLL_CTL_ADD, STDIN_FILENO, &ev);
    ev.events = EPOLLIN;
    ev.data.fd = c->sock;
    epoll_ctl(c->epollfd, EPOLL_CTL_ADD, c->sock, &ev);

    queue_init(&c->queue, MB(1));

    const char *args[] = {"--quiet"};
    c->vlc_instance = libvlc_new(1, args);

    if (c->vlc_instance == NULL) {
        fprintf(stderr, "Failed to init vlc: %s\n", strerror(errno));
        return 0;
    }

    c->vlc_media = libvlc_media_new_callbacks(c->vlc_instance, open_cb, read_cb, seek_cb, close_cb, c);
    c->vlc_mp = libvlc_media_player_new_from_media(c->vlc_media);
    libvlc_media_release(c->vlc_media);

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

int audio_client_create_udp_socket(const char *server_addr, int port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (fd == -1) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
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

    if (c->sock > 0) {
        close(c->sock);
    }

    c->is_playing = 0;
    queue_destroy(&c->queue);
}

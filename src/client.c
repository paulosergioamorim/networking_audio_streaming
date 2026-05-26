#include "packets.h"
#include "signals.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
// #include <vlc/libvlc.h>
// #include <vlc/libvlc_media.h>
// #include <vlc/vlc.h>

// libvlc_instance_t *vlc_instance;
// libvlc_media_player_t *vlc_mp;
// libvlc_media_t *vlc_media;

typedef struct {
    int tcp_sock;
    int udp_sock;
    // libvlc_instance_t *vlc_instance;
    // libvlc_media_player_t *vlc_mp;
    // libvlc_media_t *vlc_media;
} Audio_Client;

int audio_client_init(Audio_Client *c, const char *server_addr, int tcp_port, int udp_port);

int audio_client_create_tcp_socket(const char *server_addr, int port);

int audio_client_create_udp_socket(const char *server_addr, int port);

void audio_client_destroy(Audio_Client *c);

int main(int argc, char **argv) {
    Audio_Client c;
    int tcp_port = atoi(argv[2]);
    int udp_port = atoi(argv[3]);

    if (tcp_port == 0 || udp_port == 0) {
        fprintf(stderr,
                "Args Error!\nCommand help: ./client <server-ip-address> <server-tcp_port> <server-udp_port>\n");
        return 1;
    }

    int ok = audio_client_init(&c, argv[1], tcp_port, udp_port);

    if (!ok) {
        audio_client_destroy(&c);
        return 1;
    }

    printf("/help for more info\n");

    Message req;
    Message res;
    char prompt[256] = {0};
    while (!signaled) {
        printf(">>> ");
        memset(prompt, 0, sizeof(prompt));
        memset(&req, 0, sizeof(req));
        memset(&res, 0, sizeof(res));
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
            printf("/help -> more info\n"
                   "/list -> list avaliable songs\n"
                   "/start <file> -> start streaming file\n"
                   "/stop -> stop streaming\n"
                   "/resume -> resume streaming\n"
                   "/exit or ^C to exit\n");
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
                printf("%s\n", res.buf);
                int br = recv(c.tcp_sock, &res, sizeof(res), 0);

                if (br == -1) {
                    perror("recv");
                    break;
                }
            } while (res.kind != RES_LIST_END);
            continue;
        }
    }

    audio_client_destroy(&c);
    return 0;
}

int audio_client_init(Audio_Client *c, const char *server_addr, int tcp_port, int udp_port) {
    if (signals_sigint_sigaction() == -1) {
        return 0;
    }

    c->tcp_sock = audio_client_create_tcp_socket(server_addr, tcp_port);

    if (c->tcp_sock == -1) {
        return 0;
    }

    c->udp_sock = audio_client_create_udp_socket(server_addr, udp_port);

    if (c->udp_sock == -1) {
        return 0;
    }

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

    struct sockaddr_in srv_addr = {0};
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_addr.s_addr = inet_addr(server_addr);
    srv_addr.sin_port = htons(port);

    /**
     * TODO: CRIAR ALGUM MECANISMO DE ACK PARA O UDP SERVER CONHECER O UDP CLIENT
     */

    return fd;
}

void audio_client_destroy(Audio_Client *c) {
    if (c->tcp_sock > 0) {
        close(c->tcp_sock);
    }
    if (c->udp_sock > 0) {
        close(c->udp_sock);
    }
}

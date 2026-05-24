#include "packets.h"
#include "signals.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
// #include <vlc/libvlc.h>
// #include <vlc/libvlc_media.h>
// #include <vlc/vlc.h>

// libvlc_instance_t *vlc_instance;
// libvlc_media_player_t *vlc_mp;
// libvlc_media_t *vlc_media;

int main(int argc, char **argv) {
    if (signals_sigint_sigaction() == -1) {
        return 1;
    }

    // vlc_instance = libvlc_new(0, NULL);
    // vlc_media = libvlc_media_new_path(vlc_instance, "./audios/chelonia.mp4");
    // vlc_mp = libvlc_media_player_new_from_media(vlc_media);
    // libvlc_media_player_play(vlc_mp);

    int tcpfd = socket(AF_INET, SOCK_STREAM, 0);

    if (tcpfd == -1) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return 1;
    }

    struct sockaddr_in saddr = {0};
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = htonl(INADDR_ANY);
    saddr.sin_port = htons(8000);

    if (connect(tcpfd, (struct sockaddr *)&saddr, sizeof(saddr)) == -1) {
        fprintf(stderr, "Failed to connect socket: %s\n", strerror(errno));
        close(tcpfd);
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
            strcat(req.buf, "/list");
        } else if (strncmp(prompt, "/start", 6) == 0) {
            strncpy(req.buf, prompt, sizeof(prompt) - 1);
            req.buf[sizeof(prompt) - 1] = '\0';
        } else {
            printf("Invalid input\n");
            continue;
        }

        int wr = send(tcpfd, &req, sizeof(req), MSG_NOSIGNAL);
        if (wr == -1) {
            perror("send");
            break;
        }

        int br = recv(tcpfd, &res, sizeof(res), 0);

        if (br == -1) {
            perror("recv");
            break;
        }

        if (res.kind == MESSAGE_DISPLAY) {
            printf("%s", res.buf);
        }

        if (strcmp(res.buf, "START OK") == 0) {
            printf("%s\n", res.buf);
        }
    }

    if (close(tcpfd) == -1) {
        fprintf(stderr, "Failed to close socket: %s\n", strerror(errno));
        return 1;
    }

    return 0;
}

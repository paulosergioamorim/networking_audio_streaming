#include "event.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlc/libvlc.h>
#include <vlc/libvlc_media.h>
#include <vlc/vlc.h>

int fd;

libvlc_instance_t *vlc_instance;
libvlc_media_player_t *vlc_mp;
libvlc_media_t *vlc_media;

void server_handle_sigint(int signal) {
    close(fd);
    char buf[] = "Exiting server by ^C\n";
    write(STDOUT_FILENO, buf, sizeof(buf));
    exit(0);
}

int main(int argc, char **argv) {
    struct sigaction sa = {0};
    sa.sa_handler = server_handle_sigint;
    vlc_instance = libvlc_new(0, NULL);
    vlc_media = libvlc_media_new_path(vlc_instance, "chelonia.mp4");
    vlc_mp = libvlc_media_player_new_from_media(vlc_media);
    libvlc_media_player_play(vlc_mp);

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        fprintf(stderr, "Failed to create handler to SIGINT: %s\n", strerror(errno));
        return 1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return 1;
    }

    struct sockaddr_in saddr = {0};
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = htonl(INADDR_ANY);
    saddr.sin_port = htons(8000);

    if (connect(fd, (struct sockaddr *)&saddr, sizeof(saddr)) == -1) {
        fprintf(stderr, "Failed to connect socket: %s\n", strerror(errno));
        close(fd);
        return 1;
    }

    while (1) {
        char buf[256] = {0};
        printf(">>> ");
        scanf("%255s", buf);
        Event event;
        if (strcmp(buf, "/help") == 0) {
            event = (Event){.kind = EVENT_COMMAND, .buf = "/help"};
        } else if (strcmp(buf, "/list") == 0) {
            event = (Event){.kind = EVENT_COMMAND, .buf = "/list"};
        } else if (strncmp(buf, "/start", 6) == 0) {
            event = (Event){
                .kind = EVENT_COMMAND,
            };
            strncpy(event.buf, buf, sizeof(buf) - 1);
            event.buf[sizeof(buf) - 1] = '\0';
        } else {
            printf("Invalid input\n");
            continue;
        }
        if (write(fd, &event, sizeof(event)) == -1) {
            perror("write");
            continue;
        }
        recvfrom(fd, &event, sizeof(event), 0, NULL, 0);
        if (event.kind == EVENT_MESSAGE) {
            printf("%s", event.buf);
        }
    }

    if (close(fd) == -1) {
        fprintf(stderr, "Failed to close socket: %s\n", strerror(errno));
        return 1;
    }

    return 0;
}

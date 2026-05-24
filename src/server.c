#include "event.h"
#include <arpa/inet.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#define SUFFIX_IMPLEMENTATION
#include "suffix.h"
#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

int create_tcp_server_socket();

int create_udp_server_socket();

volatile sig_atomic_t signaled = 0;

void server_handle_sigint(int signal) {
    signaled = 1;
}

int create_tcp_server_socket() {
    struct sockaddr_in saddr;
    int fd, ret_val;

    /* Step1: create a TCP socket */
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "socket failed [%s]\n", strerror(errno));
        return -1;
    }
    printf("Created a socket with fd: %d\n", fd);

    /* Initialize the socket address structure */
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(8000);
    saddr.sin_addr.s_addr = INADDR_ANY;

    /* Step2: bind the socket to port 8000 on the local host */
    ret_val = bind(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val != 0) {
        fprintf(stderr, "bind failed [%s]\n", strerror(errno));
        close(fd);
        return -1;
    }

    /* Step3: listen for incoming connections */
    ret_val = listen(fd, 5);
    if (ret_val != 0) {
        fprintf(stderr, "listen failed [%s]\n", strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

#define MAX_EVENTS 10

int main(int argc, char **argv) {
    struct sockaddr_in new_addr = {0};
    socklen_t addrlen = 0;
    Event event;

    struct epoll_event ev, events[MAX_EVENTS];
    int listen_sock, conn_sock, nfds, epollfd;

    listen_sock = create_tcp_server_socket();

    if (listen_sock == -1) {
        fprintf(stderr, "Failed to create a server\n");
        return -1;
    }

    epollfd = epoll_create1(0);
    if (epollfd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    ev.events = EPOLLIN;
    ev.data.fd = listen_sock;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, listen_sock, &ev) == -1) {
        perror("epoll_ctl: listen_sock");
        exit(EXIT_FAILURE);
    }

    while (!signaled) {
        nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1);
        if (nfds == -1) {
            perror("epoll_wait");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == listen_sock) {
                conn_sock = accept(listen_sock, (struct sockaddr *)&new_addr, &addrlen);
                if (conn_sock == -1) {
                    perror("accept");
                    exit(EXIT_FAILURE);
                }
                // setnonblocking(conn_sock);
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = conn_sock;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
                    perror("epoll_ctl: conn_sock");
                    exit(EXIT_FAILURE);
                }
            } else {
                int ok = read(events[i].data.fd, &event, sizeof(event));
                if (ok == 0) {
                    printf("Closing fd %d of index %d\n", events[i].data.fd, i);
                    if (epoll_ctl(epollfd, EPOLL_CTL_DEL, events[i].data.fd, &ev) == -1) {
                        perror("epoll_ctl: conn");
                        exit(EXIT_FAILURE);
                    }
                }
                if (ok > 0) {
                    if (event.kind == EVENT_COMMAND) {
                        if (strcmp(event.buf, "/help") == 0) {
                            Event content =
                                (Event){.kind = EVENT_MESSAGE,
                                        .buf = "/help\n/list\n/start <file>\n/pause\n/resume\n^C to exit\n"};
                            write(events[i].data.fd, &content, sizeof(content));
                        } else if (strcmp(event.buf, "/list") == 0) {
                            DIR *dir = opendir(".");
                            struct dirent *de;
                            Event content = (Event){.kind = EVENT_MESSAGE, .buf = ""};
                            while ((de = readdir(dir)) != NULL) {
                                if (de->d_type != 'd' && IsAudioFile(de->d_name)) {
                                    strncat(content.buf, de->d_name, sizeof(content.buf) - strlen(content.buf) - 1);
                                    strncat(content.buf, "\n", sizeof(content.buf) - strlen(content.buf) - 1);
                                }
                            }
                            write(events[i].data.fd, &content, sizeof(content));
                            closedir(dir);
                        } else if (strncmp(event.buf, "/start", 6) == 0) {
                            printf("%s", event.buf);
                        }
                    }
                }
                if (ok == -1) {
                    break;
                }
            }
        }
    }

    close(epollfd);

    return 0;
}

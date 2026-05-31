#include "token.h"
#include <fcntl.h>
#include <unistd.h>

unsigned long generate_secure_token() {
    unsigned long token = 0;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd != -1) {
        read(fd, &token, sizeof(token));
        close(fd);
    }
    return token;
}

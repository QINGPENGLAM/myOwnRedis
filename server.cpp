#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

const size_t k_max_msg = 4096;

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t one_request(int connfd) {
    char rbuf[4 + k_max_msg];
    errno = 0;
    int32_t err = read_full(connfd, rbuf, 4);
    if (err) {
        return err;
    }
    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > k_max_msg) {
        return -1;
    }
    err = read_full(connfd, &rbuf[4], len);
    if (err) {
        return err;
    }
    printf("client says: %.*s\n", len, &rbuf[4]);
    const char reply[] = "world";
    len = (uint32_t)strlen(reply);
    char wbuf[4 + sizeof(reply)];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], reply, len);
    return write_all(connfd, wbuf, 4 + len);
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);
    bind(fd, (const struct sockaddr*)&addr, sizeof(addr));
    listen(fd, SOMAXCONN);
    printf("server listening on port 1234...\n");

    while (1) {
        struct sockaddr_in client_addr = {};
        socklen_t addrlen = sizeof(client_addr);
        int connfd = accept(fd, (struct sockaddr*)&client_addr, &addrlen);
        if (connfd < 0) continue;
        while (1) {
            int32_t err = one_request(connfd);
            if (err) break;
        }
        close(connfd);
    }
    return 0;
}

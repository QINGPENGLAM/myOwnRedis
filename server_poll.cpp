#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

const size_t k_max_msg = 4096;
const size_t k_max_clients = 1024;

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

// 和第 4 章一样：处理一条请求
static int32_t one_request(int connfd) {
    char rbuf[4 + k_max_msg];
    errno = 0;
    int32_t err = read_full(connfd, rbuf, 4);
    if (err) {
        return err;
    }
    uint32_t len = 0;
    memcpy(&len, rbuf, 4);  // 假设小端
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

struct Conn {
    int fd;
    int in_use;
};

int main() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int val = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0); // 0.0.0.0
    bind(listen_fd, (const struct sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, SOMAXCONN);

    printf("server_poll listening on port 1234...\n");

    // 保存所有客户端连接
    struct Conn conns[k_max_clients];
    for (size_t i = 0; i < k_max_clients; ++i) {
        conns[i].fd = -1;
        conns[i].in_use = 0;
    }

    while (1) {
        // 构造 pollfd 数组
        struct pollfd pfds[1 + k_max_clients];
        size_t nfds = 0;

        // 下标 0：监听 socket
        pfds[nfds].fd = listen_fd;
        pfds[nfds].events = POLLIN;
        nfds++;

        // 并且记下每个 pollfd 对应哪个 Conn
        struct Conn *idx2conn[1 + k_max_clients];
        idx2conn[0] = NULL;  // 监听 fd 占 0

        for (size_t i = 0; i < k_max_clients; ++i) {
            if (!conns[i].in_use) continue;
            pfds[nfds].fd = conns[i].fd;
            pfds[nfds].events = POLLIN;  // 目前只关心读
            idx2conn[nfds] = &conns[i];
            nfds++;
        }

        int rv = poll(pfds, nfds, -1);  // -1 表示一直等
        if (rv < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        // 1) 先看监听 fd 是否有新连接
        if (pfds[0].revents & POLLIN) {
            struct sockaddr_in client_addr = {};
            socklen_t addrlen = sizeof(client_addr);
            int connfd = accept(listen_fd, (struct sockaddr*)&client_addr, &addrlen);
            if (connfd >= 0) {
                // 找一个空位存这个连接
                size_t i;
                for (i = 0; i < k_max_clients; ++i) {
                    if (!conns[i].in_use) {
                        conns[i].fd = connfd;
                        conns[i].in_use = 1;
                        break;
                    }
                }
                if (i == k_max_clients) {
                    // 太多 client 了，关掉这个
                    close(connfd);
                }
            }
        }

        // 2) 处理已有连接上的数据
        for (size_t i = 1; i < nfds; ++i) {
            if (!(pfds[i].revents & POLLIN)) continue;
            struct Conn *conn = idx2conn[i];
            if (!conn || !conn->in_use) continue;

            int32_t err = one_request(conn->fd);
            if (err) {
                // 出错或客户端断开，关掉这个连接
                close(conn->fd);
                conn->fd = -1;
                conn->in_use = 0;
            }
        }
    }

    close(listen_fd);
    return 0;
}

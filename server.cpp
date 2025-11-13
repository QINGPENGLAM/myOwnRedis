// server.cpp
// Non-blocking, poll-based KV server with intrusive chaining hashtable.
// Commands: get <key>, set <key> <val>, del <key>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

#include <string>
#include <vector>

#include "hashtable.h"  // intrusive chaining HT with progressive rehashing

// ----------- utils -----------
static void msg(const char *m) { fprintf(stderr, "%s\n", m); }
static void msg_errno(const char *m) { fprintf(stderr, "[errno:%d] %s\n", errno, m); }
static void die(const char *m) { fprintf(stderr, "[%d] %s\n", errno, m); abort(); }

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) die("fcntl(F_GETFL)");
    flags |= O_NONBLOCK;
    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) die("fcntl(F_SETFL)");
}

// Max message and args (same framing as your client)
const size_t k_max_msg  = 32u << 20;       // allow large payloads
const size_t k_max_args = 200u * 1000u;    // safety limit

// ----------- connection state -----------
struct Conn {
    int fd = -1;

    // app intent for event loop
    bool want_read  = false;
    bool want_write = false;
    bool want_close = false;

    // IO buffers
    std::vector<uint8_t> incoming;  // bytes to parse
    std::vector<uint8_t> outgoing;  // framed responses
};

// Append to back
static inline void buf_append(std::vector<uint8_t> &b, const uint8_t *p, size_t n) {
    b.insert(b.end(), p, p + n);
}
// Consume from front
static inline void buf_consume(std::vector<uint8_t> &b, size_t n) {
    b.erase(b.begin(), b.begin() + n);
}

// ----------- accept callback -----------
static Conn *handle_accept(int fd) {
    struct sockaddr_in caddr = {};
    socklen_t alen = sizeof(caddr);
    int cfd = accept(fd, (struct sockaddr*)&caddr, &alen);
    if (cfd < 0) {
        msg_errno("accept()");
        return nullptr;
    }
    uint32_t ip = caddr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
            ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, (ip >> 24) & 255,
            ntohs(caddr.sin_port));

    fd_set_nb(cfd);

    Conn *conn = new Conn();
    conn->fd = cfd;
    conn->want_read = true;
    return conn;
}

// ----------- protocol parsing -----------
static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) return false;
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}
static bool read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) return false;
    out.assign((const char*)cur, (const char*)cur + n);
    cur += n;
    return true;
}

// Request frame (body):
// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+
static int32_t parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;

    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr)) return -1;
    if (nstr > k_max_args) return -1;

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) return -1;
        out.emplace_back();
        if (!read_str(data, end, len, out.back())) return -1;
    }
    if (data != end) return -1; // trailing bytes
    return 0;
}

// ----------- response framing -----------
enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2,     // not found
};
struct Response {
    uint32_t status = RES_OK;
    std::vector<uint8_t> data; // arbitrary payload (value)
};

// +--------+---------+
// | status | data... |
// +--------+---------+
static void make_response(const Response &resp, std::vector<uint8_t> &out) {
    uint32_t body_len = 4 + (uint32_t)resp.data.size();
    buf_append(out, (const uint8_t*)&body_len, 4);
    buf_append(out, (const uint8_t*)&resp.status, 4);
    if (!resp.data.empty()) {
        buf_append(out, resp.data.data(), resp.data.size());
    }
}

// ----------- intrusive hashtable-backed KV -----------

// Stored KV entry with intrusive node
struct Entry {
    HNode       node;
    std::string key;
    std::string val;
};

// Lookup key used only for probing (no val)
struct LookupKey {
    HNode       node;
    std::string key;
};

// Equality: Entry vs LookupKey (after hash match)
static bool key_eq(HNode *lhs, HNode *rhs) {
    Entry    *le = container_of(lhs, Entry,    node);
    LookupKey* rk = container_of(rhs, LookupKey, node);
    return le->key == rk->key;
}

static struct {
    HMap db;    // intrusive hashtable for top-level KV
} g_data;

// ----------- command handling -----------
static void do_request(std::vector<std::string> &cmd, Response &out) {
    HMap *db = &g_data.db;

    if (cmd.size() == 2 && cmd[0] == "get") {
        LookupKey lk;
        lk.key = std::move(cmd[1]);
        lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());

        if (HNode *n = hm_lookup(db, &lk.node, &key_eq)) {
            Entry *e = container_of(n, Entry, node);
            out.data.assign(e->val.begin(), e->val.end());
        } else {
            out.status = RES_NX;
        }
        return;
    }

    if (cmd.size() == 3 && cmd[0] == "set") {
        // Try update in place
        LookupKey lk;
        lk.key = cmd[1]; // copy so we can swap into Entry if not exists
        lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());

        if (HNode *n = hm_lookup(db, &lk.node, &key_eq)) {
            Entry *e = container_of(n, Entry, node);
            e->val.swap(cmd[2]);
            return;
        }
        // Insert new entry
        Entry *e = new Entry();
        e->key.swap(cmd[1]);
        e->val.swap(cmd[2]);
        e->node.hcode = str_hash((const uint8_t*)e->key.data(), e->key.size());
        hm_insert(db, &e->node);
        return;
    }

    if (cmd.size() == 2 && cmd[0] == "del") {
        LookupKey lk;
        lk.key = std::move(cmd[1]);
        lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());

        if (HNode *n = hm_delete(db, &lk.node, &key_eq)) {
            Entry *e = container_of(n, Entry, node);
            delete e; // free KV storage
        } else {
            out.status = RES_NX;
        }
        return;
    }

    out.status = RES_ERR; // unrecognized command
}

// ----------- request processing on a connection -----------
static bool try_one_request(Conn *conn) {
    // Need at least 4 bytes of header
    if (conn->incoming.size() < 4) return false;

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->want_close = true;
        return false;
    }

    if (conn->incoming.size() < 4 + len) return false; // wait for full body

    // Parse body
    const uint8_t *body = &conn->incoming[4];
    std::vector<std::string> cmd;
    if (parse_req(body, len, cmd) < 0) {
        msg("bad request");
        conn->want_close = true;
        return false;
    }

    // Handle command
    Response resp;
    do_request(cmd, resp);
    make_response(resp, conn->outgoing);

    // Remove consumed frame; support pipeline (may loop again)
    buf_consume(conn->incoming, 4 + len);
    return true;
}

// Writable
static void handle_write(Conn *conn) {
    assert(!conn->outgoing.empty());
    ssize_t rv = write(conn->fd, conn->outgoing.data(), conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) return;
    if (rv < 0) {
        msg_errno("write()");
        conn->want_close = true;
        return;
    }
    buf_consume(conn->outgoing, (size_t)rv);

    if (conn->outgoing.empty()) {
        conn->want_write = false;
        conn->want_read  = true;
    }
}

// Readable
static void handle_read(Conn *conn) {
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) return;

    if (rv < 0) {
        msg_errno("read()");
        conn->want_close = true;
        return;
    }
    if (rv == 0) {
        if (conn->incoming.empty()) msg("client closed");
        else msg("unexpected EOF");
        conn->want_close = true;
        return;
    }

    buf_append(conn->incoming, buf, (size_t)rv);

    while (try_one_request(conn)) {}

    if (!conn->outgoing.empty()) {
        conn->want_read  = false;
        conn->want_write = true;
        // optimistic immediate write
        handle_write(conn);
    }
}

// ----------- main event loop -----------
int main() {
    // listen socket
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) die("socket()");
    int val = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0); // 0.0.0.0
    if (bind(lfd, (const sockaddr*)&addr, sizeof(addr))) die("bind()");
    fd_set_nb(lfd);
    if (listen(lfd, SOMAXCONN)) die("listen()");

    // init database
    hm_init(&g_data.db);

    // fd → Conn*
    std::vector<Conn*> fd2conn;

    // event loop
    std::vector<struct pollfd> pfds;
    while (true) {
        pfds.clear();

        // index 0: listen socket
        pfds.push_back({lfd, POLLIN, 0});

        // connection sockets
        for (Conn *c : fd2conn) {
            if (!c) continue;
            short ev = POLLERR; // always track errors
            if (c->want_read)  ev |= POLLIN;
            if (c->want_write) ev |= POLLOUT;
            pfds.push_back({c->fd, ev, 0});
        }

        int rv = poll(pfds.data(), (nfds_t)pfds.size(), -1);
        if (rv < 0 && errno == EINTR) continue;
        if (rv < 0) die("poll()");

        // handle listen socket
        if (pfds[0].revents) {
            if (Conn *c = handle_accept(lfd)) {
                if (fd2conn.size() <= (size_t)c->fd) fd2conn.resize(c->fd + 1, nullptr);
                assert(!fd2conn[c->fd]);
                fd2conn[c->fd] = c;
            }
        }

        // handle connection sockets
        for (size_t i = 1; i < pfds.size(); ++i) {
            uint32_t ready = pfds[i].revents;
            if (!ready) continue;

            Conn *c = fd2conn[pfds[i].fd];
            if (!c) continue;

            if (ready & POLLIN) {
                assert(c->want_read);
                handle_read(c);
            }
            if (ready & POLLOUT) {
                assert(c->want_write);
                handle_write(c);
            }
            if ((ready & POLLERR) || c->want_close) {
                (void)close(c->fd);
                fd2conn[c->fd] = nullptr;
                delete c;
            }
        }
    }

    return 0;
}

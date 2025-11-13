// server.cpp
// Non-blocking, poll-based KV server with TLV serialization (Chapter 9)
// Commands:
//   get <key>        -> TAG_STR(value) or TAG_NIL
//   set <key> <val>  -> TAG_NIL
//   del <key>        -> TAG_INT(0|1)
//   keys             -> TAG_ARR(n) then n * TAG_STR(key)

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

#include "hashtable.h"   // intrusive chaining HT with progressive rehashing

// ---------------------------- utils ----------------------------
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

const size_t k_max_msg  = 32u << 20;       // 32 MB
const size_t k_max_args = 200u * 1000u;    // safety

// ----------------------- connection state ----------------------
struct Conn {
    int fd = -1;
    bool want_read  = false;
    bool want_write = false;
    bool want_close = false;

    std::vector<uint8_t> incoming;  // bytes to parse
    std::vector<uint8_t> outgoing;  // framed TLV responses
};

static inline void buf_append(std::vector<uint8_t> &b, const uint8_t *p, size_t n) {
    b.insert(b.end(), p, p + n);
}
static inline void buf_consume(std::vector<uint8_t> &b, size_t n) {
    b.erase(b.begin(), b.begin() + n);
}

// ----------------------- accept callback -----------------------
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

// ------------------------ protocol parse -----------------------
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

// Request body (unchanged):
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

// -------------------- TLV serialization (9.3) ------------------
using Buffer = std::vector<uint8_t>;

enum : uint8_t {
    TAG_NIL = 0,
    TAG_ERR = 1,   // error message: TAG_ERR + u32 len + bytes
    TAG_STR = 2,   // string: TAG_STR + u32 len + bytes
    TAG_INT = 3,   // int64: TAG_INT + i64
    TAG_DBL = 4,   // reserved
    TAG_ARR = 5,   // array: TAG_ARR + u32 n_items + items...
};

static inline void buf_append_u8(Buffer &buf, uint8_t v) {
    buf.push_back(v);
}
static inline void buf_append_u32(Buffer &buf, uint32_t v) {
    buf_append(buf, (const uint8_t*)&v, 4); // little-endian
}
static inline void buf_append_i64(Buffer &buf, int64_t v) {
    buf_append(buf, (const uint8_t*)&v, 8); // little-endian
}

static void out_nil(Buffer &out) {
    buf_append_u8(out, TAG_NIL);
}
static void out_str(Buffer &out, const char *s, size_t n) {
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)n);
    if (n) buf_append(out, (const uint8_t*)s, n);
}
static void out_int(Buffer &out, int64_t v) {
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, v);
}
static void out_arr(Buffer &out, uint32_t n_items) {
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n_items);
}
static void out_err_msg(Buffer &out, const char* m) {
    buf_append_u8(out, TAG_ERR);
    uint32_t mlen = (uint32_t)strlen(m);
    buf_append_u32(out, mlen);
    if (mlen) buf_append(out, (const uint8_t*)m, mlen);
}

// Outer 4-byte length prefix for each response message
static void response_begin(Buffer &out, size_t *header_pos) {
    *header_pos = out.size();
    buf_append_u32(out, 0); // reserve space
}
static size_t response_size(const Buffer &out, size_t header_pos) {
    return out.size() - header_pos - 4;
}
static void response_end(Buffer &out, size_t header_pos) {
    size_t body = response_size(out, header_pos);
    if (body > k_max_msg) {
        // Replace body with a small error message
        out.resize(header_pos + 4);
        out_err_msg(out, "response too big");
        body = response_size(out, header_pos);
    }
    uint32_t len_le = (uint32_t)body;
    memcpy(&out[header_pos], &len_le, 4);
}

// ------------------ Intrusive HT-backed database ----------------
struct Entry {
    HNode       node;
    std::string key;
    std::string val;
};
struct LookupKey {
    HNode       node;
    std::string key;
};
static bool key_eq(HNode *lhs, HNode *rhs) {
    Entry     *le = container_of(lhs, Entry,    node);
    LookupKey *rk = container_of(rhs, LookupKey, node);
    return le->key == rk->key;
}

static struct {
    HMap db;
} g_data;

// Iterate a single HTab with a plain C-style callback
typedef void (*htab_iter_cb)(HNode* node, void* arg);

static void for_each_htab_slot(HTab *t, htab_iter_cb cb, void* arg) {
    if (!t || !t->tab) return;
    size_t cap = t->mask + 1;
    for (size_t i = 0; i < cap; ++i) {
        for (HNode *n = t->tab[i]; n; n = n->next) {
            cb(n, arg);
        }
    }
}

static size_t ht_total_size(const HMap &m) {
    return (m.newer.size) + (m.older.tab ? m.older.size : 0);
}

// ------------------------ command logic ------------------------
static void do_get(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() != 2) { out_nil(out); return; }
    LookupKey lk;
    lk.key = std::move(cmd[1]);
    lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());
    if (HNode *n = hm_lookup(&g_data.db, &lk.node, &key_eq)) {
        Entry *e = container_of(n, Entry, node);
        out_str(out, e->val.data(), e->val.size());
    } else {
        out_nil(out);
    }
}
static void do_set(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() != 3) { out_nil(out); return; }

    LookupKey lk;
    lk.key = cmd[1];
    lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());
    if (HNode *n = hm_lookup(&g_data.db, &lk.node, &key_eq)) {
        Entry *e = container_of(n, Entry, node);
        e->val.swap(cmd[2]);
        out_nil(out);
        return;
    }
    Entry *e = new Entry();
    e->key.swap(cmd[1]);
    e->val.swap(cmd[2]);
    e->node.hcode = str_hash((const uint8_t*)e->key.data(), e->key.size());
    hm_insert(&g_data.db, &e->node);
    out_nil(out);
}
static void do_del(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() != 2) { out_int(out, 0); return; }
    LookupKey lk;
    lk.key = std::move(cmd[1]);
    lk.node.hcode = str_hash((const uint8_t*)lk.key.data(), lk.key.size());
    if (HNode *n = hm_delete(&g_data.db, &lk.node, &key_eq)) {
        Entry *e = container_of(n, Entry, node);
        delete e;
        out_int(out, 1);
    } else {
        out_int(out, 0);
    }
}
static void do_keys(std::vector<std::string> &cmd, Buffer &out) {
    (void)cmd;

    // Emit array header with current size snapshot
    uint32_t n = (uint32_t)ht_total_size(g_data.db);
    out_arr(out, n);

    // Emit keys from newer and older tables
    auto emit_key_cb = [](HNode* node, void* arg) {
        Buffer &bout = *reinterpret_cast<Buffer*>(arg);
        const std::string &k = container_of(node, Entry, node)->key;
        out_str(bout, k.data(), k.size());
    };
    for_each_htab_slot(&g_data.db.newer, emit_key_cb, &out);
    for_each_htab_slot(&g_data.db.older, emit_key_cb, &out);
}

static void do_request(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.empty()) { out_nil(out); return; }
    const std::string &op = cmd[0];
    if      (op == "get")  return do_get(cmd, out);
    else if (op == "set")  return do_set(cmd, out);
    else if (op == "del")  return do_del(cmd, out);
    else if (op == "keys") return do_keys(cmd, out);

    out_err_msg(out, "ERR bad command");
}

// --------------- per-connection request handling ---------------
static bool try_one_request(Conn *conn) {
    if (conn->incoming.size() < 4) return false;

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->want_close = true;
        return false;
    }
    if (conn->incoming.size() < 4 + len) return false;

    const uint8_t *body = &conn->incoming[4];
    std::vector<std::string> cmd;
    if (parse_req(body, len, cmd) < 0) {
        msg("bad request");
        conn->want_close = true;
        return false;
    }

    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);
    do_request(cmd, conn->outgoing);
    response_end(conn->outgoing, header_pos);

    buf_consume(conn->incoming, 4 + len);
    return true;
}

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
        // optimistic write
        handle_write(conn);
    }
}

// -------------------------- main loop --------------------------
int main() {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);  // FIXED: AF_INET
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

    // init DB
    hm_init(&g_data.db);

    std::vector<Conn*> fd2conn;
    std::vector<struct pollfd> pfds;

    while (true) {
        pfds.clear();
        pfds.push_back({lfd, POLLIN, 0}); // index 0

        for (Conn *c : fd2conn) {
            if (!c) continue;
            short ev = POLLERR;
            if (c->want_read)  ev |= POLLIN;
            if (c->want_write) ev |= POLLOUT;
            pfds.push_back({c->fd, ev, 0});
        }

        int rv = poll(pfds.data(), (nfds_t)pfds.size(), -1);
        if (rv < 0 && errno == EINTR) continue;
        if (rv < 0) die("poll()");

        if (pfds[0].revents) {
            if (Conn *c = handle_accept(lfd)) {
                if (fd2conn.size() <= (size_t)c->fd) fd2conn.resize(c->fd + 1, nullptr);
                assert(!fd2conn[c->fd]);
                fd2conn[c->fd] = c;
            }
        }

        for (size_t i = 1; i < pfds.size(); ++i) {
            uint32_t ready = pfds[i].revents;
            if (!ready) continue;

            Conn *c = fd2conn[pfds[i].fd];
            if (!c) continue;

            if (ready & POLLIN)  { assert(c->want_read);  handle_read(c); }
            if (ready & POLLOUT) { assert(c->want_write); handle_write(c); }
            if ((ready & POLLERR) || c->want_close) {
                (void)close(c->fd);
                fd2conn[c->fd] = nullptr;
                delete c;
            }
        }
    }
    return 0;
}

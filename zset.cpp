// zset.cpp
#include "zset.h"
#include <string.h>
#include <assert.h>

static ZNode* znode_new(const char *name, size_t len, double score) {
    ZNode *node = (ZNode*)malloc(sizeof(ZNode) + len);
    avl_init(&node->tree);
    node->hmap.next  = nullptr;
    node->hmap.hcode = str_hash((const uint8_t*)name, len);
    node->score = score;
    node->len   = len;
    memcpy(node->name, name, len);
    return node;
}

static void znode_del(ZNode *node) {
    free(node);
}

void zset_init(ZSet *zs) {
    zs->root = nullptr;
    hm_init(&zs->hmap);
}

static void zset_clear_rec(AVLNode *root) {
    if (!root) return;
    zset_clear_rec(root->left);
    zset_clear_rec(root->right);
    ZNode *z = container_of(root, ZNode, tree);
    znode_del(z);
}

void zset_clear(ZSet *zs) {
    if (zs->root) {
        zset_clear_rec(zs->root);
        zs->root = nullptr;
    }
    hm_destroy(&zs->hmap);
}

// --------------- Hashtable key wrapper ---------------

struct HKey {
    HNode       node;
    const char *name = nullptr;
    size_t      len  = 0;
};

static bool hcmp(HNode *lhs, HNode *rhs) {
    ZNode *znode = container_of(lhs, ZNode, hmap);
    HKey  *hkey  = container_of(rhs, HKey, node);
    if (znode->len != hkey->len) return false;
    return ::memcmp(znode->name, hkey->name, znode->len) == 0;
}

ZNode *zset_lookup(ZSet *zset, const char *name, size_t len) {
    if (!zset->root) return nullptr;
    HKey key;
    key.node.hcode = str_hash((const uint8_t*)name, len);
    key.name = name;
    key.len  = len;
    HNode *found = hm_lookup(&zset->hmap, &key.node, &hcmp);
    return found ? container_of(found, ZNode, hmap) : nullptr;
}

// --------------- Tree comparison helpers ---------------

// (lhs.score, lhs.name) < (rhs.score, rhs.name)
static bool zless_node(AVLNode *lhs, AVLNode *rhs) {
    ZNode *zl = container_of(lhs, ZNode, tree);
    ZNode *zr = container_of(rhs, ZNode, tree);
    if (zl->score != zr->score) {
        return zl->score < zr->score;
    }
    size_t minlen = zl->len < zr->len ? zl->len : zr->len;
    int rv = ::memcmp(zl->name, zr->name, minlen);
    if (rv != 0) return rv < 0;
    return zl->len < zr->len;
}

// (node.score, node.name) < (score, name)
static bool zless_key(AVLNode *node, double score, const char *name, size_t len) {
    ZNode *zn = container_of(node, ZNode, tree);
    if (zn->score != score) {
        return zn->score < score;
    }
    size_t minlen = zn->len < len ? zn->len : len;
    int rv = ::memcmp(zn->name, name, minlen);
    if (rv != 0) return rv < 0;
    return zn->len < len;
}

static void tree_insert(ZSet *zset, ZNode *node) {
    AVLNode *parent = nullptr;
    AVLNode **from = &zset->root;
    AVLNode *cur = zset->root;

    while (cur) {
        parent = cur;
        if (zless_node(&node->tree, cur)) {
            from = &cur->left;
            cur  = cur->left;
        } else {
            from = &cur->right;
            cur  = cur->right;
        }
    }

    *from = &node->tree;
    node->tree.parent = parent;
    avl_update(&node->tree);
    zset->root = avl_fix(&node->tree);
}

static void zset_update(ZSet *zset, ZNode *node, double score) {
    // detach from tree
    zset->root = avl_del(&node->tree);
    avl_init(&node->tree);
    node->score = score;
    // reinsert
    tree_insert(zset, node);
}

bool zset_insert(ZSet *zset, const char *name, size_t len, double score) {
    if (ZNode *node = zset_lookup(zset, name, len)) {
        zset_update(zset, node, score);
        return false; // updated existing
    }
    ZNode *node = znode_new(name, len, score);
    hm_insert(&zset->hmap, &node->hmap);
    tree_insert(zset, node);
    return true; // inserted new
}

void zset_delete(ZSet *zset, ZNode *node) {
    // remove from hashtable
    HKey key;
    key.node.hcode = node->hmap.hcode;
    key.name = node->name;
    key.len  = node->len;
    HNode *found = hm_delete(&zset->hmap, &key.node, &hcmp);
    (void)found;
    assert(found == &node->hmap);

    // remove from tree
    zset->root = avl_del(&node->tree);
    znode_del(node);
}

// --------------- Range & rank operations ---------------

ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len) {
    AVLNode *found = nullptr;
    AVLNode *cur = zset->root;
    while (cur) {
        if (zless_key(cur, score, name, len)) {
            cur = cur->right; // cur < key
        } else {
            found = cur;      // candidate
            cur = cur->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : nullptr;
}

ZNode *znode_offset(ZNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_offset(&node->tree, offset) : nullptr;
    return tnode ? container_of(tnode, ZNode, tree) : nullptr;
}

int64_t znode_rank(ZNode *node) {
    if (!node) return -1;
    return avl_rank(&node->tree);
}

// zset.h
#pragma once
#include <stddef.h>
#include <stdint.h>
#include "avl.h"
#include "hashtable.h"

struct ZSet;

// Node for one (score, name) pair
struct ZNode {
    AVLNode tree;   // AVL index by (score, name)
    HNode   hmap;   // hash index by name
    double  score = 0;
    size_t  len   = 0;
    char    name[0];  // flexible array
};

struct ZSet {
    AVLNode *root = nullptr; // AVL tree root
    HMap     hmap;           // hashtable index by name
};

// Initialization / cleanup
void   zset_init(ZSet *zs);
void   zset_clear(ZSet *zs);

// Point operations
bool   zset_insert(ZSet *zset, const char *name, size_t len, double score);
// returns nullptr if not found
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
void   zset_delete(ZSet *zset, ZNode *node);

// Range / rank helpers
ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len);
ZNode *znode_offset(ZNode *node, int64_t offset);
int64_t znode_rank(ZNode *node);

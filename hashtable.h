// hashtable.h
#pragma once
#include <stddef.h>     // offsetof
#include <stdint.h>
#include <stdlib.h>

struct HNode {
    HNode*    next  = nullptr;
    uint64_t  hcode = 0;    // precomputed hash
};

struct HTab {
    HNode** tab = nullptr;  // slot array
    size_t  mask = 0;       // size = mask+1; power of 2
    size_t  size = 0;       // number of nodes
};

typedef bool (*h_eq_fn)(HNode*, HNode*);

// basic table helpers
void   h_init(HTab* ht, size_t n);
void   h_destroy(HTab* ht);
HNode* h_detach(HTab* ht, HNode** from);
HNode** h_lookup(HTab* ht, HNode* key, h_eq_fn eq);
void   h_insert(HTab* ht, HNode* node);

// incremental rehashing map
struct HMap {
    HTab   newer;
    HTab   older;
    size_t resizing_pos = 0;
};

void   hm_init(HMap* hmap);
void   hm_destroy(HMap* hmap);
void   hm_insert(HMap* hmap, HNode* node);
HNode* hm_lookup(HMap* hmap, HNode* key, h_eq_fn eq);
HNode* hm_delete(HMap* hmap, HNode* key, h_eq_fn eq);

// FNV-1a hash for strings
static inline uint64_t str_hash(const uint8_t* p, size_t n) {
    const uint64_t FNV_OFFSET = 1469598103934665603ull;
    const uint64_t FNV_PRIME  = 1099511628211ull;
    uint64_t h = FNV_OFFSET;
    for (size_t i = 0; i < n; ++i) {
        h ^= (uint64_t)p[i];
        h *= FNV_PRIME;
    }
    return h;
}

// Standard intrusive helper if not already defined
#ifndef container_of
#define container_of(ptr, T, member) \
    ((T*)((char*)(ptr) - offsetof(T, member)))
#endif

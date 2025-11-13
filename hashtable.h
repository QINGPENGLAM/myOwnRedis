// hashtable.h
#pragma once
#include <stddef.h>     // offsetof
#include <stdint.h>
#include <stdlib.h>

struct HNode {
    HNode*    next = nullptr;
    uint64_t  hcode = 0;    // precomputed hash
};

struct HTab {
    HNode** tab = nullptr;  // slot array
    size_t  mask = 0;       // size = mask+1; power of 2
    size_t  size = 0;       // number of nodes
};

struct HMap {
    HTab   newer{};         // active table
    HTab   older{};         // migrating-from table
    size_t migrate_pos = 0; // next slot to migrate
};

// Intrusive equality callback signature
using h_eq_fn = bool(*)(HNode*, HNode*);

// Public API
void     h_init(HTab* ht, size_t n);                 // n must be power of 2
void     h_insert(HTab* ht, HNode* node);
HNode**  h_lookup(HTab* ht, HNode* key, h_eq_fn eq); // returns &incoming_ptr
HNode*   h_detach(HTab* ht, HNode** from);

void     hm_init(HMap* hmap);                        // optional helper
HNode*   hm_lookup(HMap* hmap, HNode* key, h_eq_fn eq);
void     hm_insert(HMap* hmap, HNode* node);
HNode*   hm_delete(HMap* hmap, HNode* key, h_eq_fn eq);

// 64-bit FNV-1a hash for byte spans
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

// Standard intrusive helper
#define container_of(ptr, T, member) \
    ((T*)((char*)(ptr) - offsetof(T, member)))

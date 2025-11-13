// hashtable.cpp
#include "hashtable.h"
#include <assert.h>
#include <string.h>
#include "hashtable.h"


static inline bool is_pow2(size_t n) { return n && ((n & (n - 1)) == 0); }

void h_init(HTab* ht, size_t n) {
    assert(is_pow2(n));
    ht->tab  = (HNode**)calloc(n, sizeof(HNode*)); // calloc for lazy-zero pages
    ht->mask = n - 1;
    ht->size = 0;
}

void h_insert(HTab* ht, HNode* node) {
    size_t pos = (size_t)(node->hcode & ht->mask);
    node->next = ht->tab[pos];
    ht->tab[pos] = node;
    ht->size++;
}

HNode** h_lookup(HTab* ht, HNode* key, h_eq_fn eq) {
    if (!ht || !ht->tab) return nullptr;
    size_t pos = (size_t)(key->hcode & ht->mask);
    HNode** from = &ht->tab[pos];
    for (HNode* cur; (cur = *from) != nullptr; from = &cur->next) {
        if (cur->hcode == key->hcode && eq(cur, key)) {
            return from; // address of incoming pointer to the target
        }
    }
    return nullptr;
}

HNode* h_detach(HTab* ht, HNode** from) {
    HNode* node = *from;
    *from = node->next;
    node->next = nullptr;
    ht->size--;
    return node;
}

// ---------------- Resizable map with progressive rehash ----------------

static const size_t k_max_load_factor  = 8;   // chaining → >1 is fine
static const size_t k_rehashing_work   = 128; // constant work per assist

static void hm_help_rehashing(HMap* hmap) {
    if (!hmap->older.tab) return;
    size_t moved = 0;
    while (moved < k_rehashing_work && hmap->older.size > 0) {
        if (hmap->migrate_pos > hmap->older.mask) {
            // no more slots (safety)
            break;
        }
        HNode** slot = &hmap->older.tab[hmap->migrate_pos];
        if (!*slot) {
            hmap->migrate_pos++;
            continue;
        }
        HNode* node = h_detach(&hmap->older, slot);
        h_insert(&hmap->newer, node);
        moved++;
    }
    if (hmap->older.size == 0) {
        free(hmap->older.tab);
        hmap->older = HTab{};
    }
}

static void hm_trigger_rehashing(HMap* hmap) {
    // Move current newer → older; allocate doubled newer
    hmap->older = hmap->newer;
    h_init(&hmap->newer, (hmap->older.mask + 1) * 2);
    hmap->migrate_pos = 0;
}

void hm_init(HMap* hmap) {
    *hmap = HMap{};
    h_init(&hmap->newer, 4); // small power-of-2 to start
}

HNode* hm_lookup(HMap* hmap, HNode* key, h_eq_fn eq) {
    hm_help_rehashing(hmap);
    if (HNode** from = h_lookup(&hmap->newer, key, eq)) return *from;
    if (HNode** from = h_lookup(&hmap->older, key, eq)) return *from;
    return nullptr;
}

void hm_insert(HMap* hmap, HNode* node) {
    if (!hmap->newer.tab) h_init(&hmap->newer, 4);
    h_insert(&hmap->newer, node);
    if (!hmap->older.tab) {
        size_t capacity = hmap->newer.mask + 1;
        if (hmap->newer.size >= capacity * k_max_load_factor) {
            hm_trigger_rehashing(hmap);
        }
    }
    hm_help_rehashing(hmap);
}

HNode* hm_delete(HMap* hmap, HNode* key, h_eq_fn eq) {
    hm_help_rehashing(hmap);
    if (HNode** from = h_lookup(&hmap->newer, key, eq)) {
        return h_detach(&hmap->newer, from);
    }
    if (HNode** from = h_lookup(&hmap->older, key, eq)) {
        return h_detach(&hmap->older, from);
    }
    return nullptr;
}

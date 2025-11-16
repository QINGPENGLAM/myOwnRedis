// hashtable.cpp
#include "hashtable.h"
#include <assert.h>
#include <string.h>

static inline bool is_pow2(size_t n) { return n && ((n & (n - 1)) == 0); }

void h_init(HTab* ht, size_t n) {
    assert(is_pow2(n));
    ht->tab  = (HNode**)calloc(n, sizeof(HNode*));
    ht->mask = n - 1;
    ht->size = 0;
}

void h_destroy(HTab* ht) {
    free(ht->tab);
    ht->tab  = nullptr;
    ht->mask = 0;
    ht->size = 0;
}

HNode** h_lookup(HTab* ht, HNode* key, h_eq_fn eq) {
    if (!ht->tab) return nullptr;
    size_t idx = (size_t)key->hcode & ht->mask;
    HNode** from = &ht->tab[idx];
    for (HNode* cur = *from; cur; cur = cur->next) {
        if (cur->hcode == key->hcode && eq(cur, key)) {
            return from;
        }
        from = &cur->next;
    }
    return nullptr;
}

void h_insert(HTab* ht, HNode* node) {
    size_t idx = (size_t)node->hcode & ht->mask;
    node->next = ht->tab[idx];
    ht->tab[idx] = node;
    ht->size++;
}

HNode* h_detach(HTab* ht, HNode** from) {
    HNode* node = *from;
    *from = node->next;
    ht->size--;
    return node;
}

// --------------------- HMap (incremental rehashing) ---------------------

static void hm_start_resizing(HMap* hmap) {
    assert(hmap->newer.tab && !hmap->older.tab);
    size_t n = (hmap->newer.mask + 1) * 2;
    if (n < 4) n = 4;
    h_init(&hmap->older, n);
    hmap->resizing_pos = 0;
}

static void hm_help_rehashing(HMap* hmap) {
    if (!hmap->older.tab) return; // not resizing

    size_t nmove = 0;
    // move a few buckets each time we touch the table
    while (nmove < 64 && hmap->resizing_pos <= hmap->newer.mask) {
        HNode* node = hmap->newer.tab[hmap->resizing_pos];
        hmap->newer.tab[hmap->resizing_pos] = nullptr;
        while (node) {
            HNode* next = node->next;
            h_insert(&hmap->older, node);
            node = next;
        }
        hmap->resizing_pos++;
        nmove++;
    }

    if (hmap->resizing_pos > hmap->newer.mask) {
        // done
        h_destroy(&hmap->newer);
        hmap->newer = hmap->older;
        hmap->older.tab = nullptr;
        hmap->older.mask = 0;
        hmap->older.size = 0;
        hmap->resizing_pos = 0;
    }
}

void hm_init(HMap* hmap) {
    memset(hmap, 0, sizeof(*hmap));
    h_init(&hmap->newer, 4);
}

void hm_destroy(HMap* hmap) {
    h_destroy(&hmap->newer);
    h_destroy(&hmap->older);
    hmap->resizing_pos = 0;
}

static HTab* hm_primary(HMap* hmap) {
    return &hmap->newer;
}

static void hm_maybe_start_resizing(HMap* hmap) {
    HTab* ht = hm_primary(hmap);
    if (ht->size >= (ht->mask + 1) * 2) { // load factor 2.0
        hm_start_resizing(hmap);
    }
}

void hm_insert(HMap* hmap, HNode* node) {
    hm_help_rehashing(hmap);
    HTab* ht = hm_primary(hmap);
    h_insert(ht, node);
    hm_maybe_start_resizing(hmap);
}

HNode* hm_lookup(HMap* hmap, HNode* key, h_eq_fn eq) {
    hm_help_rehashing(hmap);
    if (HNode** from = h_lookup(&hmap->newer, key, eq)) {
        return *from;
    }
    if (hmap->older.tab) {
        if (HNode** from = h_lookup(&hmap->older, key, eq)) {
            return *from;
        }
    }
    return nullptr;
}

HNode* hm_delete(HMap* hmap, HNode* key, h_eq_fn eq) {
    hm_help_rehashing(hmap);
    if (HNode** from = h_lookup(&hmap->newer, key, eq)) {
        return h_detach(&hmap->newer, from);
    }
    if (hmap->older.tab) {
        if (HNode** from = h_lookup(&hmap->older, key, eq)) {
            return h_detach(&hmap->older, from);
        }
    }
    return nullptr;
}

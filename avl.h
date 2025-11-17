// avl.h
#pragma once
#include <stdint.h>
#include <stddef.h>
#include <assert.h>

#ifndef container_of
#define container_of(ptr, T, member) ((T*)((char*)(ptr) - offsetof(T, member)))
#endif

struct AVLNode {
    AVLNode *parent = nullptr;
    AVLNode *left   = nullptr;
    AVLNode *right  = nullptr;
    uint32_t height = 0;   // 0 for null, 1 for leaf
    uint32_t cnt    = 0;   // subtree size (order statistic)
};

inline void avl_init(AVLNode *n) {
    n->parent = n->left = n->right = nullptr;
    n->height = 1;
    n->cnt    = 1;
}

inline uint32_t avl_height(AVLNode *n) { return n ? n->height : 0u; }
inline uint32_t avl_cnt   (AVLNode *n) { return n ? n->cnt    : 0u; }

inline void avl_update(AVLNode *n) {
    uint32_t hl = avl_height(n->left);
    uint32_t hr = avl_height(n->right);
    n->height = 1u + (hl > hr ? hl : hr);
    n->cnt    = 1u + avl_cnt(n->left) + avl_cnt(n->right);
}

// Rotations (return new subtree root; caller fixes parent link)
AVLNode* avl_rot_left (AVLNode *x);
AVLNode* avl_rot_right(AVLNode *y);

// Rebalance helpers when one side is 2 taller
AVLNode* avl_fix_left (AVLNode *n);
AVLNode* avl_fix_right(AVLNode *n);

// Rebalance up to the root starting from node `n`. Returns new root.
AVLNode* avl_fix(AVLNode *n);

// Delete `node` from the tree. Returns new root.
AVLNode* avl_del(AVLNode *node);

// Search+insert and search+delete helpers
void     avl_search_and_insert(AVLNode **root, AVLNode *new_node,
                               bool (*less)(AVLNode*, AVLNode*));
AVLNode* avl_search_and_delete(AVLNode **root,
                               int32_t (*cmp)(AVLNode*, void*), void *key);

// In-order iteration
AVLNode* avl_first(AVLNode *root);
AVLNode* avl_next(AVLNode *n);

// Order-statistic helpers (Chapter 11)
// Offset from `node` by `offset` positions in sorted order (0 = same node).
// Returns nullptr if going out of range.
AVLNode* avl_offset(AVLNode *node, int64_t offset);

// Return 0-based rank of `node` in sorted order. Returns -1 if node is null.
int64_t  avl_rank(AVLNode *node);

// test_avl.cpp
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <random>
#include "avl.h"

struct Item {
    int key;
    AVLNode node;
};

static bool less_node(AVLNode *a, AVLNode *b) {
    return container_of(a, Item, node)->key < container_of(b, Item, node)->key;
}
static int32_t cmp_node_key(AVLNode *n, void *kptr) {
    int k = *(int*)kptr;
    int nk = container_of(n, Item, node)->key;
    return (nk < k) ? -1 : (nk > k ? 1 : 0);
}

static bool verify_avl(AVLNode *root) {
    // inorder strictly increasing
    int prev = -2147483648;
    for (AVLNode *it = avl_first(root); it; it = avl_next(it)) {
        int k = container_of(it, Item, node)->key;
        if (!(prev < k)) return false;
        prev = k;
    }
    // local AVL invariant and height correctness
    std::vector<AVLNode*> stack;
    if (root) stack.push_back(root);
    while (!stack.empty()) {
        AVLNode *n = stack.back(); stack.pop_back();
        uint32_t hl = avl_height(n->left), hr = avl_height(n->right);
        if (!(hl <= hr+1 && hr <= hl+1)) return false;
        if (n->height != 1u + (hl > hr ? hl : hr)) return false;
        if (n->left)  { if (n->left->parent  != n) return false; stack.push_back(n->left); }
        if (n->right) { if (n->right->parent != n) return false; stack.push_back(n->right); }
    }
    return true;
}

int main() {
    const int N = 5000;
    std::mt19937 rng(12345);
    std::vector<int> keys(N);
    for (int i=0;i<N;++i) keys[i]=i+1;
    std::shuffle(keys.begin(), keys.end(), rng);

    std::vector<Item*> pool; pool.reserve(N);
    AVLNode *root = nullptr;

    // insert
    for (int k: keys) {
        Item *it = new Item{ k, {} };
        avl_search_and_insert(&root, &it->node, &less_node);
        pool.push_back(it);
    }
    assert(verify_avl(root));

    // delete half
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int i=0;i<N/2;i++) {
        int k = keys[i];
        AVLNode *det = avl_search_and_delete(&root, &cmp_node_key, &k);
        assert(det);
        delete container_of(det, Item, node);
    }
    assert(verify_avl(root));

    // print first 10 keys
    int cnt = 0;
    for (AVLNode *it = avl_first(root); it && cnt < 10; it = avl_next(it), ++cnt) {
        int k = container_of(it, Item, node)->key;
        std::printf("%d ", k);
    }
    std::puts("\nOK");
    
    return 0;
}

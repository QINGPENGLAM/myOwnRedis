// test_offset.cpp
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

int main() {
    std::mt19937_64 rng{12345};
    const int N = 1000;

    AVLNode *root = nullptr;
    std::vector<Item*> items;
    items.reserve(N);

    for (int i = 0; i < N; ++i) {
        Item *it = new Item;
        it->key = (int)(rng() % 100000);
        avl_init(&it->node);
        avl_search_and_insert(&root, &it->node, &less_node);
        items.push_back(it);
    }

    assert(verify_avl(root));

    // Gather nodes in sorted order
    std::vector<AVLNode*> inorder;
    for (AVLNode *p = avl_first(root); p; p = avl_next(p)) {
        inorder.push_back(p);
    }

    // sanity: size
    assert((int)inorder.size() <= N);

    // Check avl_rank matches position
    for (size_t i = 0; i < inorder.size(); ++i) {
        int64_t r = avl_rank(inorder[i]);
        assert(r == (int64_t)i);
    }

    // Check avl_offset from each node within +/- 10 steps
    int max_step = 10;
    for (size_t i = 0; i < inorder.size(); ++i) {
        for (int d = -max_step; d <= max_step; ++d) {
            int64_t j = (int64_t)i + d;
            AVLNode *from = inorder[i];
            AVLNode *to = avl_offset(from, d);
            if (j < 0 || j >= (int64_t)inorder.size()) {
                assert(to == nullptr);
            } else {
                assert(to == inorder[(size_t)j]);
            }
        }
    }

    std::puts("OK");
    return 0;
}

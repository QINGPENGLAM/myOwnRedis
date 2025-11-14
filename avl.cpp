// avl.cpp
#include "avl.h"

AVLNode* avl_rot_left(AVLNode *x) {
    AVLNode *p = x->parent;
    AVLNode *y = x->right;
    if (!y) return x;
    AVLNode *B = y->left;

    y->left = x;     x->parent = y;
    x->right = B;    if (B) B->parent = x;
    y->parent = p;

    avl_update(x);
    avl_update(y);
    return y;
}

AVLNode* avl_rot_right(AVLNode *y) {
    AVLNode *p = y->parent;
    AVLNode *x = y->left;
    if (!x) return y;
    AVLNode *B = x->right;

    x->right = y;    y->parent = x;
    y->left  = B;    if (B) B->parent = y;
    x->parent = p;

    avl_update(y);
    avl_update(x);
    return x;
}

AVLNode* avl_fix_left(AVLNode *n) {
    if (avl_height(n->left->left) < avl_height(n->left->right)) {
        AVLNode *L = n->left;
        AVLNode *L2 = avl_rot_left(L);
        n->left = L2; L2->parent = n;
    }
    return avl_rot_right(n);
}

AVLNode* avl_fix_right(AVLNode *n) {
    if (avl_height(n->right->right) < avl_height(n->right->left)) {
        AVLNode *R = n->right;
        AVLNode *R2 = avl_rot_right(R);
        n->right = R2; R2->parent = n;
    }
    return avl_rot_left(n);
}

AVLNode* avl_fix(AVLNode *n) {
    while (n) {
        AVLNode *parent = n->parent;
        AVLNode **from = parent ? (parent->left == n ? &parent->left : &parent->right) : &n;

        avl_update(n);
        uint32_t hl = avl_height(n->left), hr = avl_height(n->right);
        if (hl == hr + 2) {
            *from = avl_fix_left(n);
            (*from)->parent = parent;
        } else if (hr == hl + 2) {
            *from = avl_fix_right(n);
            (*from)->parent = parent;
        }

        if (!parent) return *from; // reached root; return new root
        n = parent;
    }
    return nullptr;
}

static AVLNode* avl_del_easy(AVLNode *node) {
    AVLNode *child  = node->left ? node->left : node->right; // may be null
    AVLNode *parent = node->parent;

    if (child) child->parent = parent;
    if (!parent) { if (child) avl_update(child); return child; }

    AVLNode **from = (parent->left == node) ? &parent->left : &parent->right;
    *from = child;
    return avl_fix(parent);
}

AVLNode* avl_del(AVLNode *node) {
    if (!node->left || !node->right) {
        return avl_del_easy(node);
    }

    // successor: leftmost of right subtree
    AVLNode *s = node->right;
    while (s->left) s = s->left;

    // detach successor (easy case) – returns new root after removal
    AVLNode *root = avl_del_easy(s);

    // move node's children/parent onto s
    s->left  = node->left;   if (s->left)  s->left->parent  = s;
    s->right = node->right;  if (s->right) s->right->parent = s;
    s->parent = node->parent;

    if (!s->parent) {
        avl_update(s);
        return avl_fix(s);
    } else {
        AVLNode **from = (s->parent->left == node) ? &s->parent->left : &s->parent->right;
        *from = s;
        avl_update(s);
        return avl_fix(s);
    }
}

void avl_search_and_insert(AVLNode **root, AVLNode *new_node,
                           bool (*less)(AVLNode*, AVLNode*)) {
    avl_init(new_node);
    AVLNode *parent = nullptr;
    AVLNode **from = root;
    AVLNode *cur = *root;

    while (cur) {
        parent = cur;
        if (less(new_node, cur)) { from = &cur->left;  cur = cur->left;  }
        else                      { from = &cur->right; cur = cur->right; }
    }
    *from = new_node;
    new_node->parent = parent;

    *root = avl_fix(new_node);
}

AVLNode* avl_search_and_delete(AVLNode **root,
                               int32_t (*cmp)(AVLNode*, void*), void *key) {
    AVLNode *cur = *root;
    while (cur) {
        int32_t r = cmp(cur, key);
        if (r < 0)       cur = cur->right;
        else if (r > 0)  cur = cur->left;
        else {
            AVLNode *victim = cur;
            *root = avl_del(cur);
            return victim;
        }
    }
    return nullptr;
}

AVLNode* avl_first(AVLNode *root) {
    if (!root) return nullptr;
    AVLNode *n = root;
    while (n->left) n = n->left;
    return n;
}

AVLNode* avl_next(AVLNode *n) {
    if (!n) return nullptr;
    if (n->right) {
        n = n->right;
        while (n->left) n = n->left;
        return n;
    }
    AVLNode *p = n->parent;
    while (p && p->right == n) { n = p; p = p->parent; }
    return p;
}

#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "rbtree.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

RBTree::RBTree(size_t capacity, Node* root): root(root), max_size{capacity} {
    curr_size = 0;
    min_key = numeric_limits<int64_t>::max();
    max_key = numeric_limits<int64_t>::min();
}
RBTree::~RBTree() {
    #ifdef DEBUG
        cout << "Deleting tree..." << endl;
    #endif
    
    delete root;
    root = nullptr;
}

/* Insert or update key-value pair into the memtable */
Result RBTree::put(const int64_t& key, const int64_t& value) {
    // Check if current tree size reaches maximum, and write to SST
    if (curr_size >= max_size) {
        return memtableFull;
    }

    // Otherwise, insert a new node
    Node* node = new Node(key, value);
    insertNode(node);
    return allGood;
}

/* Retrieve a value by key */
Result RBTree::get(int64_t*& result, const int64_t& key) {
    // Search for the key in the Red-Black Tree
    Node* node = search(root, key);

    if (node == nullptr) {
        #ifdef DEBUG
            cout << "Not found Key: " << key << " in memtable. Now searching SSTs..." << endl;
        #endif
        return notInMemtable;
    } else {
        result = new int64_t(node->value);
    }
    return allGood;
}

/* Search matching key in memtable */
Node* RBTree::search(Node* root, const int64_t& key) {
    if (root == nullptr || root->key == key) {
        return root;
    }

    if (key < root->key) {
        return search(root->left, key);
    } else {
        return search(root->right, key);
    }
}

/* Scan the memtable and SST to retrieve all KV-pairs in a key range in key order (key1 < key2) */
void RBTree::scan(vector<pair<int64_t, int64_t>>& sorted_KV, const Node* root, const int64_t& key1, const int64_t& key2) {
    if (root != nullptr) {
        scan(sorted_KV, root->left, key1, key2);
        // Only include KV-pairs that are in the range
        if (key1 <= root->key && root->key <= key2) {
            sorted_KV.emplace_back(root->key, root->value);
        }
        scan(sorted_KV, root->right, key1, key2);
    }
}


/*
 * Helper function to perform left rotation
 *
 * Simple illustration for left rotation on 'x':
 *
 *    root                            root
 *     /                               /
 *    x                               y
 *   /  \          ------>           / \
 *  lx   y                          x  ry
 *     /   \                       /  \
 *    ly   ry                     lx  ly
 *
 *  After left rotation on 'x', 'y' become the new root of the subtree with left child as x,
 *  and the previous left-child of 'y' will become the right-child of 'x'.
 *  Note that, the left-child of x and right-child of y remain the same.
 *  lx, ly, ry can be nullptr.
 */
void RBTree::rotateLeft(Node* x) {
    /* Step 1: mark the right-child of x as y. */
    Node *y = x->right;

    /* Step 2: the left-child of y become the right-child of x, and update child's parent if not null */
    x->right = y->left;
    if (y->left != NULL) {
        y->left->parent = x;
    }

    /* Step 3: set x and y parents accordingly */
    y->parent = x->parent;
    if (x->parent == NULL) {
        root = y;               // x is root
    } else if (x->parent->left == x) {
        x->parent->left = y;    // x is the left child of its parent
    } else {
        x->parent->right = y;    // x is the right child of its parent
    }

    /* Step 4: lastly, set the left-child of y as x and set x's parent to y */
    y->left = x;
    x->parent = y;
}

/*
 * Helper function to perform right rotation
 *
 * Simple illustration for right rotation on 'x':
 *
 *       root                             root
 *        /                                /
 *       x                                y                
 *      /  \        -------->            /  \
 *     y   rx                           ly   x 
 *    / \                                   / \
 *  ly  ry                                ry  rx
 *
 * Right rotation is symmetrical to left rotation.
 */
void RBTree::rotateRight(Node* x) {
    /* Step 1: mark the left-child of x as y. */
    Node *y = x->left;

    /* Step 2: the right-child of y become the left-child of x, and update child's parent if not null */
    x->left = y->right;
    if (y->right != NULL) {
        y->right->parent = x;
    }

    /* Step 3: set x and y parents accordingly */
    y->parent = x->parent;
    if (x->parent == NULL) {
        root = y;               // x is root
    } else if (x->parent->left == x) {
        x->parent->left = y;    // x is the left child of its parent
    } else {
        x->parent->right = y;    // x is the right child of its parent
    }

    /* Step 4: lastly, set the right-child of y as x and set x's parent to y */
    y->right = x;
    x->parent = y;
}

/* 
 * Fix Red-Black tree properties violations after insertion.
 *
 * For insertion, possible proporties violations are:
 * 1. The root is black.
 * 2. If a node is red, then both of its children are black.
 * 
 * Since we color newly inserted node to red, so property #1 will be violated 
 * when we insert a root. We can fix it by simply color the root black.
 * 
 * Property #2 (if a node is red, then both its children are black) will violate 
 * when the parent of the inserted node is red. Note that, in the case of violation
 * property #2, the grandparent of the inserted node must be black. 
 * 
 * There can be 6 different cases:
 * Cases 1-3 are when the parent of the node is a left child of its parent 
 * Cases 4-6 are vice-versa, when the parent of the node is a right child of its parent
 * (cases 4-6 are symmetric to cases 1-3)
 * 
 * More detail information can be found: https://www.codesdope.com/course/data-structures-red-black-trees-insertion/
 * 
 */
void RBTree::insertFixup(Node* node) {
    Node* y;
    while (node->parent && node->parent->color == red) {
        if (node->parent == node->parent->parent->left) {
            /* Cases 1-3: the parent of the node is a left child of its parent */
            y = node->parent->parent->right; // uncle of node
            if (y && y->color == red) {
                /* Case 1: When uncle of node is also red:
                * shift the red color upward until there is no violation,
                * the black height of any node won't be affected.
                */
                node->parent->color = black;
                y->color = black;
                node->parent->parent->color = red;
                node = node->parent->parent;
                continue;
            } else {
                /* Case 2 and 3: 
                * the uncle of the node is black and the node is the right/left child. 
                */
                if (node->parent->right == node) {
                    /* transform the case 2 into 3 by performing left rotation */
                    node = node->parent; 
                    rotateLeft(node);
                }

                /* color parent black and grandparent red, and right rotation on the grandparent. */
                node->parent->color = black;
                node->parent->parent->color = red;
                rotateRight(node->parent->parent);
            }
        } else {
            /* Cases 4-6: the parent of the node is a right child of its parent. 
            * Code is symmetric as above.
            */
            y = node->parent->parent->left;
            if (y && y->color == red) {
                node->parent->color = black;
                y->color = black;
                node->parent->parent->color = red;
                node = node->parent->parent;
                continue;
            } else {
                if (node->parent->left == node) {
                    node = node->parent; 
                    rotateRight(node);
                }
                node->parent->color = black;
                node->parent->parent->color = red;
                rotateLeft(node->parent->parent);
            }
        }
    }
    root->color = black;
}

/* Insert a node into the Red-Black Tree */
void RBTree::insertNode(Node* node) {
    // Perform a standard Binary Search Tree insertion
    Node* y = nullptr;
    Node* x = root;

    while (x != nullptr) {
        y = x;
        if (node->key == x->key) {
            x->value = node->value;
            delete node;
            return;
        }
        else if (node->key < x->key)
            x = x->left;
        else
            x = x->right;
    }

    // Set the parent of the new node
    node->parent = y;

    if (y == nullptr) {
        // If the tree is empty, make the new node the root
        root = node;
    } else if (node->key < y->key) {
        // Otherwise, insert the new node as a left child if the key is smaller
        y->left = node;
    } else {
        // Insert as a right child otherwise
        y->right = node;
    }

    // Fix any Red-Black Tree violations that may have been introduced by the insertion
    node->color = red;
    insertFixup(node);

    // Update tree metadata
    curr_size++;
    if (node->key < min_key) min_key = node->key;
    if (node->key > max_key) max_key = node->key;

    #ifdef DEBUG
        cout << "Insert key: " << node->key << " value: " << node->value << endl;
    #endif
}
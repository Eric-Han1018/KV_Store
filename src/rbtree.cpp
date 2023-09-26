#include <iostream>
#include "rbtree.h"
#include <map>
#include <list>
using namespace std;

// TODO: Insert a key-value pair into the memtable
void RBTree::put(const double& key, const double& value) {
    // Check if the key already exists and update its value (?)

    // Otherwise, insert a new node
    Node* node = new Node(key, value);
    insertNode(node);

    // TODO: Implement the logic to handle rotation and capacity

}

// Retrieve a value by key
double RBTree::get(const double& key) {
    // Search for the key in the Red-Black Tree
    Node* node = search(root, key);

    if (node == nullptr) {
        cout << "Not found Key: " << key << endl;
        return -1; // for now
    } else {
        return node->value;
    }
}

// Scan the memtable to retrieve all KV-pairs in a key range in key order (key1 < key2)
vector<pair<int, int>> RBTree::scan(const int& key1, const int& key2) {
    vector<pair<int, int>> sorted_KV;

    inorderScan(sorted_KV, this->root, key1, key2);

    return sorted_KV;
}

// Helper function to recursively perform inorder scan
void RBTree::inorderScan(vector<pair<int, int>>& sorted_KV, Node* root, const int& key1, const int& key2) {
    if (root != nullptr) {
        inorderScan(sorted_KV, root->left, key1, key2);
        // Only include KV-pairs that are in the range
        if (key1 <= root->key && root->key <= key2) {
            sorted_KV.push_back({root->key, root->value});
        }
        inorderScan(sorted_KV, root->right, key1, key2);
    }
}

Node* RBTree::search(Node* root, const double& key) {
    if (root == nullptr || root->key == key) {
        return root;
    }

    if (key < root->key) {
        return search(root->left, key);
    } else {
        return search(root->right, key);
    }
}

// TODO: Helper function to perform left rotation
void RBTree::rotateLeft(Node* node) {
    // Implement left rotation here
}

// TODO: Helper function to perform right rotation
void RBTree::rotateRight(Node* node) {
    // Implement right rotation here
}

// TODO: Helper function to fix Red-Black Tree properties violations after insertion
void RBTree::selfBalance(Node* node) {
    // Implement fix for Red-Black Tree properties violations here
}

// Helper function to insert a node into the Red-Black Tree
void RBTree::insertNode(Node* node) {
    // Perform a standard Binary Search Tree insertion
    Node* y = nullptr;
    Node* x = root;

    while (x != nullptr) {
        y = x;
        if (node->key < x->key)
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

    // Initialize additional properties for the new node
    // node->left = nullptr;
    // node->right = nullptr;

    // Fix any Red-Black Tree violations that may have been introduced by the insertion
    selfBalance(node);
    curr_size++;
    cout << "Insert key: " << node->key << " value: " << node->value << endl;
}

// TODO: Helper function to delete a node from the Red-Black Tree
void RBTree::deleteNode(Node* node) {
    // Implement deletion logic here
}
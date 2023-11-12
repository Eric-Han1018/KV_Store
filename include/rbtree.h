#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
using namespace std;
namespace fs = std::filesystem;

enum Color {black, red};
enum Result {allGood, notInMemtable, memtableFull};

class Node {
    public:
        int64_t key;
        int64_t value;
        Color color;
        Node* parent;
        Node* left;
        Node* right;

        Node(int64_t key, int64_t value, Color color=black, Node* parent=nullptr, Node* left=nullptr, Node* right=nullptr):
            key(key), value(value), color(color), parent(parent), left(left), right(right) {}

        ~Node() {
            delete left;
            left = nullptr;
            delete right;
            right = nullptr;
            #ifdef DEBUG
                cout << "Deleted node with key: " << key << endl;
            #endif
        }

};

class RBTree {
    public:
        Node* root;
        size_t memtable_size;     // Maximum capacity
        size_t curr_size;         // Current size
        int64_t min_key;          // Minimum key stored in the tree
        int64_t max_key;          // Maximum key stored in the tree

        RBTree(size_t capacity, Node* root=nullptr);
        ~RBTree();

        Result put(const int64_t& key, const int64_t& value);
        Result get(int64_t*& result, const int64_t& key);
        void scan(vector<pair<int64_t, int64_t>>& sorted_KV, const Node* root, const int64_t& key1, const int64_t& key2);

    private:
        Node* search(Node* root, const int64_t& key);
        void rotateLeft(Node* x);
        void rotateRight(Node* x);
        void insertFixup(Node* node);
        void insertNode(Node* node);
        void deleteNode(Node* node);
};
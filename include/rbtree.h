#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
using namespace std;
namespace fs = std::filesystem;

// Global constant variables
namespace constants {
}

enum Color {black, red};

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
            cout << "Deleted node with key: " << key << endl;
        }

};

class RBTree {
    public:
        Node* root;
        size_t memtable_size;     // Maximum capacity
        size_t curr_size;         // Current size
        int64_t min_key;          // Minimum key stored in the tree
        int64_t max_key;          // Maximum key stored in the tree
        vector<fs::path> sorted_dir; // The sorted list of all SST files (ascending order, need to reverse when iterate)

        RBTree(size_t capacity, Node* root=nullptr);
        ~RBTree();

        void put(const int64_t& key, const int64_t& value);
        const int64_t* get(const int64_t& key);
        const vector<pair<int64_t, int64_t>>* scan(const int64_t& key1, const int64_t& key2);
        string writeToSST();

    private:
        Node* search(Node* root, const int64_t& key);
        const int64_t* search_SSTs(const int64_t& key);
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2);
        void scan_memtable(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root, const int64_t& key1, const int64_t& key2);

        void rotateLeft(Node* x);
        void rotateRight(Node* x);
        void insertFixup(Node* node);
        void insertNode(Node* node);
        void deleteNode(Node* node);

        void clear_tree();
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key);
};
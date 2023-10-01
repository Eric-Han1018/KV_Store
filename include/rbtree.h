#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
using namespace std;
namespace fs = std::filesystem;

// Global constant variables
namespace constants {
    const string DATA_FOLDER = "./data/";
    const int KEY_VALUE_SIZE = sizeof(int64_t);
    const int PAIR_SIZE = sizeof(pair<int64_t, int64_t>);
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
        vector<fs::path>* sorted_dir; // The sorted list of all SST files

        RBTree(size_t capacity, Node* root=nullptr): root(root), memtable_size{capacity} {
            curr_size = 0;
            min_key = numeric_limits<int64_t>::max();
            max_key = numeric_limits<int64_t>::min();

            // Iterate the data/ folder to get a sorted list of files
            set<fs::path> sorted_dir_set;
            for (auto& file_path : fs::directory_iterator(constants::DATA_FOLDER)) {
                sorted_dir_set.insert(file_path);
            }

            // Convert Set to Vector (faster for pushing back in future)
            sorted_dir = new vector<fs::path>(sorted_dir_set.begin(), sorted_dir_set.end());

        }
        ~RBTree() {
            cout << "Deleting tree..." << endl;
            delete sorted_dir;
            delete root;
            root = nullptr;
        }

        void put(const int64_t& key, const int64_t& value);
        int64_t get(const int64_t& key);
        vector<pair<int64_t, int64_t>> scan(const int64_t& key1, const int64_t& key2);
        string writeToSST();

    private:
        Node* search(Node* root, const int64_t& key);
        int64_t search_SSTs(const int64_t& key);
        int64_t search_SST(const fs::path& file_path, const int64_t& key);
        vector<pair<int64_t, int64_t>> scan_SST(const string& file_path, const int64_t& key1, const int64_t& key2);
        void inorderScan(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root, const int64_t& key1, const int64_t& key2);

        void rotateLeft(Node* x);
        void rotateRight(Node* x);
        void insertFixup(Node* node);
        void insertNode(Node* node);
        void deleteNode(Node* node);

        void clear_tree();
        void parse_SST_name(string file_name, int64_t& min_key, int64_t& max_key);
};
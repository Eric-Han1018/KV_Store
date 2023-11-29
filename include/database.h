#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "rbtree.h"
#include "SST.h"
#include "bufferpool.h"
#include "aligned_KV_vector.h"
#include "constants.h"

using namespace std;
namespace fs = std::filesystem;

class Database {
    public:
        string db_name;
        RBTree* memtable;
        SST* sst;
        Bufferpool* bufferpool;
        size_t memtable_capacity;
        Node* memtable_root;

        Database(size_t memtable_capacity, Node* memtable_root=nullptr) :
            memtable_capacity(memtable_capacity), memtable_root(memtable_root) {}

        ~Database() {
            memtable = nullptr;
            sst = nullptr;
            bufferpool = nullptr;
        }

        void openDB(const string db_name);
        void closeDB();
        void put(const int64_t& key, const int64_t& value);
        const int64_t* get(const int64_t& key, const bool use_btree);
        const vector<pair<int64_t, int64_t>>* scan(const int64_t& key1, const int64_t& key2, const bool use_btree);

    private:
        string writeToSST();
        void scan_memtable(aligned_KV_vector& sorted_KV, Node* root);
        void clear_tree();
        int32_t convertToSST(vector<vector<BTreeNode>>& non_leaf_nodes, aligned_KV_vector& sorted_KV);
        void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, int64_t& key, int32_t current_level, const int32_t& max_size);
};
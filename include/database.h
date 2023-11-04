#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "rbtree.h"
#include "SST.h"
using namespace std;
namespace fs = std::filesystem;

class Database {
    public:
        RBTree* memtable;
        SST* sst;


        Database(size_t memtable_capacity, Node* memtable_root=nullptr) {
            memtable = new RBTree(memtable_capacity, memtable_root);
            sst = new SST();
        }

        ~Database() {
            delete memtable;
            delete sst;
            memtable = nullptr;
            sst = nullptr;
        }

        void put(const int64_t& key, const int64_t& value);
        const int64_t* get(const int64_t& key, const bool use_btree);
        const vector<pair<int64_t, int64_t>>* scan(const int64_t& key1, const int64_t& key2, const bool use_btree);

    private:
        string writeToSST();
        void scan_memtable(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root);
        void clear_tree();
        int32_t convertToSST(vector<vector<BTreeNode>>& non_leaf_nodes, vector<pair<int64_t, int64_t>>& sorted_KV);
        void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, int64_t& key, int32_t current_level);
};
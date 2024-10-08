#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "rbtree.h"
#include "LSMTree.h"
#include "bufferpool.h"
#include "aligned_KV_vector.h"
#include "bloomFilter.h"
#include "constants.h"

using namespace std;
namespace fs = std::filesystem;

class Database {
    public:
        string db_name;
        RBTree* memtable;
        LSMTree* lsmtree;
        Bufferpool* bufferpool;
        size_t memtable_capacity;
        Node* memtable_root;

        Database(size_t memtable_capacity, Node* memtable_root=nullptr) :
            memtable_capacity(memtable_capacity), memtable_root(memtable_root) {}

        ~Database() {
            memtable = nullptr;
            lsmtree = nullptr;
            bufferpool = nullptr;
        }

        void openDB(const string db_name);
        void closeDB();
        void put(const int64_t& key, const int64_t& value);
        const int64_t* get(const int64_t& key, const bool use_btree);
        const vector<pair<int64_t, int64_t>>* scan(const int64_t& key1, const int64_t& key2, const bool use_btree);
        void del(const int64_t& key);

    private:
        void removeTombstones(std::vector<std::pair<int64_t, int64_t>>*& sorted_KV, int64_t tombstone);
        string writeToSST();
        void scan_memtable(aligned_KV_vector& sorted_KV, Node* root);
        void clear_tree();
};
#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "bufferpool.h"
#include "rbtree.h"
#include "SST.h"
using namespace std;
namespace fs = std::filesystem;

// TODO: set TOMBSTONE

size_t SIZE_RATIO = 2;

class Level {
    public:
        size_t cur_size;
        // TODO: 在这个level对应的SSTs、目前存放index？initialize
        vector<int> sst_index;

        Level() {
            cur_size = 0;
            // ssts.resize(SIZE_RATIO);
        };

        ~Level() {};

        void Clear();
};

class LSMTree {
    public:
        string db_name;
        RBTree* memtable;
        size_t memtable_capacity;
        Node* memtable_root;
        Bufferpool* bufferpool;
        vector<Level*> levels;
        size_t cur_size;
        SST* sst;

        LSMTree(string db_name, size_t memtable_capacity, Node* memtable_root=nullptr) : db_name(db_name), cur_size(0) {
            memtable = new RBTree(memtable_capacity, memtable_root);
            bufferpool = new Bufferpool(constants::BUFFER_POOL_CAPACITY);
            sst = new SST(db_name, bufferpool);
        };

        ~LSMTree() {};

    private:
        void merge_SST();
};
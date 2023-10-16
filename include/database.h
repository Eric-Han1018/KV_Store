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

namespace databaseConstants {
    const string DATA_FOLDER = "./data/";
    const int KEY_VALUE_SIZE = sizeof(int64_t);
    const int PAIR_SIZE = sizeof(pair<int64_t, int64_t>);
}

class Database {
    public:
        RBTree* memtable;
        BPlusTree* SST;

        Database(size_t memtable_capacity, Node* memtable_root=nullptr) {
            memtable = new RBTree(memtable_capacity, memtable_root);
            SST = new BPlusTree();
        }

        ~Database() {
            delete memtable;
            delete SST;
            memtable = nullptr;
            SST = nullptr;
        }
};
#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "SST.h"

using namespace std;
namespace fs = std::filesystem;

class Frame {
    public:
        string p_id;
        BTreeNode btree_node;
        
        Frame(string page_id, BTreeNode btree_node):
            p_id(page_id), btree_node(btree_node) {}

};


class Bufferpool {
    public:
        size_t current_size;
        size_t maximal_size;

        vector<list<Frame>> hash_directory;

        Bufferpool(size_t initial_size, size_t maximal_size):
            current_size(initial_size), maximal_size(maximal_size), hash_directory(initial_size) {}

        ~Bufferpool() {}

    private:
        void change_maximal_size(size_t new_maximal_size);
        size_t murmur_hash(const string& key);
};
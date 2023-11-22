#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include <cmath>
#include "SST.h"

using namespace std;
namespace fs = std::filesystem;

// B-Tree non-leaf Node (members stored contiguously)
typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeNode {
    int64_t keys[constants::KEYS_PER_NODE] = {0}; // Keys in each node
    int32_t ptrs[constants::KEYS_PER_NODE + 1] = {0}; // File offsets to children
    int32_t size = 0;
} BTreeNode;

typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeLeafNode {
    pair<int64_t, int64_t> data[constants::KEYS_PER_NODE];
} BTreeLeafNode;

class Frame {
    public:
        string p_id;
        bool leaf_page;
        char* data;
        bool clock_bit;
        
        Frame(string page_id, bool leaf_page, char* data):
            p_id(page_id), leaf_page(leaf_page), data(data), clock_bit(true) {}

};


class Bufferpool {
    public:
        size_t current_size;
        size_t maximal_size;
        vector<list<Frame>> hash_directory;
        int clock_hand;

        Bufferpool(size_t max_size){
            current_size = 0;
            maximal_size = max_size;
            hash_directory.resize(floor(maximal_size / 5));
            clock_hand = 0;
        }

        ~Bufferpool() {
            for (auto& bucket : hash_directory) {
                for (auto& frame : bucket) {
                    delete (BTreeNode*)(frame.data);
                }
            }
        }

        void change_maximal_size(size_t new_maximal_size);
        void insert_to_buffer(const string& p_id, bool leaf_page, char* data);
        bool get_from_buffer(const string& p_id, char*& data);
        void print();

    private:
        void evict_clock(int num_pages);
};
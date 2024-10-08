#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "BTree.h"
#include <cmath>

using namespace std;
namespace fs = std::filesystem;

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
                    delete (BTreeNonLeafNode*)(frame.data);
                }
            }
        }

        void insert_to_buffer(const string& p_id, bool leaf_page, char* data);
        bool get_from_buffer(const string& p_id, char*& data);
        void print();

    private:
        void evict_clock(int num_pages);
};
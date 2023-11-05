#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "rbtree.h"

using namespace std;
namespace fs = std::filesystem;

// Define the page size in bytes (4KB)
const size_t PAGE_SIZE = 4 * 1024;

class Frame {
    public:
        string p_id;
        // std::array<int64_t, PAGE_SIZE> page;
        string page;
        
        Frame(string p_id){
            p_id = p_id;
            // page.fill(0);
        }
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
        Result get(int64_t*& result, const string& p_id);
        void insert(const string& p_id, string data);
        size_t murmur_hash(const string& key);
};
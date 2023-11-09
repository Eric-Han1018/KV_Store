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

class Bucket {
    public:
        string p_id;
        bool leaf_page;
        char* data;
        bool clock_bit;
        
        Bucket(string page_id, bool leaf_page, char* data):
            p_id(page_id), leaf_page(leaf_page), data(move(data)), clock_bit(true) {}
};


class Bufferpool {
    public:
        size_t current_size;
        size_t maximal_size;
        vector<list<Bucket>> hash_directory;
        int clock_hand;

        Bufferpool(size_t max_size){
            current_size = 0;
            maximal_size = max_size;
            hash_directory.resize(10);
            clock_hand = 0;
        }

        ~Bufferpool() {}

        void change_maximal_size(size_t new_maximal_size);
        void insert_to_buffer(const string& p_id, bool leaf_page, char* data);
        bool get_from_buffer(const string& p_id, char*& data);
        void print();

    private:
        size_t murmur_hash(const string& key);
        void evict_clock(int num_pages);
};
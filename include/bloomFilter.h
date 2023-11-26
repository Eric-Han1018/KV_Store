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
#include <bitset>
using namespace std;
namespace fs = std::filesystem;

class BloomFilter {
    public:
        bitset<constants::CACHE_LINE_SIZE>* bitmap;
        size_t total_num_bits;
        size_t total_num_cache_lines;

        // size: num of kv entries
        BloomFilter(size_t size) {
            bitmap = new(align_val_t(constants::PAGE_SIZE)) bitset<constants::CACHE_LINE_SIZE>[(size * constants::BLOOM_FILTER_NUM_BITS) >> constants::CACHE_LINE_SIZE_SHIFT];
            total_num_bits = size * constants::BLOOM_FILTER_NUM_BITS;
            total_num_cache_lines = total_num_bits >> constants::CACHE_LINE_SIZE_SHIFT;
        } 

        ~BloomFilter() {
            ::operator delete[] (bitmap, align_val_t(constants::PAGE_SIZE));
            total_num_cache_lines = 0;
            total_num_bits = 0;
        }

        void set(const int64_t& key);
        void writeToBloomFilter(const string& filter_path);      
};
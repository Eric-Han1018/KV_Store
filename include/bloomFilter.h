#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "LSMTree.h"
#include <cmath>
#include <bitset>
using namespace std;
namespace fs = std::filesystem;

class BloomFilter {
    public:
        bitset<constants::CACHE_LINE_SIZE>* bitmap;
        size_t total_num_bits;
        size_t total_num_cache_lines;
        size_t bits_per_entry;

        // size: num of kv entries
        BloomFilter(const size_t& size, const size_t& current_level, const size_t& total_level) {
            bits_per_entry = calculate_num_bits_per_entry(current_level, total_level);
            total_num_bits = size * bits_per_entry;
            total_num_cache_lines = total_num_bits >> constants::CACHE_LINE_SIZE_SHIFT;

            // Allocate a memory aligned cacheline arrays
            bitmap = new(align_val_t(constants::PAGE_SIZE))
            bitset<constants::CACHE_LINE_SIZE>[(size * bits_per_entry) >> constants::CACHE_LINE_SIZE_SHIFT];
        } 

        ~BloomFilter() {
            ::operator delete[] (bitmap, align_val_t(constants::PAGE_SIZE));
            total_num_cache_lines = 0;
            total_num_bits = 0;
            bits_per_entry = 0;
        }

        void set(const int64_t& key);
        void writeToBloomFilter(const string& filter_path);
        // Calculate the adjusted number of bits based on the closed form forluma on Monkey paper
        static inline size_t calculate_num_bits_per_entry(const size_t& current_level, const size_t& total_level) {
            return 3 * (total_level - current_level) - 2;
        }
};
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
        bitset<constants::CACHE_LINE_SIZE_BITS>* bitmap;
        float bits_per_entry; // M
        size_t num_of_hashes; // M*ln2
        size_t total_num_bits; // bits_per_entry * num_of_entries
        size_t total_num_cache_lines; // total_num_bits / cacheline_size
        size_t padded_num_cache_line; // total num cachelines after padding

        // size: num of kv entries
        BloomFilter(const size_t& size, const size_t& current_level, const size_t& total_level) {
            bits_per_entry = calculate_num_bits_per_entry(current_level, total_level);
            total_num_bits = (size_t)(size * bits_per_entry);
            total_num_cache_lines = (total_num_bits >> constants::CACHE_LINE_SIZE_BITS_SHIFT);
            num_of_hashes = calculate_num_of_hashes(bits_per_entry);

            // If the array is not a multiple of pages, we need to pad it to become multiple of pages
            padded_num_cache_line = (total_num_bits >> constants::CACHE_LINE_SIZE_BITS_SHIFT) + constants::NUM_CACHELINE_PER_PAGE - 1;
            padded_num_cache_line -= (padded_num_cache_line % constants::NUM_CACHELINE_PER_PAGE);

            // Allocate a memory aligned cacheline arrays
            bitmap = new(align_val_t(constants::PAGE_SIZE))
            bitset<constants::CACHE_LINE_SIZE_BITS>[padded_num_cache_line];
        } 

        ~BloomFilter() {
            ::operator delete[] (bitmap, align_val_t(constants::PAGE_SIZE));
            total_num_cache_lines = 0;
            total_num_bits = 0;
            bits_per_entry = 0;
        }

        void set(const int64_t& key);
        void writeToStorage(const string& filter_path);
        // Calculate the adjusted number of bits based on the formula on Monkey paper
        static inline float calculate_num_bits_per_entry(const size_t& current_level, const size_t& total_level) {
            if (current_level == total_level - 1) { // TODO: If you change the LSM-Tree RATIO, remember to change the following numbers
                return 5.3913; // (log4 - log0.1 - log3) / ln2
            } else {
                return 2.885 * (total_level - current_level) + 4.7925; // ((L+1-i)log4 - log0.1) / ln2
            }
        }
        // # hashes = M*ln2
        static inline size_t calculate_num_of_hashes(const float& bits_per_entry) {
            // We use ceil because it is possible for bits_per_entry to be < 1
            return (size_t)ceil(0.7 * bits_per_entry); // ln2 ~= 0.7
        }
};
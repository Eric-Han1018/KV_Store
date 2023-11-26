#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include "bloomFilter.h"
#include "util.h"
using namespace std;

// Set the corresponding bits after inserted an entry
void BloomFilter::set(const int64_t& key) {
    for (uint32_t i = 0; i < constants::BLOOM_FILTER_NUM_HASHES; ++i) {
        // Using 0,1,2,3... as the seed to represent different hashes
        size_t hash = murmur_hash(key, i) % total_num_bits;
        bitmap[hash >> constants::CACHE_LINE_SIZE_SHIFT].set(hash & ((1<<constants::CACHE_LINE_SIZE_SHIFT) - 1));
    }
}

// Test if the filter returns positive for the entry
inline bool BloomFilter::test(const int64_t& key, const uint32_t& seed) {
    size_t hash = murmur_hash(key, seed) % total_num_bits;

    if (!bitmap[hash >> constants::CACHE_LINE_SIZE_SHIFT].test(hash & ((1<<constants::CACHE_LINE_SIZE_SHIFT) - 1))) return false;
    return true;
}

// Write the filter to storage
void BloomFilter::writeToBloomFilter(const string& filter_path) {
    int fd = open(filter_path.c_str(), O_WRONLY | O_CREAT | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd!=-1);
    #endif
    #ifdef ASSERT
        // Check if the bloom filter is a multiple of pages
        assert((total_num_bits >> constants::BYTE_BIT_SHIFT) % (1<<constants::PAGE_SIZE_SHIFT) == 0);
    #endif
    int nbytes = pwrite(fd, (char*)bitmap, (total_num_bits >> constants::BYTE_BIT_SHIFT), 0);
    #ifdef ASSERT
        assert(nbytes == (int)(total_num_bits >> constants::BYTE_BIT_SHIFT));
    #endif
    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif
}
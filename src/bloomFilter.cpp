#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "bloomFilter.h"
#include "util.h"
using namespace std;

// Set the corresponding bits after inserted an entry
void BloomFilter::set(const int64_t& key) {
    for (uint32_t i = 0; i < constants::BLOOM_FILTER_NUM_HASHES; ++i) {
        // Using 0,1,2,3... as the seed to represent different hashes
        size_t hash = murmur_hash(key, i) % (constants::MEMTABLE_SIZE * constants::BLOOM_FILTER_NUM_BITS);
        bitmap.set(hash);
    }
}

// Test if the filter returns positive for the entry
inline bool BloomFilter::test(const int64_t& key, const uint32_t& seed) {
    size_t hash = murmur_hash(key, seed) % (constants::MEMTABLE_SIZE * constants::BLOOM_FILTER_NUM_BITS);
    if (!bitmap.test(hash)) return false;
    return true;
}
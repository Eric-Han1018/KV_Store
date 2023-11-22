#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "bloomFilter.h"
#include "util.h"
using namespace std;

// Set the correspondings bits after inserted an entry
template<size_t N>
void BloomFilter<N>::set(const int64_t& key) {
    for (uint32_t i = 0; i < constants::BLOOM_FILTER_NUM_HASHES; ++i) {
        // Using 0,1,2,3... as the seed to represent different hashes
        size_t hash = murmur_hash(key, i) % (N * constants::BLOOM_FILTER_NUM_BITS);
        bitmap.set(hash);
    }
}

// Test if the filter returns positive for the entry
template<size_t N>
bool BloomFilter<N>::test(const int64_t& key) {
    for (uint32_t i = 0; i < constants::BLOOM_FILTER_NUM_HASHES; ++i) {
        size_t hash = murmur_hash(key, i) % (N * constants::BLOOM_FILTER_NUM_BITS);
        if (!bitmap.test(hash)) return false;
    }
    return true;
}
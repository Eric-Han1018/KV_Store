#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include "bloomFilter.h"
#include "util.h"
using namespace std;

// Set the corresponding bits after inserted an entry
void BloomFilter::set(const int64_t& key) {
    // We use seed=0 to get the index of cache line
    size_t cache_line_hash = murmur_hash(key, 0) % total_num_cache_lines;
    for (uint32_t i = 1; i <= num_of_hashes; ++i) {
        // Using 1,2,3... as the seed to represent different hashes
        // On the cacheline, calculate the corresponding bits
        size_t hash = murmur_hash(key, i) & ((1<<constants::CACHE_LINE_SIZE_BITS_SHIFT) - 1); // equivalent to "% constants::CACHE_LINE_SIZE"
        bitmap[cache_line_hash].set(hash);
    }
}

// Write the filter to storage
void BloomFilter::writeToStorage(const string& filter_path) {
    int fd = open(filter_path.c_str(), O_WRONLY | O_CREAT | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd!=-1);
    #endif
    #ifdef ASSERT
        // Check if the bloom filter is a multiple of pages
        assert((padded_num_cache_line << constants::CACHE_LINE_SIZE_BYTES_SHIFT) % constants::PAGE_SIZE == 0);
    #endif
    int nbytes = pwrite(fd, (char*)bitmap, padded_num_cache_line << constants::CACHE_LINE_SIZE_BYTES_SHIFT, 0);
    #ifdef ASSERT
        assert(nbytes == (int)(padded_num_cache_line << constants::CACHE_LINE_SIZE_BYTES_SHIFT));
    #endif
    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif
}
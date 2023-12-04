#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include <cmath>
using namespace std;

// Global constant variables
namespace constants {
    const string DATA_FOLDER = "./data/";
    const int KEY_VALUE_SIZE = sizeof(int64_t);
    const int PAIR_SIZE = sizeof(pair<int64_t, int64_t>);
    const int KEYS_PER_NODE = (1 << 12) / PAIR_SIZE; //4kb node
    const int MEMTABLE_SIZE = (1 << 20) / PAIR_SIZE; //1mb memtable
    const size_t PAGE_SIZE = KEYS_PER_NODE * PAIR_SIZE; // 4kb page

    // Bufferpool constants
    const int BUFFER_POOL_CAPACITY = 10 * MEMTABLE_SIZE / KEYS_PER_NODE; // 10MB
    const bool USE_BUFFER_POOL = true;

    // LSM-Tree constants
    const size_t LSMT_SIZE_RATIO = 4;
    const size_t LSMT_DEPTH = 10;

    // Sequential Flooding Prevention constants
    const size_t SEQUENTIAL_FLOODING_LIMIT = 1000;

    // Deletion constants
    const int64_t TOMBSTONE = std::numeric_limits<int64_t>::min();

    // Bloom Filter constants
    const uint32_t CACHE_LINE_SIZE_BYTES = 64; // bytes
    const uint32_t CACHE_LINE_SIZE_BITS = 64 * 8; // bits
    const uint32_t CACHE_LINE_SIZE_BYTES_SHIFT = 6; // 64=2^6, #Cachelines <=> #bytes
    const uint32_t CACHE_LINE_SIZE_BITS_SHIFT = 9; // 64*8=2^9, #Cachelines <=> #bits
    const uint32_t NUM_CACHELINE_PER_PAGE = 64; // 4kB/64B=64
    const uint32_t PAGE_SIZE_SHIFT = 12; // 4kB=2^12B, #pages <=> #bytes
    const uint32_t PAGE_CACHELINE_SHIFT = 6; // 1 CL=64 Bytes=2^6 Bytes, 1 page=2^6 CL, #pages<=>#cachelines
    const uint32_t BYTE_BIT_SHIFT = 3; // 1Byte=8bits=2^3

    // Monkey
    // If you change the LSM-Tree RATIO, remember to change the following numbers
    const float LAST_LEVEL_M = 5.3913; // (logT - logR - log(T-1)) / ln2, T=4, R=0.1
    const float LOGT_LN2 = 2.885; // logT/ln2, T=4
    const float LOGR_LN2 = 4.7925; // -logR/ln2, R=0.1
}
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
    const int KEY_VALUE_SIZE = sizeof(int64_t);
    const int PAIR_SIZE = sizeof(pair<int64_t, int64_t>);
    const string DATA_FOLDER = "./data/";
    const int KEYS_PER_NODE = (1 << 12) / PAIR_SIZE; //4kb node
    const size_t PAGE_SIZE = KEYS_PER_NODE * PAIR_SIZE; // 4kb page
    const int MEMTABLE_SIZE = (1 << 20) / PAIR_SIZE; //1mb memtable
    const int BUFFER_POOL_CAPACITY = floor((MEMTABLE_SIZE * 0.1) / KEYS_PER_NODE); //10% of data, 100kb
    const bool USE_BUFFER_POOL = true;
    const size_t LSMT_SIZE_RATIO = 2;
    const size_t LSMT_DEPTH = 10;
    const int SCAN_RANGE_LIMIT = 3;
    const int64_t TOMBSTONE = std::numeric_limits<int64_t>::min();
    const uint32_t CACHE_LINE_SIZE = 256 * 8; // bits
    const uint32_t BLOOM_FILTER_NUM_BITS = 5;
    const uint32_t BLOOM_FILTER_NUM_HASHES = 0.7 * BLOOM_FILTER_NUM_BITS; // ln2 ~= 0.7
    const uint32_t PAGE_SIZE_SHIFT = 12; // >>12
    const uint32_t CACHE_LINE_SIZE_SHIFT = 11; // >>11
    const uint32_t BYTE_BIT_SHIFT = 3;
    const uint32_t PAGE_CACHELINE_SHIFT = 4; // 1 page = 4kB = 2^12 Bytes, 1 CL = 256 Bytes = 2^8 Bytes, 1 page = 2^4 CL 
}
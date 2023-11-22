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
    const int PAGE_SIZE = KEYS_PER_NODE * PAIR_SIZE; // 4kb page
    const int MEMTABLE_SIZE = (1 << 20) / PAIR_SIZE; //1mb memtable
    const int BUFFER_POOL_CAPACITY = floor((MEMTABLE_SIZE * 0.1) / KEYS_PER_NODE); //10% of data, 100kb
    const bool USE_BUFFER_POOL = true;
    const size_t LSMT_SIZE_RATIO = 2;
    const size_t LSMT_DEPTH = 10;
    const int BLOOM_FILTER_NUM_BITS = 5;
}
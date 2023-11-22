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
        bitset<constants::MEMTABLE_SIZE * constants::BLOOM_FILTER_NUM_BITS> bitmap;

        void set(const int64_t& key);
        inline bool test(const int64_t& key, const uint32_t& seed);        
};
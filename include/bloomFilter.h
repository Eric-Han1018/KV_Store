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

template<size_t N>
class BloomFilter {
    public:
        bitset<N * constants::BLOOM_FILTER_NUM_BITS> bitmap;

        void set(const int64_t& key);
        bool test(const int64_t& key);        
};

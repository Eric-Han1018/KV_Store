#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "bufferpool.h"
using namespace std;
namespace fs = std::filesystem;

/*
 * These are functions that are used across multiple classes
 */
size_t murmur_hash(const string& key);
size_t murmur_hash(const int64_t& key, const uint32_t& seed);
void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level);
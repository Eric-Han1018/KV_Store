#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
#include "bufferpool.h"
#include "aligned_KV_vector.h"
using namespace std;
namespace fs = std::filesystem;

/*
 * These are functions that are used across multiple classes
 */
size_t murmur_hash(const string& key);
size_t murmur_hash(const int64_t& key, const uint32_t& seed);
void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level);
void insertFixUp(vector<vector<BTreeNode>>& non_leaf_nodes, const int32_t& leaf_end);
void print_B_Tree(vector<vector<BTreeNode>>& non_leaf_nodes, aligned_KV_vector& sorted_KV);
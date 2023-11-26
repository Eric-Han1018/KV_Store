#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "MurmurHash3.h"
using namespace std;
namespace fs = std::filesystem;

size_t murmur_hash(const string& key);
size_t murmur_hash(const int64_t& key, const uint32_t& seed);
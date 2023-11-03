#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
using namespace std;

// Global constant variables
namespace constants {
    const int KEYS_PER_NODE = 3;
    const int KEY_VALUE_SIZE = sizeof(int64_t);
    const int PAIR_SIZE = sizeof(pair<int64_t, int64_t>);
    const string DATA_FOLDER = "./data/";
}
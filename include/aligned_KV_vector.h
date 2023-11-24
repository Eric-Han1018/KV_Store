#pragma once
#include <iostream>
#include "constants.h"
using namespace std;

typedef struct aligned_KV_vector {
    alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) pair<int64_t, int64_t> data[constants::MEMTABLE_SIZE]; //FIXME: PAGE_SIZE
    int32_t len = 0;

    int32_t size() {
        return len;
    }

    void emplace_back(pair<int64_t, int64_t> entry) {
        data[len] = entry;
        ++len;
    }

    void emplace_back(int64_t key, int64_t value) {
        data[len].first = key;
        data[len].second = value;
        ++len;
    }

    void push_back(pair<int64_t, int64_t> entry) {
        data[len] = entry;
        ++len;
    }

    void push_back(int64_t key, int64_t value) {
        data[len].first = key;
        data[len].second = value;
        ++len;
    }

    pair<int64_t, int64_t> back() {
        return data[len - 1];
    }
} aligned_KV_vector;
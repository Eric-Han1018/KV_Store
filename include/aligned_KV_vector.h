#pragma once
#include <iostream>
#include <cassert>
#include "constants.h"
using namespace std;

typedef struct aligned_KV_vector {
    alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) pair<int64_t, int64_t> data[2 * constants::MEMTABLE_SIZE]; //FIXME: PAGE_SIZE
    int32_t len = 0;

    int32_t size() {
        return len;
    }

    void emplace_back(pair<int64_t, int64_t> entry) {
        data[len] = entry;
        ++len;
        #ifdef ASSERT
            assert(len < 2 * constants::MEMTABLE_SIZE); //FIXME: PAGE_SIZE
        #endif
    }

    void emplace_back(int64_t key, int64_t value) {
        data[len].first = key;
        data[len].second = value;
        ++len;
        #ifdef ASSERT
            assert(len < 2 * constants::MEMTABLE_SIZE); //FIXME: PAGE_SIZE
        #endif
    }

    void push_back(pair<int64_t, int64_t> entry) {
        data[len] = entry;
        ++len;
        #ifdef ASSERT
            assert(len < 2 * constants::MEMTABLE_SIZE); //FIXME: PAGE_SIZE
        #endif
    }

    void push_back(int64_t key, int64_t value) {
        data[len].first = key;
        data[len].second = value;
        ++len;
        #ifdef ASSERT
            assert(len < constants::MEMTABLE_SIZE); //FIXME: PAGE_SIZE
        #endif
    }

    pair<int64_t, int64_t> back() {
        return data[len - 1];
    }
} aligned_KV_vector;
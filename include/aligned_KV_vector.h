#pragma once
#include <iostream>
#include <cassert>
#include "constants.h"
using namespace std;

class aligned_KV_vector {
    public:
        pair<int64_t, int64_t>* data;
        size_t len;
        size_t max_len;

        aligned_KV_vector(size_t size) {
            data = new(align_val_t(constants::KEYS_PER_NODE * constants::PAIR_SIZE)) pair<int64_t, int64_t>[size];
            max_len = size;
            len = 0;
        }

        ~aligned_KV_vector() {
            ::operator delete[] (data, align_val_t(constants::KEYS_PER_NODE * constants::PAIR_SIZE));
            max_len = 0;
            data = nullptr;
        }

        int32_t size() {
            return len;
        }

        void emplace_back(pair<int64_t, int64_t> entry) {
            #ifdef ASSERT
                assert(len < max_len);
            #endif
            data[len] = entry;
            ++len;
        }

        void emplace_back(int64_t key, int64_t value) {
            #ifdef ASSERT
                assert(len < max_len);
            #endif
            data[len].first = key;
            data[len].second = value;
            ++len;
        }

        void push_back(pair<int64_t, int64_t> entry) {
            #ifdef ASSERT
                assert(len < max_len);
            #endif
            data[len] = entry;
            ++len;
        }

        void push_back(int64_t key, int64_t value) {
            #ifdef ASSERT
                assert(len < max_len);
            #endif
            data[len].first = key;
            data[len].second = value;
            ++len;
        }

        pair<int64_t, int64_t> back() {
            #ifdef ASSERT
                assert(len <= max_len);
            #endif
            return data[len - 1];
        }
};
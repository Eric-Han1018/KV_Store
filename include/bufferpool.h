#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
using namespace std;
namespace fs = std::filesystem;

class Bufferpool {
    public:
        size_t current_size;
        size_t maximal_size;

        Bufferpool(size_t initial_size, size_t maximal_size) {
            current_size = initial_size;
            maximal_size = maximal_size;
        }

        ~Bufferpool() {}

    private:
        void change_maximal_size(size_t new_maximal_size);
        
};
#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "SST.h"
using namespace std;
namespace fs = std::filesystem;

size_t SIZE_RATIO = 2;

class LSMTree {
    public:
    size_t cur_size;
    SST* sst;

        LSMTree(SST* new_sst){
            cur_size = 0;
            sst = new_sst;
        }

        ~LSMTree() {
            sst = nullptr;
        }



    private:
    void merge_SST();
};
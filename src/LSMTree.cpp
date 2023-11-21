#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "LSMTree.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
#include <cerrno>
#include <cstring>
using namespace std;

void Level::Clear() {
    // Clear level
}

void LSMTree::merge_SST(){
    // Allocating three buffers
    // In each iteration, the minimum key in the input buffers should be appended to the output buffer
    // Whenever the output buffer is full, we append its contents to the new file being created and clear it.
    // auto first_sst = sst->sorted_dir.rbegin();
    // auto second_sst = first_sst++;
    
}
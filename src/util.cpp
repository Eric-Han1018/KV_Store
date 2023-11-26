#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "util.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

size_t murmur_hash(const string& key){
    uint32_t seed = 443;
    const int length = key.length();
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x86_32(key.c_str(), length, seed, &hash);
    return static_cast<size_t>(hash);
}

size_t murmur_hash(const int64_t& key, const uint32_t& seed) {
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x86_32(&key, sizeof(int64_t), seed, &hash);
    return static_cast<size_t>(hash);
}
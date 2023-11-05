#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "SST.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
#include "bufferpool.h"

using namespace std;

size_t Bufferpool::murmur_hash(const string& key){
    uint32_t seed = 443;
    const int length = key.length();
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x64_128(key.c_str(), length, seed, &hash);
    return static_cast<size_t>(hash);
}

void Bufferpool::change_maximal_size(size_t new_size){
    if (new_size < current_size) {
        // TODO: evict
        printf(" ");
    }
    maximal_size = new_size;
}

Result Bufferpool::get(int64_t*& result, const string& p_id){
    size_t bucket_index = murmur_hash(p_id) % hash_directory.size();
    for (auto frame = hash_directory[bucket_index].begin(); frame != hash_directory[bucket_index].end(); ++frame) {
        if (frame->p_id == p_id) {
            result = frame->page;
            return result;
        }
    }
    return NotInBufferPool;
}

void insert(const string& p_id, string data) {
    size_t bucket_index = murmur_hash(p_id) % hash_directory.size();

}
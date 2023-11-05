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

Result Bufferpool::get(int64_t*& result, const int64_t& key){
    string target_id = id_directory[key];
    const string& p_id = target_id;
    size_t bucket_index = murmur_hash(p_id) % hash_directory.size();
    
    auto frame = search(p_id);
    if (frame == hash_directory[bucket_index].end()) {
        if (current_size == maximal_size) {
            // TODO: evict
        }
        return notInMemtable;
    } else {
        result = new int64_t(frame->page);
    }
    return allGood;
}

void Bufferpool::insert(const string& p_id, const int64_t& key) {
    id_directory[key] = p_id;
    size_t bucket_index = murmur_hash(p_id) % hash_directory.size();
    hash_directory[bucket_index].push_back(Frame(p_id));


}

list<Frame>::iterator Bufferpool::search(const string& p_id) {
    size_t bucket_index = murmur_hash(p_id) % hash_directory.size();
    for (auto frame = hash_directory[bucket_index].begin(); frame != hash_directory[bucket_index].end(); ++frame) {
        if (frame->p_id == p_id) {
            return frame;
        }
    }
    return hash_directory[bucket_index].end();
}
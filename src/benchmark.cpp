#include <iostream>
#include "database.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include <time.h>
#include <chrono>
using namespace std;

// Taken from https://www.gormanalysis.com/blog/reading-and-writing-csv-files-with-cpp/
void write_csv(std::string filename, std::vector<std::pair<std::string, std::vector<int>>> dataset){
    // Make a CSV file with one or more columns of integer values
    // Each column of data is represented by the pair <column name, column data>
    //   as std::pair<std::string, std::vector<int>>
    // The dataset is represented as a vector of these columns
    // Note that all columns should be the same size
    
    // Create an output filestream object
    std::ofstream myFile(filename);
    
    // Send column names to the stream
    for(int j = 0; j < (int)dataset.size(); ++j)
    {
        myFile << dataset.at(j).first;
        if(j != (int)dataset.size() - 1) myFile << ","; // No comma at end of line
    }
    myFile << "\n";
    
    // Send data to the stream
    for(int i = 0; i < (int)dataset.at(0).second.size(); ++i)
    {
        for(int j = 0; j < (int)dataset.size(); ++j)
        {
            myFile << dataset.at(j).second.at(i);
            if(j != (int)dataset.size() - 1) myFile << ","; // No comma at end of line
        }
        myFile << "\n";
    }
    
    // Close the file
    myFile.close();
}

int main(int argc, char **argv) {
    srand (1);
    size_t mematable_size = 1; // MB
    int64_t megabyte = 1 << 20;
    vector<int32_t> inputDataSize = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};

    // Convert memtable size to # of KV pairs
    mematable_size *= megabyte;
    mematable_size /= constants::PAIR_SIZE;

    cerr << "Running benchmarking..." << endl;
    vector<int32_t> put_ops;
    vector<int32_t> get_ops;
    vector<int32_t> scan_ops;
    for (int64_t inputSize : inputDataSize) {
        cerr << "-------------- Testing Input Size: " << inputSize << "MB..." << endl;
        inputSize *= megabyte;
        inputSize /= constants::PAIR_SIZE;

        cerr << "Creating Database with size: " << mematable_size << " memtable entries...";
        Database db(mematable_size);
        cerr << "Done" << endl;

        cerr << "Populating " << inputSize << " entries into DB...";
        for (int64_t i = 0; i < inputSize; ++i) {
            // FIXME: Confirm if use rand(), and if use % inputSize
            db.put((int64_t)rand() % inputSize, (int64_t)rand() % inputSize);
        }
        cerr << "Done" << endl;

        cerr << "Testing get()...";
        int64_t count = 0;
        auto start_time = chrono::high_resolution_clock::now();
        // FIXME: Confirm if use this way to determin 10sec
        while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
            // FIXME: Confirm if get by random
            delete db.get((int64_t)rand() % inputSize);
            ++count;
        }
        get_ops.emplace_back(count);
        cerr << "Done" << endl;

        cerr << "Testing scan()...";
        count = 0;
        start_time = chrono::high_resolution_clock::now();
        while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
            int64_t key1 = rand() % inputSize;
            delete db.scan(key1, key1 + constants::KEYS_PER_NODE);
            ++count;
        }
        scan_ops.emplace_back(count);
        cerr << "Done" << endl;

        cerr << "Testing put()...";
        count = 0;
        start_time = chrono::high_resolution_clock::now();
        while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
            // FIXME: Confirm if use rand(), and if use % inputSize
            db.put((int64_t)rand() % inputSize, (int64_t)rand() % inputSize);
            ++count;
        }
        put_ops.emplace_back(count);
        cerr << "Done" << endl;

        count = 0;
        for (auto& path: fs::directory_iterator(constants::DATA_FOLDER)) {
            fs::remove_all(path);
            ++count;
        }
        cerr << "Deleted " << count << "SSTs" <<endl;
    }

    // Wrap into a vector
    std::vector<std::pair<std::string, std::vector<int>>> vals = {{"InputDataSize", inputDataSize}, {"Put", put_ops}, {"Get", get_ops}, {"Scan", scan_ops}};
    
    // Write the vector to CSV
    cerr << "Writing results to benchmark.csv...";
    write_csv("benchmark.csv", vals);
    cerr << "Done" <<endl;
    
    return 0;
}
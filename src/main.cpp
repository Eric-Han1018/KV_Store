#include "database.h"
#include <sstream>
#include <string>

int main() {
    Database db(10);
    std::string line;

    std::cout << "Database CLI. \nMemtable has size capacity of 10. \nCommands: <open `db_name`> <close> <put `key` `value`> <get `key`> <scan `key1` `key2`> <delete `key`> \nType 'quit' to exit.\n";

    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line) || line == "quit") break;

        std::istringstream iss(line);
        std::string command;
        iss >> command;

        if (command == "open") {
            std::string dbName;
            iss >> dbName;
            db.openDB(dbName);
            std::cout << "Database opened: " << dbName << std::endl;
        } else if (command == "close") {
            db.closeDB();
            std::cout << "Database closed." << std::endl;
        } else if (command == "put") {
            int64_t key, value;
            iss >> key >> value;
            db.put(key, value);
            std::cout << "Inserted: (" << key << ", " << value << ")" << std::endl;
        } else if (command == "get") {
            int64_t key;
            iss >> key;
            const int64_t* value = db.get(key, false);
            if (value) {
                std::cout << "Value: " << *value << std::endl;
            } else {
                std::cout << "Key not found." << std::endl;
            }
        } else if (command == "scan") {
            int64_t key1, key2;
            iss >> key1 >> key2;

            const auto* results = db.scan(key1, key2, true);
            if (results && !results->empty()) {
                for (const auto& pair : *results) {
                    std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
                }
            } else {
                std::cout << "No entries found in the specified range." << std::endl;
            }
        } else if (command == "del") {
            int64_t key;
            iss >> key;
            db.del(key);
            std::cout << "Deleted key: " << key << std::endl;
        } else {
            std::cout << "Unknown command." << std::endl;
        }
    }

    return 0;
}

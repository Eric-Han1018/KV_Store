#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "rbtree.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

// TODO: Insert a key-value pair into the memtable
void RBTree::put(const int64_t& key, const int64_t& value) {
    // FUTURE-TODO: Check if the key already exists and update its value

    // Check if current tree size reaches maximum, and write to SST
    if (curr_size >= memtable_size) {
        string file_path = writeToSST();
        cout << "Memtable capacity reaches maximum. Data has been " <<
                "saved to: " << file_path << endl;
    }

    // Otherwise, insert a new node
    Node* node = new Node(key, value);
    insertNode(node);
}

// Retrieve a value by key
int64_t RBTree::get(const int64_t& key) {
    // Search for the key in the Red-Black Tree
    Node* node = search(root, key);

    if (node == nullptr) {
        cout << "Not found Key: " << key << " in memtable. Now searching SSTs..." << endl;
        return search_SSTs(key);
    } else {
        return node->value;
    }
}

// Search matching key in memtable
Node* RBTree::search(Node* root, const int64_t& key) {
    if (root == nullptr || root->key == key) {
        return root;
    }

    if (key < root->key) {
        return search(root->left, key);
    } else {
        return search(root->right, key);
    }
}

// Search matching key in SSTs in the order of youngest to oldest
int64_t RBTree::search_SSTs(const int64_t& key) {
    // Iterate to read each file in descending order (new->old)
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        cout << "Searching in file: " << *file_path_itr << "..." << endl;

        // Skip if the key is not between min_key and max_key
        int64_t min_key, max_key;
        parse_SST_name(*file_path_itr, min_key, max_key);
        if (key < min_key || key > max_key) {
            cout << "key is not in range of: " << *file_path_itr << endl;
            continue;
        }

        int64_t value = search_SST(*file_path_itr, key);
        if (value != -1) return value;
        cout << "Not found key: " << key << " in file: " << *file_path_itr << endl;
    }
    
    return -1;
}

// Get min_key and max_key from a SST file's name
void RBTree::parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key) {
    vector<string> parsed_name(4);
    string segment;
    stringstream name_stream(file_name);
    // Split by "_" and "."
    int first = file_name.find("_");
    int last = file_name.find_last_of("_");
    int dot = file_name.find_last_of(".");

    min_key = strtoll(file_name.substr(first+1, last - first - 1).c_str(), nullptr, 10);
    max_key = strtoll(file_name.substr(last+1, dot - last - 1).c_str(), nullptr, 10);
}

// Helper function to search the key in a SST file
int64_t RBTree::search_SST(const fs::path& file_path, const int64_t& key) {
    auto file_size = fs::file_size(file_path);
    auto num_elements = file_size / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    int low = 0;
    int high = num_elements - 1;
    int mid;

    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY);
    assert(fd != -1);


    // Binary search
    while (low <= high) {
        mid = (low + high) / 2;
        // Do one I/O at each hop
        // FIXME: need to confirm if this is what required
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, mid*constants::PAIR_SIZE);
        assert(ret == constants::PAIR_SIZE);

        if (cur.first == key) {
            return cur.second;
        } else if (cur.first > key) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    close(fd);

    return -1;
}

// Scan the memtable and SST to retrieve all KV-pairs in a key range in key order (key1 < key2)
vector<pair<int64_t, int64_t>> RBTree::scan(const int64_t& key1, const int64_t& key2) {
    // Check if key1 < key2
    assert(key1 < key2);

    vector<pair<int64_t, int64_t>> sorted_KV;
    size_t len;

    // Scan the memtable
    scan_memtable(sorted_KV, root, key1, key2);

    // Scan each SST
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        cout << "Scanning file: " << *file_path_itr << "..." << endl;
        // Skip if the keys is not between min_key and max_key
        int64_t min_key, max_key;
        parse_SST_name(*file_path_itr, min_key, max_key);
        if (key2 < min_key || key1 > max_key) {
            cout << "key is not in range of: " << *file_path_itr << endl;
            continue;
        }

        // Store current size of array for merging
        len = sorted_KV.size();

        // Scan the SST
        scan_SST(sorted_KV, *file_path_itr, key1, key2);

        // Merge into one sorted array
        // FIXME: ask Prof if merge() is allowed
        // FIXME: is using merge efficient?
        inplace_merge(sorted_KV.begin(), sorted_KV.begin()+len, sorted_KV.end());
    }

    return sorted_KV;
}

// Scan SST to get keys within range
// The implementation is similar with search_SST()
void RBTree::scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2) {
    auto file_size = fs::file_size(file_path);
    int num_elements = file_size / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    int low = 0;
    int high = num_elements - 1;
    int mid;

    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY);
    assert(fd != -1);


    // Binary search to find the first element >= key1
    while (low != high) {
        mid = (low + high) / 2;
        // Do one I/O at each hop
        // FIXME: need to confirm if this is what required
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, mid*constants::PAIR_SIZE);
        assert(ret == constants::PAIR_SIZE);

        if (cur.first < key1) {
            low = mid + 1; // target can only in right half
        } else {
            high = mid; // target can at mid or in left half
        }
    }

    // Low and high both points to what we are looking for
    for (auto i=low; i < num_elements ; ++i) {
        // Iterate each element and push to vector
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, i*constants::PAIR_SIZE);
        assert(ret == constants::PAIR_SIZE);

        if (cur.first <= key2) {
            sorted_KV.push_back(cur);
        } else {
            break; // until meeting the first value out of range
        }
    }

    close(fd);
}

// Helper function to recursively perform inorder scan
void RBTree::scan_memtable(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root, const int64_t& key1, const int64_t& key2) {
    if (root != nullptr) {
        scan_memtable(sorted_KV, root->left, key1, key2);
        // Only include KV-pairs that are in the range
        if (key1 <= root->key && root->key <= key2) {
            sorted_KV.push_back({root->key, root->value});
        }
        scan_memtable(sorted_KV, root->right, key1, key2);
    }
}

// When memtable reaches its capacity, write it into an SST
string RBTree::writeToSST() {
    // Content in std::vector is stored contiguously
    vector<pair<int64_t, int64_t>> sorted_KV;
    scan_memtable(sorted_KV, root, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());

    // Create file name based on current time
    // TODO: modify file name to a smarter way
    string file_name = constants::DATA_FOLDER;
    time_t current_time = time(0);
    file_name.append(to_string(current_time)).append("_").append(to_string(min_key)).append("_").append(to_string(max_key)).append(".bytes");

    // Write data structure to binary file
    // FIXME: do we need O_DIRECT for now?
    int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_SYNC, 0777);
    assert(fd!=-1);
    int test = pwrite(fd, (char*)&sorted_KV[0], sorted_KV.size()*constants::PAIR_SIZE, 0);
    assert(test!=-1);
    close(fd);

    // Add to the maintained directory list
    sorted_dir.push_back(file_name);

    // Clear the memtable
    clear_tree();

    return file_name;
}

/*
 * Helper function to perform left rotation
 *
 * Simple illustration for left rotation on 'x':
 *
 *    root                            root
 *     /                               /
 *    x                               y
 *   /  \          ------>           / \
 *  lx   y                          x  ry
 *     /   \                       /  \
 *    ly   ry                     lx  ly
 *
 *  After left rotation on 'x', 'y' become the new root of the subtree with left child as x,
 *  and the previous left-child of 'y' will become the right-child of 'x'.
 *  Note that, the left-child of x and right-child of y remain the same.
 *  lx, ly, ry can be nullptr.
 */
void RBTree::rotateLeft(Node* x) {
    /* Step 1: mark the right-child of x as y. */
    Node *y = x->right;

    /* Step 2: the left-child of y become the right-child of x, and update child's parent if not null */
    x->right = y->left;
    if (y->left != NULL) {
        y->left->parent = x;
    }

    /* Step 3: set x and y parents accordingly */
    y->parent = x->parent;
    if (x->parent == NULL) {
        root = y;               // x is root
    } else if (x->parent->left == x) {
        x->parent->left = y;    // x is the left child of its parent
    } else {
        x->parent->right = y;    // x is the right child of its parent
    }

    /* Step 4: lastly, set the left-child of y as x and set x's parent to y */
    y->left = x;
    x->parent = y;
}

/*
 * Helper function to perform right rotation
 *
 * Simple illustration for right rotation on 'x':
 *
 *       root                             root
 *        /                                /
 *       x                                y                
 *      /  \        -------->            /  \
 *     y   rx                           ly   x 
 *    / \                                   / \
 *  ly  ry                                ry  rx
 *
 * Right rotation is symmetrical to left rotation.
 */
void RBTree::rotateRight(Node* x) {
    /* Step 1: mark the left-child of x as y. */
    Node *y = x->left;

    /* Step 2: the right-child of y become the left-child of x, and update child's parent if not null */
    x->left = y->right;
    if (y->right != NULL) {
        y->right->parent = x;
    }

    /* Step 3: set x and y parents accordingly */
    y->parent = x->parent;
    if (x->parent == NULL) {
        root = y;               // x is root
    } else if (x->parent->left == x) {
        x->parent->left = y;    // x is the left child of its parent
    } else {
        x->parent->right = y;    // x is the right child of its parent
    }

    /* Step 4: lastly, set the right-child of y as x and set x's parent to y */
    y->right = x;
    x->parent = y;
}

/* 
 * Fix Red-Black tree properties violations after insertion.
 *
 * For insertion, possible proporties violations are:
 * 1. The root is black.
 * 2. If a node is red, then both of its children are black.
 * 
 * Since we color newly inserted node to red, so property #1 will be violated 
 * when we insert a root. We can fix it by simply color the root black.
 * 
 * Property #2 (if a node is red, then both its children are black) will violate 
 * when the parent of the inserted node is red. Note that, in the case of violation
 * property #2, the grandparent of the inserted node must be black. 
 * 
 * There can be 6 different cases:
 * Cases 1-3 are when the parent of the node is a left child of its parent 
 * Cases 4-6 are vice-versa, when the parent of the node is a right child of its parent
 * (cases 4-6 are symmetric to cases 1-3)
 * 
 * More detail information can be found: https://www.codesdope.com/course/data-structures-red-black-trees-insertion/
 * 
 */
void RBTree::insertFixup(Node* node) {
    Node* y;
    while (node->parent && node->parent->color == red) {
        if (node->parent == node->parent->parent->left) {
            /* Cases 1-3: the parent of the node is a left child of its parent */
            y = node->parent->parent->right; // uncle of node
            if (y && y->color == red) {
                /* Case 1: When uncle of node is also red:
                * shift the red color upward until there is no violation,
                * the black height of any node won't be affected.
                */
                node->parent->color = black;
                y->color = black;
                node->parent->parent->color = red;
                node = node->parent->parent;
                continue;
            } else {
                /* Case 2 and 3: 
                * the uncle of the node is black and the node is the right/left child. 
                */
                if (node->parent->right == node) {
                    /* transform the case 2 into 3 by performing left rotation */
                    node = node->parent; 
                    rotateLeft(node);
                }

                /* color parent black and grandparent red, and right rotation on the grandparent. */
                node->parent->color = black;
                node->parent->parent->color = red;
                rotateRight(node->parent->parent);
            }
        } else {
            /* Cases 4-6: the parent of the node is a right child of its parent. 
            * Code is symmetric as above.
            */
            y = node->parent->parent->left;
            if (y && y->color == red) {
                node->parent->color = black;
                y->color = black;
                node->parent->parent->color = red;
                node = node->parent->parent;
                continue;
            } else {
                if (node->parent->left == node) {
                    node = node->parent; 
                    rotateRight(node);
                }
                node->parent->color = black;
                node->parent->parent->color = red;
                rotateLeft(node->parent->parent);
            }
        }
    }
    root->color = black;
}

// Insert a node into the Red-Black Tree
void RBTree::insertNode(Node* node) {
    // Perform a standard Binary Search Tree insertion
    Node* y = nullptr;
    Node* x = root;

    while (x != nullptr) {
        y = x;
        if (node->key < x->key)
            x = x->left;
        else
            x = x->right;
    }

    // Set the parent of the new node
    node->parent = y;

    if (y == nullptr) {
        // If the tree is empty, make the new node the root
        root = node;
    } else if (node->key < y->key) {
        // Otherwise, insert the new node as a left child if the key is smaller
        y->left = node;
    } else {
        // Insert as a right child otherwise
        y->right = node;
    }

    // Fix any Red-Black Tree violations that may have been introduced by the insertion
    node->color = red;
    insertFixup(node);

    // Update tree metadata
    curr_size++;
    if (node->key < min_key) min_key = node->key;
    else if (node->key > max_key) max_key = node->key;

    cout << "Insert key: " << node->key << " value: " << node->value << endl;
}

// Clear all the nodes in the tree
void RBTree::clear_tree() {
    delete root;
    root = nullptr;
    curr_size = 0;
    min_key = numeric_limits<int64_t>::max();
    max_key = numeric_limits<int64_t>::min();
}

// TODO: Helper function to delete a node from the Red-Black Tree
void RBTree::deleteNode(Node* node) {
    // Implement deletion logic here
}
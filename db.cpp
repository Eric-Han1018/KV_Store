#include <iostream>
#include <map>
#include <list>
using namespace std;

// Define the Red-Black Tree node structure
struct Node {
    double key;
    double value;
    bool is_red;
    Node* left;
    Node* right;
    Node* parent;

    // Constructor for a new node
    // New node is inserted as red node
    Node(const double& k, const double& v)
        : key(k), value(v), is_red(true), left(nullptr), right(nullptr), parent(nullptr) {}
};

class RedBlackMemtable {
private:
    // Helper function to traverse the tree to search for a node with given key
    Node* search(Node* root, const double& key) {
        if (root == nullptr || root->key == key) {
            return root;
        }

        if (key < root->key) {
            return search(root->left, key);
        } else {
            return search(root->right, key);
        }
    }

    // TODO: Helper function to perform left rotation
    void rotateLeft(Node* node) {
        // Implement left rotation here
    }

    // TODO: Helper function to perform right rotation
    void rotateRight(Node* node) {
        // Implement right rotation here
    }

    // TODO: Helper function to fix Red-Black Tree properties violations after insertion
    void fixProperties(Node* node) {
        // Implement fix for Red-Black Tree properties violations here
    }

    // Helper function to insert a node into the Red-Black Tree
    void insertNode(Node* node) {
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

        // Initialize additional properties for the new node
        // node->left = nullptr;
        // node->right = nullptr;

        // Fix any Red-Black Tree violations that may have been introduced by the insertion
        fixProperties(node);
        curr_size++;
        cout << "Insert key: " << node->key << " value: " << node->value << endl;
    }

    // TODO: Helper function to delete a node from the Red-Black Tree
    void deleteNode(Node* node) {
        // Implement deletion logic here
    }

public:
    Node* root = nullptr;   // Red-Black Tree root
    size_t memtable_size;   // Maximum capacity
    size_t curr_size;       // Current size

    RedBlackMemtable(size_t capacity) : memtable_size(capacity) {}

    // TODO: Insert a key-value pair into the memtable
    void put(const double& key, const double& value) {
        // Check if the key already exists and update its value (?)

        // Otherwise, insert a new node
        Node* node = new Node(key, value);
        insertNode(node);

        // TODO: Implement the logic to handle rotation and capacity

    }

    // Retrieve a value by key
    double get(const double& key) {
        // Search for the key in the Red-Black Tree
        Node* node = search(root, key);

        if (node == nullptr) {
            cout << "Not found Key: " << key << endl;
            return -1; // for now
        } else {
            return node->value;
        }
    }
};

int main() {
    // TODO: Make more throughout tests
    RedBlackMemtable memtable(5); // Example with memtable_size 5

    memtable.put(1, 10);
    memtable.put(2, 20);
    memtable.put(3, 30);
    memtable.put(4, 40);
    memtable.put(5, 50);

    double key = 3;
    double value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    return 0;
}
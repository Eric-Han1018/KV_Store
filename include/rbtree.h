#include <iostream>
using namespace std;

enum Color {black, red};

class Node {
    public:
        int key;
        int value;
        Color color;
        Node* parent;
        Node* left;
        Node* right;

        Node(int key, int value, Color color=black, Node* parent=nullptr, Node* left=nullptr, Node* right=nullptr): 
            key(key), value(value), color(color), parent(parent), left(left), right(right) {}

};

class RBTree {
    public:
        Node* root;
        size_t memtable_size;   // Maximum capacity
        size_t curr_size;       // Current size

        RBTree(size_t capacity, Node* root=nullptr): root(root), memtable_size{capacity} {}
        ~RBTree() {
            // FIXME: remember to implement
        }

        void put(const double& key, const double& value);
        double get(const double& key);


    private:
        Node* search(Node* root, const double& key);
        void rotateLeft(Node* node);
        void rotateRight(Node* node);
        void selfBalance(Node* node);
        void insertNode(Node* node);
        void deleteNode(Node* node);
};
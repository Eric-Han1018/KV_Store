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

        RBTree(Node* root=nullptr): root(root) {}
        ~RBTree() {
            // FIXME: remember to implement
        }

        // Add member functions

};
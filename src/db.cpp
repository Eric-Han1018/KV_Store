#include <iostream>
#include "rbtree.h"
using namespace std;

int main(int argc, char **argv) {
    RBTree* tree = new RBTree();
    tree->root = new Node(1, 0);

    cout << "(" << tree->root->key << ", " << tree->root->value << ")" << endl;
}
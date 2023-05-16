//
// Created by zqjpeter on 2023/2/24.
//

#ifndef HOMEWORK2_SKIPLIST_H
#define HOMEWORK2_SKIPLIST_H

#include<cmath>
#include<ctime>
#include<iostream>
#include<random>
#include<string.h>
#include<utility> // to use std::pair


class SkipNode{
public:
    uint64_t key;
    // int value;
    std::string value = "";

    SkipNode * successorNode = nullptr;
    SkipNode * underNode = nullptr;
    bool isGuard = false;
};

class SkipList {
private:
    /*
     * the definition of height of list:
     * top: n
     * bottom: 1
     * */
    int maxHeight = 8;
    int currentHeight = 0;
    double probabilityCoin = 0;

    int numberOfSearch = 0; //for testing the efficiency of searching in the list

    SkipNode * headGuards = nullptr;
    SkipNode * tailGuards = nullptr;

    SkipNode ** rightestNode = nullptr; //used while inserting a new SkipNode
//    SkipNode * rightestNode = nullptr;

private:
    SkipNode* searchNodePrivate(uint64_t keyToSearch);   //return the pointer(if the key doesn't exist, return nullptr

public:
//    SkipList(double probability, int scale);
    SkipList();
    int coinFlipper();
    void insertNode(uint64_t keyToSearch, std::string valueToReplace);
    SkipNode * searchNode(uint64_t keyToSearch); // return the value of the SkipNode based on its key

    void debugFunc();
    int searchNum();

    void printTheList();

    ~SkipList();

    // 获得所有的key-value pairs
    std::vector<std::pair<uint64_t, std::string>> getAllKVPairs();

    // 获得最大的key和最小的key
    uint64_t getMinKey();
    uint64_t getMaxKey();

    int getNumOfKVPairs();
};


#endif //HOMEWORK2_SKIPLIST_H

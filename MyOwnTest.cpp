#include <iostream>
#include "SkipList.h"
#include <vector>
#include <cassert>
#include "utils.h"

/**
 * @brief 用于测试SkipList的功能
 * @return null
 */
void testSkipList()
{
    SkipList * skipList = new SkipList();

    std::cout << skipList->getNumOfKVPairs() << std::endl;

    skipList->insertNode(1, "1");
    skipList->insertNode(2, "2");
    skipList->insertNode(3, "3");
    skipList->insertNode(4, "4");

    std::cout << skipList->getNumOfKVPairs() << std::endl;

    std::vector<std::pair<uint64_t, std::string>> allKVPairs = skipList->getAllKVPairs();
    std::cout << "allKVPairs.size() = " << allKVPairs.size() << std::endl;
    for(auto & allKVPair : allKVPairs)
    {
        std::cout << allKVPair.first << " " << allKVPair.second << std::endl;
    }

    // 测试最大、最小值
    std::cout << "最值" << std::endl;
    std::cout << "minKey = " << skipList->getMinKey() << std::endl;
    std::cout << "maxKey = " << skipList->getMaxKey() << std::endl;

    // 重复插入
    std::cout << "重复插入" << std::endl;
    skipList->insertNode(1, "easonChan");
    skipList->insertNode(2, "easonChan");
    skipList->insertNode(2, "jayChou");
    allKVPairs = skipList->getAllKVPairs();
    std::cout << "allKVPairs.size() = " << allKVPairs.size() << std::endl;
    for(auto & allKVPair : allKVPairs)
    {
        std::cout << allKVPair.first << " " << allKVPair.second << std::endl;
    }

    delete skipList;

}

/**
 * @brief 输出的子目录名称：
 * 【level-0】 <- 输出长这样
 * 
 */
void testFileReadWrite()
{
    std::string currentDir = "./data";
    if(utils::dirExists(currentDir))
    {
        std::cout << "文件夹存在" << std::endl;
        // 存在的话就读文件到vector里面，我想看看读进来的子目录名称的格式
        std::vector<std::string> subDirNames;
        utils::scanDir(currentDir, subDirNames);
        for(int i = 0; i < subDirNames.size(); i++)
        {
            std::cout << subDirNames[i] << std::endl;
        }
    }
    else
    {
        std::cout << "文件夹不存在" << std::endl;
    }
}

void testMemoryLeaking()
{
    SkipNode * headNode1 = new SkipNode();
//    SkipNode * headNode2 = new SkipNode();
//
//    SkipNode ** newTower = new SkipNode*[2];
//    newTower[0] = new SkipNode();
//    newTower[1] = new SkipNode();
//
//    newTower[0]->underNode = newTower[1];
//
//    headNode1->successorNode = newTower[0];
//    headNode2->successorNode = newTower[1];
//
//    delete headNode1->successorNode;
//    delete headNode2->successorNode;
//
//    delete headNode1;
//    delete headNode2;
//
//    delete [] newTower;


}

void anotherTestForSkipList()
{
    SkipList * skipList = new SkipList();
    for(int i = 0; i < 1000; i++)
    {
        skipList->insertNode(i, std::to_string(i));
    }
    std::cout << "insert done" << std::endl;
    for(int i = 0; i < 1000; i++)
    {
//        std::string value = skipList->searchNode(i)->value;
//        std::cout << value << std::endl;
        // use assert to check
        assert(skipList->searchNode(i)->value == std::to_string(i));
    }
    std::cout << "search done" << std::endl;
    // get min key
    assert(skipList->getMinKey() == 0);
    // get max key
    assert(skipList->getMaxKey() == 999);
    // get number of kv pairs
    assert(skipList->getNumOfKVPairs() == 1000);

    delete skipList;
}

int main()
{
//    testSkipList();
//    testFileReadWrite();
//    testMemoryLeaking();
    anotherTestForSkipList();
    return 0;
}
#include <iostream>
#include "SkipList.h"
#include <vector>

#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#include "utils.h"

/**
 * @brief 用于测试SkipList的功能
 * 
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
    for(int i = 0; i < allKVPairs.size(); i++)
    {
        std::cout << allKVPairs[i].first << " " << allKVPairs[i].second << std::endl;
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
    for(int i = 0; i < allKVPairs.size(); i++)
    {
        std::cout << allKVPairs[i].first << " " << allKVPairs[i].second << std::endl;
    }

    _CrtDumpMemoryLeaks();
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

int main()
{
    // testSkipList();
    testFileReadWrite();
    return 0;
}
#include <iostream>
#include "SkipList.h"
#include <vector>
#include <cassert>
#include "utils.h"
#include "Memtable.h"
#include "BloomFilter.h"
#include <fstream>
#include "SSTable.h"

#define MAX_MEMTABLE_SIZE 2 * 1024 * 1024

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
 * @brief 用于测试文件读写：“输出的子目录名称”长什么样子
 * 【level-0】 <- 输出长这样
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

/**
 * 用于检测内存泄漏
 * */
void testMemoryLeaking()
{
//    SkipNode * headNode1 = new SkipNode();
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

/**
 * 用于检测跳表的功能和内存泄漏
 * */
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

/**
 * 用于检测由MemTable到二进制文件的转换是否正确
 * */
class CheckClass{
private:
    MemTable * memTable0;
    uint64_t currentTimestamp = 1086;
    std::string dataStoreDir = "./data";
    int explodingEdge = 2600;
    SSTable * ssTable0;

public:
    CheckClass()
    {
        this->memTable0 = new MemTable();
        this->ssTable0 = new SSTable();
    }
    ~CheckClass()
    {
        delete this->memTable0;
        delete this->ssTable0;
    }
    void addToMemTableUntilExplode()
    {
        for(int i = 0; i < 2601; i++)
        {
            std::string s = std::to_string(i);
//            if(memTable0->getSize() + s.size() >= MAX_MEMTABLE_SIZE)
//            {
//                explodingEdge = i - 1;
//                // 写入磁盘
//                convertMemTableIntoMemory();
//                this->readFileToSSTable();
//                break;
//            }
            memTable0->put(i, s);
        }
        convertMemTableIntoMemory();
        this->readFileToSSTable();
    }
    void convertMemTableIntoMemory()
    {
        // 将memtable中的内容写入磁盘；但是不加入缓存，因为是析构函数的时候使用
        std::vector<std::pair<uint64_t, std::string>> allKVPairs = this->memTable0->getAllKVPairs();

        // 把东西写成文件
        const std::string dir = this->dataStoreDir + "/level-0";
        char *buffer = new char[MAX_MEMTABLE_SIZE];

        // Header, BloomFilter, IndexArea, DataArea
        *(uint64_t *)buffer = this->currentTimestamp;
        *(uint64_t *)(buffer + 8) = allKVPairs.size();
        *(uint64_t *)(buffer + 16) = memTable0->getMinKey();
        *(uint64_t *)(buffer + 24) = memTable0->getMaxKey();

        BloomFilter *bloomFilter = new BloomFilter;

        for (int i = 0; i < allKVPairs.size(); i++)
        {
            bloomFilter->addIntoFilter(allKVPairs[i].first);
        }

        // BloomFilter写入buffer
        for (int i = 0; i < 10240; i++)
        {
            if (bloomFilter->checkBits[i])
            {
                buffer[i + 32] = '1';
            }
            else
            {
                buffer[i + 32] = '0';
            }
        }

        char *indexStart = buffer + 10240 + 32;
        int padding = 0; // 指针走过的量

        // 每一个我写到表里面的偏移量，是相对于数据区开始的偏移量
        char * length_from_data_to_file_begin = (10240 + 32 + 12 * allKVPairs.size()) + buffer;
        uint32_t offset_from_data_begin = 0;

        for (int i = 0; i < allKVPairs.size(); i++)
        {
            // 写入索引区
            *(uint64_t *)(indexStart + padding) = allKVPairs[i].first;
            *(uint32_t *)(indexStart + padding + 8) = offset_from_data_begin;

            // 把string写入数据区
            memcpy(length_from_data_to_file_begin, allKVPairs[i].second.c_str(), allKVPairs[i].second.size());

            padding += 12;
            offset_from_data_begin += allKVPairs[i].second.size();
            length_from_data_to_file_begin += allKVPairs[i].second.size();
        }

        // 文件名取名为当时的时间
        std::string fileName = dir + "/" + std::to_string(this->currentTimestamp) + ".sst";


        // 把buffer写入文件
        std::ofstream fout(fileName, std::ios::out | std::ios::binary);
        fout.write((char *)buffer, offset_from_data_begin + 10240 + 32 + 12 * allKVPairs.size()); // TODO 这里写的时候把完整的长度都写进去了
        fout.close();

        delete[] buffer;
        delete bloomFilter;
    }
    void readFileToSSTable()
    {
        std::cout << "we begin to check" << std::endl;
        this->ssTable0->convertFileToSSTable("./data/level-0/1086.sst");
        for(int i = 0; i <= this->explodingEdge; i++)
        {
            std::string value;
            this->ssTable0->findInSSTable(value, i);
            if(value == std::to_string(i))
            {
                std::cout << "key: " << i << " value: " << value << std::endl;
            }
        }
    }

};

int main()
{
//    testSkipList();
//    testFileReadWrite();
//    testMemoryLeaking();
//    anotherTestForSkipList();
    CheckClass * checkClass = new CheckClass();
    checkClass->addToMemTableUntilExplode();
    return 0;
}
#include "SSTable.h"
#include <iostream>
#include <fstream>

SSTable::SSTable()
{

}

SSTable::~SSTable()
{
    if(header != nullptr)
    {
        delete header;
        header = nullptr;
    }
    if(indexArea != nullptr)
    {
        delete indexArea;
        indexArea = nullptr;
    }
    if(dataArea != nullptr)
    {
        delete[] dataArea;
        dataArea = nullptr;
    }
    dataSize = 0;
}

void SSTable::convertFileToSSTable(std::string routine)
{
    std::cout << "we begin to convert " << routine << " to SSTable" << std::endl;
    std::ifstream fin(routine, std::ios::binary);
    if(!fin.is_open())
    {
        std::cout << "open file error" << std::endl;
        return;
    }
    // 计算整个文件大小

    fin.seekg(0, std::ios::end);    // 定位到文件末尾
    int fileSize = fin.tellg();// 获得文件长度
    fin.seekg(0, std::ios::beg);    // 定位到文件头

    // 开始正式读取文件

    // 读取header
    uint64_t timeStamp;
    uint64_t keyValueNum;
    uint64_t minKey;
    uint64_t maxKey;

    fin.read((char *)&timeStamp, sizeof(uint64_t));
    fin.read((char *)&keyValueNum, sizeof(uint64_t));
    fin.read((char *)&minKey, sizeof(uint64_t));
    fin.read((char *)&maxKey, sizeof(uint64_t));

//    header = new Header(timeStamp, keyValueNum, minKey, maxKey);
    header = new Header;
    header->setAllDataInHeader(timeStamp, keyValueNum, minKey, maxKey);
    // 读取bloom filter
    filter = new BloomFilter;

    fin.read((char *)(filter->checkBits), 10240 * sizeof(char));

    // 读取index area
    indexArea = new IndexArea;
    for(int i = 0; i < keyValueNum; i++)
    {
        uint64_t key;
        uint32_t offset;
        fin.read((char *)&key, sizeof(uint64_t));
        fin.read((char *)&offset, sizeof(uint32_t));
        indexArea->indexDataList.push_back(IndexData(key, offset));
    }

    // 读取data area
    // 数据段的大小：文件大小 - 头部大小 - 布隆过滤器大小 总长度 - 32 - 10240 - 12 * numberOfKVPairs
     int dataSize = 2 * 1024 * 1024 - 32 - 10240 - 12 * keyValueNum;
//    int dataSize = fileSize - 32 - 10240 - 12 * keyValueNum;

    dataArea = new char[dataSize];

    // 读文件读完为止
    fin.read(dataArea, dataSize);

    fin.close();

    std::cout << "we have converted " << routine << " to SSTable" << std::endl;
}

bool SSTable::findInSSTable(std::string & answer, uint64_t key)
{
    // 先判断key是否在范围内
    if(key < header->minKey || key > header->maxKey)
    {
        return false;
    }
    // bloom filter
    if(!filter->searchInFilter(key))
    {
        return false;
    }
    // index area
    for(auto it = this->indexArea->indexDataList.begin(); it != this->indexArea->indexDataList.end(); it++)
    {
        if(it->key == key)
        {
            // data area
            uint32_t offset = it->offset;
            uint32_t length = 0;
            if(it + 1 == this->indexArea->indexDataList.end())
            {
                // BUG 读最后一个文件的时候会不会出问题？
                length = this->dataSize - offset;
            }
            else
            {
                length = (it + 1)->offset - offset;
            }
            answer = std::string(this->dataArea + offset, length);
            return true;
        }
    }
}
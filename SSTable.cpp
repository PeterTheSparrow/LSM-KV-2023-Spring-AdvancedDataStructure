#include "SSTable.h"
#include <iostream>
#include <fstream>
#include <sstream>


SSTable::SSTable()
{

}

SSTable::~SSTable()
{
    delete header;
    header = nullptr;

    delete indexArea;
    indexArea = nullptr;

    delete[] dataArea;
    dataArea = nullptr;

    dataSize = 0;

    // swap释放KVPairs vector内存
    std::vector<KVNode>().swap(KVPairs);
}

void SSTable::convertFileToSSTable(std::string routine)
{
    std::ifstream fin(routine, std::ios::in | std::ios::binary);
    if(!fin.is_open())
    {
        std::cout << "open file error" << std::endl;
        return;
    }
    // 计算整个文件大小

    fin.seekg(0, std::ios::end);    // 定位到文件末尾
    int fileSize = fin.tellg();     // 获得文件长度
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

//    std::cout << timeStamp << std::endl;
//    std::cout << keyValueNum << std::endl;

    header = new Header;
    header->setAllDataInHeader(timeStamp, keyValueNum, minKey, maxKey);

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

    uint32_t dataSize0 = fileSize - 32 - 10240 - 12 * keyValueNum;
    this->dataSize = dataSize0;

    dataArea = new char[dataSize0];

    // 读文件读完为止
    fin.read(dataArea, dataSize0);

    fin.close();

    this->formKVVector();
}

//// 说明：这个查找函数已经废弃，因为现在已经将所有的信息从char * dataArea转移到了std::vector<KVNode> KVPairs中！
//bool SSTable::findInSSTableAbandoned(std::string & answer, uint64_t key)
//{
//    // 先判断key是否在范围内
//    if(key < header->minKey || key > header->maxKey)
//    {
//        return false;
//    }
//    // bloom filter
//    if(!filter->searchInFilter(key))
//    {
//        return false;
//    }
//    // index area
//    for(auto it = this->indexArea->indexDataList.begin(); it != this->indexArea->indexDataList.end(); it++)
//    {
//        if(it->key == key)
//        {
//            // data area
//            uint32_t offset = it->offset;
//            uint32_t length = 0;
//            if(it + 1 == this->indexArea->indexDataList.end())
//            {
//                // BUG 读最后一个文件的时候会不会出问题？
//                length = this->dataSize - offset;
//            }
//            else
//            {
//                length = (it + 1)->offset - offset;
//            }
//            answer = std::string(this->dataArea + offset, length);
////            if(answer == "~DELETED~")
////            {
////                return false;
////            }
//            return true;
//        }
//    }
//}

// 这里默认传入的参数，table1的时间戳比table2的时间戳大
// 其实这里更新header没有意义，因为后面都要全部merge再重塑的，但是唯一有意义的是时间戳，要用最大的时间戳
SSTable* SSTable::mergeTwoTables(SSTable *&table1, SSTable *&table2)
{
    SSTable *newTable = new SSTable;
    int index1 = 0;
    int index2 = 0;
    int size1 = table1->KVPairs.size();
    int size2 = table2->KVPairs.size();
    while(index1 < size1 && index2 < size2)
    {
        if(table1->KVPairs[index1].key < table2->KVPairs[index2].key)
        {
            newTable->KVPairs.push_back(table1->KVPairs[index1]);
            index1++;
        }
        else if(table1->KVPairs[index1].key > table2->KVPairs[index2].key)
        {
            newTable->KVPairs.push_back(table2->KVPairs[index2]);
            index2++;
        }
        else
        {
            newTable->KVPairs.push_back(table1->KVPairs[index1]);
            index1++;
            index2++;
        }
    }
    if(index1 == size1)
    {
        for(int i = index2; i < size2; i++)
        {
            newTable->KVPairs.push_back(table2->KVPairs[i]);
        }
    }
    else
    {
        for(int i = index1; i < size1; i++)
        {
            newTable->KVPairs.push_back(table1->KVPairs[i]);
        }
    }
    return newTable;
}

// 说明：这里传入的参数已经按照时间戳从大到小排好顺序了
void SSTable::mergeTables(std::vector<SSTable*> &tableList)
{
    // TODO for debug
    std::cout << "---[begin func mergeTables]: tableList.size() = " << tableList.size() << std::endl;
    // 二分法合并
    uint64_t timeStamp = tableList[0]->header->timeStamp; // 取最大的作为新的时间戳

    SSTable::mergeRecursively(tableList);
    tableList[0]->header = new Header;
    tableList[0]->header->timeStamp = timeStamp;
}

void SSTable::mergeRecursively(std::vector<SSTable*> &tableList)
{
    // TODO for debug
    std::cout << "  merge recursively: tableList.size() = " << tableList.size() << std::endl;

    int size = tableList.size();
    if(size == 1)
    {
        return;
    }
    std::vector<SSTable*> nextRound;
    for(int i = 0; i < size / 2; i++)
    {
        nextRound.push_back(mergeTwoTables(tableList[2*i],tableList[2*i+1]));
    }
    if(size % 2 == 1)
    {
        nextRound.push_back(tableList[size-1]);
    }
    // 清除
    mergeRecursively(nextRound);
    tableList = nextRound;
}


void SSTable::formKVVector()
{
    for(auto it = this->indexArea->indexDataList.begin(); it != this->indexArea->indexDataList.end(); it++)
    {
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
        std::string value = std::string(this->dataArea + offset, length);
        KVPairs.push_back(KVNode(it->key, value));
    }
}

bool SSTable::findInSSTable(std::string &answer, uint64_t key)
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
    // search in KVPairs
    for(auto it = KVPairs.begin(); it != KVPairs.end(); it++)
    {
        if(it->key == key)
        {
            answer = it->value;
            return true;
        }
    }
    return false;
}

// 传入是巨大没有被切割的SSTable，将其切割成小的SSTable，然后存到disk里面去
// 同时生成所有这些文件的缓存，将指针传回到KVStore里面去，将缓存保存起来
// 实现切分和保存文件
// TAG 合并文件的时候，时间戳是怎么定的——已经是当前最大的时间戳了
std::vector<SSTableCache *> SSTable::splitAndSave(std::string routine) {
    std::vector<SSTableCache *> newCache;
    // 循环切分
    int fileTag = 0; // 同一时间戳，可能存在多个文件，需要区分标记
//    int currentSize = 32 + 10240 + 12 * KVPairs.size();
    int currentSize = 0;
    for(auto it = this->KVPairs.begin(); it != KVPairs.end(); it++)
    {
        currentSize += it->value.size();
    }
    // 每次都切出来一张新的表
    while(currentSize > 0)
    {
        // 这里没有必要分开，直接切就可以了
        SSTableCache *newTable = cutOutOneSSTable(fileTag, routine, currentSize);
        newCache.push_back(newTable);
        fileTag++;
    }

    return newCache;
}

// 这里传入的routine参数：./data/level-X
// 切出来一个SSTable的文件，并且把它写入磁盘
SSTableCache * SSTable::cutOutOneSSTable(int fileTag, std::string routine, int & currentSize) {
    SSTableCache *newCache = new SSTableCache;
    newCache->indexArea = new IndexArea;

    // 参考函数:KVStore::convertMemTableIntoMemory
    const std::string dir = routine + "/" + std::to_string(this->header->timeStamp) + "-" + std::to_string(fileTag) + ".sst";
    char * buffer = new char[MAX_TABLE_SIZE];

    // 计算要写入的数据
    uint32_t size = 32 + 10240;
    std::vector<KVNode> pairsToWrite;
    for(auto it = this->KVPairs.begin(); it != this->KVPairs.end();)
    {
        if(size + it->value.size() + 12 > MAX_TABLE_SIZE)
        {
            break;
        }
        size += it->value.size() + 12;
        currentSize -= it->value.size();
        pairsToWrite.push_back(*it);
        KVPairs.erase(it);
        // TODO 这里我删除了循环中的it++
    }

    uint64_t minKey = pairsToWrite[0].key;
    uint64_t maxKey = pairsToWrite[pairsToWrite.size() - 1].key;

    // Header, BloomFilter, IndexArea, DataArea
    // Header
    newCache->header->timeStamp = this->header->timeStamp;
    newCache->header->minKey = minKey;
    newCache->header->maxKey = maxKey;
    newCache->header->keyValueNum = pairsToWrite.size();

    *(uint64_t *)buffer = this->header->timeStamp;
    *(uint64_t *)(buffer + 8) = pairsToWrite.size();
    *(uint64_t *)(buffer + 16) = minKey;
    *(uint64_t *)(buffer + 24) = maxKey;

    // Bloom Filter
    for(int i = 0; i < pairsToWrite.size(); i++)
    {
        newCache->bloomFilter->addIntoFilter(pairsToWrite[i].key);
    }

    for(int i = 0; i < 10240; i++)
    {
        if(newCache->bloomFilter->checkBits[i])
        {
            buffer[i+32] = '1';
        }
        else
        {
            buffer[i+32] = '0';
        }
    }

    char *indexStart = buffer + 10240 + 32;
    int padding = 0;

    char * length_from_data_to_file_begin = (10240 + 32 + 12 * pairsToWrite.size() + buffer);
    // for debug
    uint32_t length_from_data_to_file_begin_int = (10240 + 32 + 12 * pairsToWrite.size());
    uint32_t offset_from_data_begin = 0;

    for(int i = 0; i < pairsToWrite.size(); i++)
    {
        // 写入索引区
        *(uint64_t *)(indexStart + padding) = pairsToWrite[i].key;
        *(uint32_t *)(indexStart + padding + 8) = offset_from_data_begin;

        // 写入缓存
        struct IndexData * newIndexData = new IndexData(pairsToWrite[i].key, offset_from_data_begin);
        newCache->indexArea->indexDataList.push_back(*newIndexData);
        delete newIndexData;

        // 把string写入数据区
        // TODO this is buggy
        memcpy(length_from_data_to_file_begin, pairsToWrite[i].value.c_str(), pairsToWrite[i].value.size());

        padding += 12;
        offset_from_data_begin += pairsToWrite[i].value.size();
        length_from_data_to_file_begin += pairsToWrite[i].value.size();
        // for debug
        length_from_data_to_file_begin_int += pairsToWrite[i].value.size();
    }

    // 把buffer写入文件
    std::ofstream fout(dir, std::ios ::out | std::ios::binary);
    fout.write((char *)buffer, offset_from_data_begin + 10240 + 32 + 12 * pairsToWrite.size());
    fout.close();

    delete [] buffer;

    // 设置newCache中其他的必要数据
    newCache->setAllData(minKey, maxKey, pairsToWrite.size(), this->header->timeStamp, dir, this->header->timeStamp);

    return newCache;
}

SSTableCache::SSTableCache()
{
    header = new Header;
    bloomFilter = new BloomFilter;
    indexArea = nullptr;
}

SSTableCache::~SSTableCache()
{
    delete header;
    header = nullptr;

    delete bloomFilter;
    bloomFilter = nullptr;

    delete indexArea;
    indexArea = nullptr;
}

void SSTableCache::setAllData(uint64_t minKey, uint64_t maxKey, uint64_t numberOfPairs, uint64_t timeStamp, std::string fileName, uint64_t currentTime)
{
    this->header = new Header;
    this->header->setAllDataInHeader(timeStamp, numberOfPairs, minKey, maxKey);
    this->fileRoutine = fileName;
    bloomFilter = new BloomFilter;
    indexArea = new IndexArea;
//    this->timeStamp = currentTime;
}


void SSTableCache::readFileToFormCache(std::string routine, std::string fileName)
{
    // 解析文件名，获得timeStampIndex
    int theTimeStamp, theTimeStampIndex;
    std::vector<std::string> splitStrings;
    std::istringstream iss(fileName);
    std::string token;

    // split the fileNAme by '-'
    while(std::getline(iss,token,'-'))
    {
        splitStrings.push_back(token);
    }

    std::string subString = splitStrings[1];
    subString = subString.substr(0, subString.length() - 4);

    theTimeStampIndex = std::stoi(subString);

    // 初始化信息
    this->timeStampIndex = theTimeStampIndex;
    this->fileRoutine = routine;

    // open the file and read to form the whole cache
    std::ifstream  fin(routine, std::ios::in | std::ios::binary);
    if(!fin.is_open())
    {
        std::cout << "open file error" << std::endl;
        return;
    }

    // read file
    uint64_t timeStamp;
    uint64_t kvNum;
    uint64_t minKey;
    uint64_t maxKey;

    fin.read((char *)&timeStamp, sizeof(uint64_t));
    fin.read((char *)&kvNum, sizeof(uint64_t));
    fin.read((char *)&minKey, sizeof(uint64_t));
    fin.read((char *)&maxKey, sizeof(uint64_t));

    this->header->setAllDataInHeader(timeStamp, kvNum, minKey, maxKey);

    // read bloom filter
    fin.read((char *)this->bloomFilter->checkBits, sizeof(char) * 10240);

    // read index area
    this->indexArea = new IndexArea;
    for(int i = 0; i < kvNum; i++)
    {
        uint64_t key;
        uint32_t offset;
        fin.read((char *)&key, sizeof(uint64_t));
        fin.read((char *)&offset, sizeof(uint32_t));
        this->indexArea->indexDataList.push_back(IndexData(key,offset));
    }

    fin.close();
}

#include "kvstore.h"
#include <string>
#include <iostream>
#include <fstream>
#include "utils.h"
#include <algorithm>
#include <cmath>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    std::string currentDir = dir;

    if(currentDir[currentDir.size()-1] == '/')
    {
        currentDir = currentDir.substr(0, currentDir.size()-1);
    }

    if (utils::dirExists(currentDir))
    {
        this->dataStoreDir = currentDir;
        // 如果存在，那就读文件，生成缓存，更新class的时间戳
        // 扫描指定目录下的所有文件和子目录，并将文件名和子目录名保存到一个字符串向量中。
        int levelNum = utils::scanDir(currentDir, this->levelDir);
        if(levelNum == 0)
        {
            utils::mkdir((currentDir + "/level-0").c_str());
        }
        else
        {
            for(int i = 0; i < levelNum; i++)
            {
                // 初始化该层的cache
                std::vector<SSTableCache *> newLevelCache;
                this->theCache.push_back(newLevelCache);

                // 读文件，生成缓存，更新时间戳
                // 读文件的时候，memTable是空白的对吧？
                std::vector<std::string> fileNames;
                std::string routine = currentDir + "/level-" + std::to_string(i)+"/";
                int fileNumbers = utils::scanDir(routine, fileNames);
                for(int j = 0; j < fileNumbers; j++)
                {
                    SSTableCache * newCache = new SSTableCache;
                    // TODO 实现读取磁盘来生成缓存信息
                    newCache->readFileToFormCache(routine + fileNames[j], fileNames[j]);
                    this->theCache[i].push_back(newCache);
                    // 更新时间戳
                    if(newCache->header->timeStamp > this->currentTimestamp)
                    {
                        this->currentTimestamp = newCache->header->timeStamp;
                    }
                }
                // TODO 把该层的cache文件按照时间戳排序
                // 因为我们merge的时候，需要把时间戳最小的文件拿出来merge
            }
        }
    }
    else
    {
        utils::mkdir(currentDir.c_str());
        utils::mkdir((currentDir + "/level-0").c_str());
        this->dataStoreDir = currentDir;
        // 先实现内存的部分
        this->memTable0 = new MemTable();
    }
}

KVStore::~KVStore()
{
    // 将MemTable写入磁盘
    if(memTable0->getSize() != 0)
    {
        this->convertMemTableIntoMemoryWithoutCache();
    }
    // 进行一次文件的检查和归并
    this->checkCompaction();

    // 释放内存
    delete memTable0;

    for(int i = 0; i < this->theCache.size(); i++)
    {
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            delete this->theCache[i][j];
        }
    }

    // clean vector
    for(auto & i : this->theCache)
    {
        // 通过swap完全释放内存
        std::vector<SSTableCache *>().swap(i);
    }
    std::vector<std::vector<SSTableCache *>>().swap(this->theCache);
    std::vector<std::string>().swap(this->levelDir);
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    // 判断大小
    if (memTable0->getSize() + s.size() >= MAX_MEMTABLE_SIZE)
    {
        // 将跳表写入SSTable；
        this->convertMemTableIntoMemory();
        this->checkCompaction();

        // 清空跳表，将s写入跳表
        this->memTable0->clearMemTable();
        this->memTable0->put(key, s);
        return;
    }
    // 没有超过大小，直接在跳表里面插入
    memTable0->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string answer = memTable0->get(key);
    if(answer == "~DELETED~")
    {
        return "";
    }
    if (answer != "")
    {
        return answer;
    }


    // TODO 实现存储的部分——读文件，把文件读到内存中，转化为SSTable，然后查找
    // 遍历所有文件缓存
    // TAG 对啊！我遍历文件缓存的时候直接所有都遍历一遍就行了！不用管是第几层的吧，然后对应找文件名就可以（毕竟访问内存里面的速度是很快的，所以不如直接遍历）
    // TAG 这里对应不同的缓存策略，我调用不同的函数
    bool doFind = findInDisk1(answer, key);
    // bool doFind = findInDisk2(answer, key);
    // bool doFind = findInDisk3(answer, key);
    if(doFind)
    {
        return answer;
    }

    return "";
}


/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    // 注意到我们其实不能修改sstable中的内容，我们只能对memtable操作
    // 先全局搜索（注意！不仅仅是搜索内存，也包括磁盘！）
    std::string answer = this->get(key);
    if(answer == "")
    {
        return false;
    }
    else
    {
        this->memTable0->put(key, "~DELETED~");
        return true;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    // 清除内存中的memtable
    if (memTable0 != nullptr)
    {
        delete memTable0;
        memTable0 = nullptr;
    }
    memTable0 = new MemTable();

    // 清除磁盘中的文件
    for(int i = 0; i < this->theCache.size(); i++)
    {
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            std::string routine = theCache[i][j]->fileRoutine;
            utils::rmfile(routine.c_str());
        }
    }

    // 清除内存中的缓存
    for(auto & i : this->theCache)
    {
        for(int j = 0; j < i.size(); j++)
        {
            delete i[j];
        }
    }

    // clean vector
    for(auto & i : this->theCache)
    {
        // 通过swap完全释放内存
        std::vector<SSTableCache *>().swap(i);
    }
    std::vector<std::vector<SSTableCache *>>().swap(this->theCache);

    // 删除文件夹
    for(const auto & i : this->levelDir)
    {
        // 由于levelDir中，保存的实际上是儿子文件夹的名称，所以要加上父亲文件夹的名称才构成路径
        std::string routine = this->dataStoreDir + "/" + i;
        utils::rmdir(routine.c_str());
    }

    /*
     * 重新创建文件夹：
     * （1）这里两个文件夹是要留下来的：/data， /data/level-0
     * */
    utils::mkdir((this->dataStoreDir + "/level-0").c_str());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
}

void KVStore::convertMemTableIntoMemory()
{
    // 将MemTable的东西写入磁盘
    std::vector<std::pair<uint64_t, std::string>> allKVPairs = this->memTable0->getAllKVPairs();

    // 把东西写入缓存
    SSTableCache *newCache;
    newCache = new SSTableCache;
    newCache->indexArea = new IndexArea;

    // 把东西写成文件
    const std::string dir = this->dataStoreDir + "/level-0";
    char *buffer = new char[MAX_MEMTABLE_SIZE];

    // Header, BloomFilter, IndexArea, DataArea
    *(uint64_t *)buffer = this->currentTimestamp;
    *(uint64_t *)(buffer + 8) = allKVPairs.size();
    *(uint64_t *)(buffer + 16) = memTable0->getMinKey();
    *(uint64_t *)(buffer + 24) = memTable0->getMaxKey();

    for (int i = 0; i < allKVPairs.size(); i++)
    {
        newCache->bloomFilter->addIntoFilter(allKVPairs[i].first);
    }

    // BloomFilter写入buffer
    for (int i = 0; i < 10240; i++)
    {
        if (newCache->bloomFilter->checkBits[i])
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

        // TAG 写入缓存
        struct IndexData *newIndexData = new IndexData(allKVPairs[i].first, offset_from_data_begin);
        newCache->indexArea->indexDataList.push_back(*newIndexData);
        delete newIndexData;

        // 把string写入数据区
        memcpy(length_from_data_to_file_begin, allKVPairs[i].second.c_str(), allKVPairs[i].second.size());

        padding += 12;
        offset_from_data_begin += allKVPairs[i].second.size();
        length_from_data_to_file_begin += allKVPairs[i].second.size();
    }

    // 文件名取名为当时的时间
    std::string fileName = dir + "/" + std::to_string(this->currentTimestamp) + "-0.sst";

    // 把buffer写入文件
    std::ofstream fout(fileName, std::ios::out | std::ios::binary);
    fout.write((char *)buffer, offset_from_data_begin + 10240 + 32 + 12 * allKVPairs.size());
    fout.close();

    delete[] buffer;

    newCache->setAllData(memTable0->getMinKey(), memTable0->getMaxKey(), allKVPairs.size(), this->currentTimestamp, fileName, this->currentTimestamp);

    // TAG 其实cache的传入可以在这里，我传入一个cache的指针，然后修改里面的内容，最后把这个指针append到vector里面去！
    // 直接加入第0层的cache，后面调整是后面的事情
    if(this->theCache.empty())
    {
        std::vector<SSTableCache *> newLevel;
        newLevel.push_back(newCache);
        this->theCache.push_back(newLevel);
    }
    else
    {
        this->theCache[0].push_back(newCache);
    }

    this->currentTimestamp += 1;//时间戳只能这里加，因为前面需要更新缓存的时间
    // 把第0层的cache排序，按照时间戳从大到小排，这很重要！
//    std::sort(theCache[0].begin(),theCache[0].end(), [](const SSTableCache * a, SSTableCache *b){
//        return a->header->timeStamp > b->header->timeStamp;
//    });
    std::sort(theCache[0].begin(),theCache[0].end(), SSTableCache::CompareSSTableCache);
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

// 不缓存，直接在文件里查找
bool KVStore::findInDisk1(std::string & answer, uint64_t key)
{
    // 现在只有第0层，查找的时候就遍历文件
    if(this->theCache.empty() ||  this->theCache[0].empty())
    {
        return false;
    }
//    for(auto & it : this->theCache[0])
//    {
//        std::string fileRoutine = it->fileRoutine;
//
//        SSTable *theSSTable = new SSTable;
//        theSSTable->convertFileToSSTable(fileRoutine);
//        if(theSSTable->findInSSTable(answer, key))
//        {
//            delete theSSTable;
//            return true;
//        }
//        delete theSSTable;
//    }
    int levelNumber = this->theCache.size(); // the number of levels in the disk
    // traverse the whole './data' folder
    for(int i = 0; i < levelNumber; i++)
    {
        for(auto & it: this->theCache[i])
        {
            std::string fileRoutine = it->fileRoutine;

            SSTable * theSSTable = new SSTable;
            theSSTable->convertFileToSSTable(fileRoutine);
            if(theSSTable->findInSSTable(answer, key))
            {
                delete theSSTable;
                return true;
            }
            delete theSSTable;
        }
    }
    return false;
}

// 缓存index，使用binary search
bool KVStore::findInDisk2(std::string & answer, uint64_t key)
{
    return false;
}

// 缓存index和bloom filter
bool KVStore::findInDisk3(std::string & answer, uint64_t key)
{
    return false;
}

// 遍历缓存，查看每一层是否需要merge
void KVStore::checkCompaction()
{
    int maxFileNum = LEVEL_CHANGE;
    int levelIndex = 1;
    for(auto it = this->theCache.begin(); it != this->theCache.end(); it++)
    {
        if(it->size() > maxFileNum)
        {
            // 归并当前层
            compactSingleLevel(levelIndex);
        }
        else
        {
            break;
        }
        maxFileNum *= LEVEL_CHANGE;
        levelIndex += 1;
    }
}

// 归并某个特定的层
void KVStore::compactSingleLevel(int levelNum)
{
    // 检查下一层是否存在，如果不存在直接新建一层
    std::vector<SSTable> tablesToMerge;
    int minKeyInAll = INT_MAX, maxKeyInAll = INT_MIN;

    // TODO 记得把缓存里面这些选出来的东西都给删了
    // 应该用erase就可以了

    if(levelNum == 0)
    {
        // 选择第0层的所有文件
        for(auto it = this->theCache[0].begin(); it != this->theCache[0].end(); it++)
        {
            std::string fileRoutine = (*it)->fileRoutine;
            SSTable * theSSTable = new SSTable;
            theSSTable->convertFileToSSTable(fileRoutine);
            if(theSSTable->header->minKey < minKeyInAll)
            {
                minKeyInAll = theSSTable->header->minKey;
            }
            if(theSSTable->header->maxKey > maxKeyInAll)
            {
                maxKeyInAll = theSSTable->header->maxKey;
            }
            tablesToMerge.push_back(*theSSTable);
            // 把cache里的东西删了
            theCache[0].erase(it);
            // 删除磁盘中原来的文件
            utils::rmfile(fileRoutine.c_str());
            delete theSSTable;
        }
    }
    else
    {
        // 选择该层时间戳最小的若干文件
        int fileNum = this->theCache[levelNum].size() - (int)pow(LEVEL_CHANGE, levelNum + 1);
        // TODO 其实这里不一定有必要sort，但是还是sort一下；如果每次我们修改缓存都是按照时间戳排列的话，这里就没有必要sort了
        // 把时间戳从大到小排，选择最后的fileNum个文件
//        std::sort(this->theCache[levelNum].begin(), this->theCache[levelNum].end(), [](SSTableCache * a, SSTableCache * b){
//            return a->header->timeStamp > b->header->timeStamp;
//        });
        // 上面这段lambda在我把缓存的数据结构移出去新建了一个头文件以后就没有办法使用了，我觉得很疑惑
        std::sort(this->theCache[levelNum].begin(), this->theCache[levelNum].end(), SSTableCache::CompareSSTableCache);

        int levelSize = this->theCache[levelNum].size();
        for(int i = levelSize - fileNum; i < levelSize; i++)
        {
            std::string fileRoutine = this->theCache[levelNum][i]->fileRoutine;
            SSTable * theSSTable = new SSTable;
            theSSTable->convertFileToSSTable(fileRoutine);
            if(theSSTable->header->minKey < minKeyInAll)
            {
                minKeyInAll = theSSTable->header->minKey;
            }
            if(theSSTable->header->maxKey > maxKeyInAll)
            {
                maxKeyInAll = theSSTable->header->maxKey;
            }
            tablesToMerge.push_back(*theSSTable);
            // 把cache里的东西删了
            this->theCache[levelNum].erase(this->theCache[levelNum].begin() + i);
            // TODO 这里有可能会出问题，因为erase之后，后面的元素会往前移动，所以i也要减一？？
            // 删除磁盘中的文件
            utils::rmfile(fileRoutine.c_str());
            delete theSSTable;
        }
    }
    levelNum += 1;
    if(levelNum < theCache.size())
    {
        // 在下一层中选择所有数据范围和min与max之间有交集的文件
        for(auto it = this->theCache[levelNum].begin(); it != this->theCache[levelNum].end(); it++)
        {
//            std::string fileRoutine = (*it)->fileRoutine;
//            SSTable * theSSTable = new SSTable;
//            theSSTable->convertFileToSSTable(fileRoutine);
//            // 检查数据范围
//            if(theSSTable->header->minKey <= maxKeyInAll && theSSTable->header->maxKey >= minKeyInAll)
//            {
//                tablesToMerge.push_back(*theSSTable);
//            }
//            delete theSSTable;
            // 其实这里检查数据范围的交集在缓存就能做，如果有交集再读文件，时间上可以好很多
            if((*it)->header->minKey <= maxKeyInAll && (*it)->header->maxKey >= minKeyInAll)
            {
                std::string fileRoutine = (*it)->fileRoutine;
                SSTable * theSSTable = new SSTable;
                theSSTable->convertFileToSSTable(fileRoutine);
                tablesToMerge.push_back(*theSSTable);
                // 把cache里的东西删了
                this->theCache[levelNum].erase(it);
                delete theSSTable;
            }
        }
    }
    else
    {
        // 新增一层
        utils::mkdir((this->dataStoreDir + "/level-" + std::to_string(levelNum)).c_str());
        this->theCache.push_back(std::vector<SSTableCache*>());
    }

    // 将这些SSTable文件merge，扔到下一层
    SSTable::mergeTables(tablesToMerge);

    // 切割获得的新SSTable，将里面的东西保存到磁盘中，同时返回新生成的缓存的std::vector
    std::vector<SSTableCache*> newCache = tablesToMerge[0].splitAndSave(this->dataStoreDir + "/level-" + std::to_string(levelNum));

    // 把获得的新SSTableCache加入到缓存中，同时将该层的缓存按照时间戳从大到小重新排序
    for(auto it = newCache.begin(); it != newCache.end(); it++)
    {
        this->theCache[levelNum].push_back(*it);
    }

    std::sort(this->theCache[levelNum].begin(), this->theCache[levelNum].end(), SSTableCache::CompareSSTableCache);
}

void KVStore::convertMemTableIntoMemoryWithoutCache() {
    // 将MemTable中的内容写入磁盘；但是不加入缓存，因为是析构函数的时候使用
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
    std::string fileName = dir + "/" + std::to_string(this->currentTimestamp) + "-0.sst";


    // 把buffer写入文件
    std::ofstream fout(fileName, std::ios::out | std::ios::binary);
    fout.write((char *)buffer, offset_from_data_begin + 10240 + 32 + 12 * allKVPairs.size());
    fout.close();

    delete[] buffer;
    delete bloomFilter;
}

//SSTableCache::SSTableCache()
//{
//    header = new Header;
//    bloomFilter = new BloomFilter;
//    indexArea = nullptr;
//}
//
//SSTableCache::~SSTableCache()
//{
//    delete header;
//    header = nullptr;
//
//    delete bloomFilter;
//    bloomFilter = nullptr;
//
//    delete indexArea;
//    indexArea = nullptr;
//}
//
//
//void SSTableCache::readFileToFormCache(std::string routine, std::string fileName)
//{
//    // 解析文件名，获得timeStampIndex
//    int theTimeStamp, theTimeStampIndex;
//    std::vector<std::string> splitStrings;
//    std::istringstream iss(fileName);
//    std::string token;
//
//    // split the fileNAme by '-'
//    while(std::getline(iss,token,'-'))
//    {
//        splitStrings.push_back(token);
//    }
//
//    std::string subString = splitStrings[1];
//    subString = subString.substr(0, subString.length() - 4);
//
//    theTimeStamp = std::stoi(splitStrings[0]);
//    theTimeStampIndex = std::stoi(subString);
//
//    // 初始化信息
//    this->timeStampIndex = theTimeStampIndex;
//    this->fileRoutine = routine;
//
//    // open the file and read to form the whole cache
//    std::ifstream  fin(routine, std::ios::in | std::ios::binary);
//    if(!fin.is_open())
//    {
//        std::cout << "open file error" << std::endl;
//        return;
//    }
//
//    // read file
//    uint64_t timeStamp;
//    uint64_t kvNum;
//    uint64_t minKey;
//    uint64_t maxKey;
//
//    fin.read((char *)&timeStamp, sizeof(uint64_t));
//    fin.read((char *)&kvNum, sizeof(uint64_t));
//    fin.read((char *)&minKey, sizeof(uint64_t));
//    fin.read((char *)&maxKey, sizeof(uint64_t));
//
//    this->header->setAllDataInHeader(timeStamp, kvNum, minKey, maxKey);
//
//    // read bloom filter
//    fin.read((char *)this->bloomFilter->checkBits, sizeof(char) * 10240);
//
//    // read index area
//    this->indexArea = new IndexArea;
//    for(int i = 0; i < kvNum; i++)
//    {
//        uint64_t key;
//        uint32_t offset;
//        fin.read((char *)&key, sizeof(uint64_t));
//        fin.read((char *)&offset, sizeof(uint32_t));
//        this->indexArea->indexDataList.push_back(IndexData(key,offset));
//    }
//
//    fin.close();
//}
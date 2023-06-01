#include "kvstore.h"
#include <string>
#include <iostream>
#include <fstream>
#include "utils.h"
#include <algorithm>

/**
 * @brief KVStore的构造函数
 * @param dir 数据存储的目录
 * */
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    std::string currentDir = dir;
    this->memTable0 = new MemTable();
    if(currentDir[currentDir.size()-1] == '/')
    {
        currentDir = currentDir.substr(0, currentDir.size()-1);
    }

    if (utils::dirExists(currentDir))
    {
        this->dataStoreDir = currentDir;
        // 就读文件，生成缓存，更新class的时间戳
        // 扫描指定目录下的所有文件和子目录，并将文件名和子目录名保存到一个字符串向量中
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
                std::vector<std::string> fileNames;
                std::string routine = currentDir + "/level-" + std::to_string(i)+"/";
                int fileNumbers = utils::scanDir(routine, fileNames);
                for(int j = 0; j < fileNumbers; j++)
                {
                    SSTableCache * newCache = new SSTableCache;
                    newCache->readFileToFormCache(routine + fileNames[j], fileNames[j]);
                    this->theCache[i].push_back(newCache);
                    // 更新时间戳
                    if(newCache->header->timeStamp > this->currentTimestamp)
                    {
                        this->currentTimestamp = newCache->header->timeStamp;
                    }
                }
                // 把该层的cache文件按照时间戳排序。因为merge的时候，需要把时间戳最小的文件拿出来merge
                std::sort(this->theCache[i].begin(), this->theCache[i].end(), SSTableCache::CompareSSTableCache);
            }
        }
//        // for debug
//        // 打印缓存信息
//        this->WriteAllCacheInfo(1008989898);
    }
    else
    {
        utils::mkdir(currentDir.c_str());
        utils::mkdir((currentDir + "/level-0").c_str());
        this->dataStoreDir = currentDir;
    }
}

KVStore::~KVStore()
{
    // 将MemTable写入磁盘
    if(memTable0->getSize() != 0)
    {
        this->convertMemTableIntoMemory();
    }
    // 进行一次文件的检查和归并
    this->checkCompaction();

    // 释放内存
    delete memTable0;

    for(uint64_t i = 0; i < this->theCache.size(); i++)
    {
        for(uint64_t j = 0; j < this->theCache[i].size(); j++)
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

    // 通过调用不同的函数，实现不同的缓存策略
//   bool doFind = findInDisk1(answer, key);
//    bool doFind = findInDisk2(answer, key);
     bool doFind = findInDisk3(answer, key);
    if(doFind)
    {
        if(answer == "~DELETED~")
            answer = "";
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
    // 注意到，不能修改SSTable中的内容，我们只能对MemTable操作
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
    // 清除内存中的MemTable
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

/**
 * 在跳表满了之后，将跳表中的数据转化为SSTable并存储在磁盘中；
 * 同时为这张SSTable生成对应的缓存，存储在Cache Vector中。
 * */
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
//            buffer[i + 32] = '1';
            buffer[i + 32] = true;
        }
        else
        {
//            buffer[i + 32] = '0';
            buffer[i + 32] = false;
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


        struct IndexData *newIndexData = new IndexData(allKVPairs[i].first, offset_from_data_begin);
        newCache->indexArea->indexDataList.push_back(*newIndexData);
        delete newIndexData;
//        newCache->indexArea->indexDataList.push_back(IndexData(allKVPairs[i].first, offset_from_data_begin));

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

//    // for debug 打印布隆过滤器的每个byte
//    std::cout << "bloom" << std::endl;
//    for(int i = 0; i < 10240; i++)
//    {
//        std::cout << newCache->bloomFilter->checkBits[i];
//    }
//    std::cout << std::endl;

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
    // 把第0层的cache排序，按照时间戳从大到小排（其实不排喜欢也没关系，反正第0层到时候merge的时候是全部拿走）
    std::sort(theCache[0].begin(),theCache[0].end(), SSTableCache::CompareSSTableCache);

//    // for debug 打印缓存中的布隆过滤器
//    std::cout << "bloom in cache" << std::endl;
//    for(int i = 0; i < 10240; i++)
//    {
//        std::cout << this->theCache[0][0]->bloomFilter->checkBits[i];
//    }
}



/**
 * @brief KVStore::findInDisk1 不使用缓存在内存中的index和bloomfilter，根据文件名读文件查找
 * @param answer 返回的value
 * @param key 要查找的key
 * @return 是否找到
 * */
bool KVStore::findInDisk1(std::string & answer, uint64_t key)
{
    if(this->theCache.empty())
    {
        return false;
    }
//    // for debug
//    std::cout << "-------------we begin to find in disk1------------" << "key: " << key << std::endl;

    // 只在第0层查找
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
//        // for debug
//        std::cout << "-----we are in level " << i << "-----" << std::endl;

        for(auto it = this->theCache[i].begin(); it != this->theCache[i].end(); it++)
        {
//            // for debug
//            std::cout << ">> " << (*it)->fileRoutine <<"  " << "min:" << (*it)->header->minKey << " max:" << (*it)->header->maxKey << std::endl;

            std::string fileRoutine = (*it)->fileRoutine;

            SSTable *theSSTable = new SSTable;
            theSSTable->convertFileToSSTable(fileRoutine);
            if(theSSTable->findInSSTable(answer, key))
            {
                delete theSSTable;
                return true;
            }
            delete theSSTable;
        }
//        for(auto & it: this->theCache[i])
//        {
//            std::string fileRoutine = it->fileRoutine;
//
//            SSTable * theSSTable = new SSTable;
//            theSSTable->convertFileToSSTable(fileRoutine);
//            if(theSSTable->findInSSTable(answer, key))
//            {
//                delete theSSTable;
//                return true;
//            }
//            delete theSSTable;
//        }
    }
    return false;
}


/**
 * @brief KVStore::findInDisk2 使用缓存在内存中的index，进行二分查找，找到以后读文件
 * @param answer 返回的value
 * @param key 要查找的key
 * @return 是否找到
 * */
bool KVStore::findInDisk2(std::string & answer, uint64_t key)
{
    // 遍历缓存，在缓存中搜索，如果搜索到了，直接查询对应的文件即可
    for(auto it1 = this->theCache.begin(); it1 != this->theCache.end(); it1++)
    {
        for(auto it2 = it1->begin(); it2 != it1->end(); it2++)
        {
            // 先使用minKey和maxKey框一下范围
            // 如果在范围里面，再使用二分查找
            if(key < (*it2)->header->minKey || key > (*it2)->header->maxKey)
            {
                continue;
            }
            else
            {
//                // for debug
//                std::cout << "size of indexDataList: " << (*it2)->indexArea->indexDataList.size() << std::endl;
                // 使用二分查找
                int left = 0;
                int right = (*it2)->indexArea->indexDataList.size() - 1;
                while(left <= right)
                {
                    int mid = (left + right) / 2;
                    if((*it2)->indexArea->indexDataList[mid].key == key)
                    {
                        // 找到了，直接读取文件
//                        // for debug
//                        std::cout << "->we find the key in the cache!" << key << std::endl;
                        // 事实证明确实都是0，就很惊悚
                        std::string fileRoutine = (*it2)->fileRoutine;
                        SSTable * theSSTable = new SSTable;
                        theSSTable->convertFileToSSTable(fileRoutine);
                        theSSTable->findInSSTable(answer, key);
                        delete theSSTable;
                        return true;
                    }
                    else if((*it2)->indexArea->indexDataList[mid].key < key)
                    {
                        left = mid + 1;
                    }
                    else
                    {
                        right = mid - 1;
                    }
                }
                // 这里如果没有找到并不能直接返回，需要继续搜索完整个磁盘为止。
            }
        }
    }
    return false;
}



/**
 * @brief KVStore::findInDisk3 使用缓存在内存中的index和bloom filter，先过滤，后二分，再读文件
 * @param answer 返回的value
 * @param key 要查找的key
 * @return 是否找到
 **/
bool KVStore::findInDisk3(std::string & answer, uint64_t key)
{
    // 遍历缓存，对于每一个缓存，用bloom filter判断是否存在，如果存在，再读文件查找，如果找到了，返回true
    // 如果没找到，则继续搜索，直到搜索完毕所有缓存为止
    for(auto it1 = theCache.begin(); it1 != theCache.end(); it1++)
    {
        for(auto it2 = it1->begin(); it2 != it1->end(); it2++)
        {
            // 直接用bloom filter判断是否存在
            if((*it2)->bloomFilter->searchInFilter(key))
            {
                // 若存在，先二分查找
                int left = 0;
                int right = (*it2)->indexArea->indexDataList.size() - 1;

                while(left <= right)
                {
                    int mid = (left + right) / 2;
                    if((*it2)->indexArea->indexDataList[mid].key == key)
                    {
                        // 找到了，直接读取文件
                        std::string fileRoutine = (*it2)->fileRoutine;
                        SSTable * theSSTable = new SSTable;
                        theSSTable->convertFileToSSTable(fileRoutine);
                        theSSTable->findInSSTable(answer, key);
                        delete theSSTable;
                        return true;
                    }
                    else if((*it2)->indexArea->indexDataList[mid].key < key)
                    {
                        left = mid + 1;
                    }
                    else
                    {
                        right = mid - 1;
                    }
                }


//                std::string fileRoutine = (*it2)->fileRoutine;
//                SSTable * theSSTable = new SSTable;
//                theSSTable->convertFileToSSTable(fileRoutine);
//                if(theSSTable->findInSSTable(answer, key))
//                {
//                    delete theSSTable;
//                    return true;
//                }
//                delete theSSTable;
            }
        }
    }

    return false;
}


/**
 * @brief 遍历缓存中的每一层，判断每一层的文件数是否超过阈值，如果超过，就进行归并
 **/
void KVStore::checkCompaction()
{
    int maxFileNum = LEVEL_CHANGE;


    int height = this->theCache.size();

    for(int i = 0; i < height; i++)    // // for debug 直接把第0层开到无穷大
//    int maxFileNum = INT_MAX;
    {
        if(this->theCache[i].empty()) // 未初始化的vector，直接调用size()函数会得到垃圾值
        {
            break;
        }
        if(this->theCache[i].size() > maxFileNum)
        {
            // 归并当前层
            compactSingleLevel(i);
        }
        else
        {
            break;
        }
        maxFileNum *= LEVEL_CHANGE;
    }
}

/**
 * @brief KVStore::compactSingleLevel 归并单层
 * */
void KVStore::compactSingleLevel(int levelNum)
{
    std::vector<SSTable *> tablesToMerge;
    uint64_t minKeyInAll = UINT64_MAX, maxKeyInAll = 0;

    if(levelNum == 0)
    {
        for(auto it = this->theCache[0].begin(); it != this->theCache[0].end(); it++)
        {
            std::string fileRoutine = (*it)->fileRoutine;
            SSTable * theSSTable = new SSTable;
            int returnValue = theSSTable->convertFileToSSTable(fileRoutine);
            if(returnValue == 1)
            {
                this->WriteAllCacheInfo(1);
            }
            if(theSSTable->header->minKey < minKeyInAll)
            {
                minKeyInAll = theSSTable->header->minKey;
            }
            if(theSSTable->header->maxKey > maxKeyInAll)
            {
                maxKeyInAll = theSSTable->header->maxKey;
            }
            tablesToMerge.push_back(theSSTable);
            // 删除文件
            utils::rmfile(fileRoutine.c_str());
        }
        // 清除缓存：第0层的所有文件
        this->theCache[0].erase(this->theCache[0].begin(), this->theCache[0].end());
    }
    else
    {
        // 选择该层时间戳最小的若干文件
        int maxFileNumber = 2;
        for(int i = 0; i < levelNum; i++)
            maxFileNumber *= 2;
        int fileNum = this->theCache[levelNum].size() - maxFileNumber;

        // 其实这里不一定有必要sort，但是还是sort一下；如果每次我们修改缓存都是按照时间戳排列的话，这里就没有必要sort了
        // 把时间戳从大到小排，选择最后的fileNum个文件
        std::sort(this->theCache[levelNum].begin(), this->theCache[levelNum].end(), SSTableCache::CompareSSTableCache);

        int levelSize = this->theCache[levelNum].size();

        for(int i = levelSize - fileNum; i < levelSize; i++)
        {
            std::string fileRoutine = this->theCache[levelNum][i]->fileRoutine;
            SSTable * theSSTable = new SSTable;
            int returnValue = theSSTable->convertFileToSSTable(fileRoutine);
            if(returnValue == 1)
            {
                this->WriteAllCacheInfo(2);
            }
            if(theSSTable->header->minKey < minKeyInAll)
            {
                minKeyInAll = theSSTable->header->minKey;
            }
            if(theSSTable->header->maxKey > maxKeyInAll)
            {
                maxKeyInAll = theSSTable->header->maxKey;
            }
            tablesToMerge.push_back(theSSTable);
            // 删除磁盘中的文件
            utils::rmfile(fileRoutine.c_str());
        }
        // 删除缓存中的文件
        this->theCache[levelNum].erase(this->theCache[levelNum].begin() + levelSize - fileNum, this->theCache[levelNum].end());
    }

    levelNum += 1;

    if(levelNum < theCache.size())
    {
        // 在下一层中选择所有数据范围和min与max之间有交集的文件
        for(auto it = this->theCache[levelNum].begin(); it != this->theCache[levelNum].end();)
        {
            // 其实这里检查数据范围的交集在缓存就能做，如果有交集再读文件，时间上可以好很多
            if((*it)->header->minKey <= maxKeyInAll && (*it)->header->maxKey >= minKeyInAll)
            {
                std::string fileRoutine = (*it)->fileRoutine;
                SSTable * theSSTable = new SSTable;
                int return_value = theSSTable->convertFileToSSTable(fileRoutine);
                if(return_value == 1)
                {
//                    // 打印当前所有缓存的值
//                    this->WriteAllCacheInfo(3);
                }
                tablesToMerge.push_back(theSSTable);
                // 把cache里的东西删了
                this->theCache[levelNum].erase(it);
                // 删除文件
                utils::rmfile(fileRoutine.c_str());
            }
            else
            {
                it++;
            }
        }
    }
    else
    {
        // 新增一层
        utils::mkdir((this->dataStoreDir + "/level-" + std::to_string(levelNum)).c_str());
        std::vector<SSTableCache *> newFloor;
        newFloor.resize(0);
        this->theCache.push_back(newFloor);
    }

    // 应该没有问题
    // 将这些SSTable文件merge，扔到下一层
    SSTable::mergeTables(tablesToMerge);

    // 去下一层的缓存里，根据现有的timestamp去查找有没有时间戳重复的文件，如果有，则统计个数
    int counter = 0;
    for(auto it = this->theCache[levelNum].begin(); it != this->theCache[levelNum].end(); it++)
    {
//        if((*it)->header->timeStamp == tablesToMerge[0]->header->timeStamp)
//            counter ++;
        // 似乎应该获得最大的那个timeStampIndex，否则似乎还是会导致覆盖的问题
        if((*it)->timeStampIndex > counter)
            counter = (*it)->timeStampIndex;
    }
    counter ++;

    // 切割获得的新SSTable，将里面的东西保存到磁盘中，同时返回新生成的缓存的std::vector
    std::vector<SSTableCache*> newCache = tablesToMerge[0]->splitAndSave(this->dataStoreDir + "/level-" + std::to_string(levelNum),counter);
    // 把获得的新SSTableCache加入到缓存中，同时将该层的缓存按照时间戳从大到小重新排序
    for(auto it = newCache.begin(); it != newCache.end(); it++)
    {
        this->theCache[levelNum].push_back(*it);
    }

    std::sort(this->theCache[levelNum].begin(), this->theCache[levelNum].end(), SSTableCache::CompareSSTableCache);
}

/**
 * @brief 将所有缓存的信息写入文件(for debug)
 * @param whereEnter 从哪里进入的这个函数（就是为了看哪里出错了，没有别的含义）
 * */
void KVStore::WriteAllCacheInfo(int whereEnter) {
    // 将当前所有缓存的信息写入文件
    std::ofstream out;
    out.open("holyShit.txt", std::ios::out);

    out << "where enter:" << whereEnter << std::endl;

    for(int i = 0; i < this->theCache.size(); i++)
    {
        out << "level " << i << " has" << theCache[i].size() << " files" << std::endl;
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            out << this->theCache[i][j]->fileRoutine << "    time stamp:" << this->theCache[i][j]->header->timeStamp << " min:" << this->theCache[i][j]->header->minKey << " max:" << this->theCache[i][j]->header->maxKey << std::endl;
        }
    }

    out.close();
}

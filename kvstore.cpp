#include "kvstore.h"
#include <string>
#include <iostream>
#include <fstream>
#include "utils.h"
#include <algorithm>

// CorrectnessTest test("./data", verbose);
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    // 实现从路径中恢复磁盘文件的部分
    // 所有SSTable要放在dir下面
    std::string currentDir = dir;

    if(currentDir[currentDir.size()-1] == '/')
    {
        currentDir = currentDir.substr(0, currentDir.size()-1);
    }

    // check dir是否存在
    if (utils::dirExists(currentDir))
    {
        this->dataStoreDir = currentDir;
        // 如果存在，那就读文件，生成缓存，更新class的时间戳！
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
                // 读文件，生成缓存，更新时间戳
                // 读文件的时候，memTable是空白的对吧？
                std::vector<std::string> fileNames;
                std::string routine = currentDir + "/level-" + std::to_string(i)+"/";
                int fileNumbers = utils::scanDir(routine, fileNames);
                for(int j = 0; j < fileNumbers; j++)
                {
                    SSTableCache * newCache = new SSTableCache;
                    newCache->readFileToFormCache(routine+fileNames[j]);
                    this->theCache[i].push_back(newCache);
                    // 更新时间戳
                    if(newCache->header->timeStamp > this->currentTimestamp)
                    {
                        this->currentTimestamp = newCache->header->timeStamp;
                    }
                }
                // TODO 把该层的cache文件按照时间戳排序
                // 因为第0层的时候，我们查找文件，要从时间戳大的找起
            }
        }
    }
    else
    {
        utils::mkdir(currentDir.c_str());
        utils::mkdir((currentDir + "/level-0").c_str());
        this->dataStoreDir = currentDir;
        // 先实现内存的部分
        this->memtable0 = new MemTable();
    }
}

KVStore::~KVStore()
{
    // 将memtable写入磁盘
    if(memtable0->getSize() != 0)
    {
        this->convertMemTableIntoMemory();
    }
    // 进行一次文件的检查和归并
    this->checkCompaction();
    delete memtable0;
    for(int i = 0; i < this->theCache.size();i++)
    {
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            delete this->theCache[i][j];
        }
    }
    // clean vector
    for(int i = 0; i < this->theCache.size();i++)
    {
        this->theCache[i].clear();
    }
    this->theCache.clear();

    this->levelDir.clear();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    // 判断大小
    if (memtable0->getSize() + s.size() >= MAX_MEMTABLE_SIZE)
    {
        // 将跳表写入SSTable；
        this->convertMemTableIntoMemory();
        this->checkCompaction();

        // 清空跳表，将s写入跳表
        this->memtable0->clearMemTable();
        this->memtable0->put(key, s);
        return;
    }
    // 没有超过大小，直接在跳表里面插入
    memtable0->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    // 只实现内存的部分

    std::string answer = memtable0->get(key);
    if (answer != "")
    {
        // 说明在sstable中找到了
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
    return memtable0->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    // 清除内存中的memtable
    if (memtable0 != nullptr)
    {
        delete memtable0;
        memtable0 = nullptr;
    }
    memtable0 = new MemTable();

    // 清除内存中的缓存
    for(int i = 0; i < this->theCache.size();i++)
    {
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            delete this->theCache[i][j];
        }
    }
    // 清除磁盘中的文件
    for(int i = 0; i < this->theCache.size(); i++)
    {
        for(int j = 0; j < this->theCache[i].size(); j++)
        {
            std::string routine = theCache[i][j]->fileRoutine;
            utils::rmfile(routine.c_str());
        }
    }
    // clean vector
    for(int i = 0; i < this->theCache.size();i++)
    {
        this->theCache[i].clear();
    }
    this->theCache.clear();

    // 删除文件夹
    for(int i = 0; i < this->levelDir.size(); i++)
    {
        // 由于levelDir中，保存的实际上是儿子文件夹的名称，所以要加上父亲文件夹的名称才构成路径
        std::string routine = this->dataStoreDir + "/" + this->levelDir[i];
        utils::rmdir(routine.c_str());
    }
    utils::rmdir(this->dataStoreDir.c_str());
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
    // TODO 将memtable中的内容写入磁盘
    std::vector<std::pair<uint64_t, std::string>> allKVPairs = this->memtable0->getAllKVPairs();

    // 把东西写入缓存
    SSTableCache *newCache;
    newCache = new SSTableCache;

    // 把东西写成文件
    const std::string dir = this->dataStoreDir + "/level-0";
    char *buffer = new char[MAX_MEMTABLE_SIZE];

    // Header, BloomFilter, IndexArea, DataArea
    *(uint64_t *)buffer = this->currentTimestamp;
    *(uint64_t *)(buffer + 8) = allKVPairs.size();
    *(uint64_t *)(buffer + 16) = memtable0->getMinKey();
    *(uint64_t *)(buffer + 24) = memtable0->getMaxKey();

    // BloomFilter
    BloomFilter *theFilter = new BloomFilter;

    for (int i = 0; i < allKVPairs.size(); i++)
    {
        theFilter->addIntoFilter(allKVPairs[i].first);
    }

    // BloomFilter写入buffer
    for (int i = 0; i < 10240; i++)
    {
        if (theFilter->checkBits[i] == true)
        {
            buffer[i + 32] = '1';
        }
        else
        {
            buffer[i + 32] = '0';
        }
    }

    // IndexArea
    // 我突然顿悟了，在这里存储数据的时候我可以直接用vector存，但是从文件里面读数据的时候我需要用偏移量！
    char *indexStart = buffer + 10240 + 32;
    int padding = 0; // 指针走过的量

    // 索引区到数据区的偏移量
    int offset = 10240 + 32 + (8 + 4) * allKVPairs.size();

    for (int i = 0; i < allKVPairs.size(); i++)
    {
        // 写入索引区
        *(uint64_t *)(indexStart + padding) = allKVPairs[i].first;
        *(uint32_t *)(indexStart + padding + 8) = offset;

        // TAG 写入缓存
        struct IndexData newIndexData(allKVPairs[i].first, offset);
        newCache->indexArea->indexDataList.push_back(newIndexData);

        // 把string写入数据区
        memcpy(buffer + offset, allKVPairs[i].second.c_str(), allKVPairs[i].second.size());

        padding += 12;
        offset += allKVPairs[i].second.size();
    }

    // 文件名取名为当时的时间
    std::string fileName = dir + "/" + std::to_string(this->currentTimestamp) + ".sst";

    // 把buffer写入文件
    std::ofstream fout(fileName, std::ios::out | std::ios::binary);
    fout.write(buffer, MAX_MEMTABLE_SIZE);
    fout.close();

    delete[] buffer;

    newCache->setAllData(memtable0->getMinKey(), memtable0->getMaxKey(), allKVPairs.size(), this->currentTimestamp, fileName, this->currentTimestamp);

    newCache->bloomFilter = theFilter;

    // TAG 其实cache的传入可以在这里，我传入一个cache的指针，然后修改里面的内容，最后把这个指针append到vector里面去！
    // 直接加入第0层的cache，后面调整是后面的事情
    this->theCache[0].push_back(newCache);

    this->currentTimestamp += 1;
    // 把第0层的cache排序，按照时间戳从大到小排，这很重要！
    std::sort(theCache[0].begin(),theCache[0].end(), [](const SSTableCache * a, SSTableCache *b){
        return a->timeStamp > b->timeStamp;
    });
}

void SSTableCache::setAllData(uint64_t minKey, uint64_t maxKey, uint64_t numberOfPairs, uint64_t timeStamp, std::string fileName, uint64_t currentTime)
{
    this->header = new Header(timeStamp, numberOfPairs, minKey, maxKey);
    this->fileRoutine = fileName;
    bloomFilter = new BloomFilter;
    indexArea = new IndexArea;
    this->timeStamp = currentTime;
}

// 不缓存，直接在文件里查找
bool KVStore::findInDisk1(std::string & answer, uint64_t key)
{
    // 现在只有第0层，查找的时候就遍历文件
    if(this->theCache.empty())
    {
        return false;
    }
    for(auto it = this->theCache[0].begin(); it != this->theCache[0].end(); it++)
    {
        std::string fileRoutine = (*it)->fileRoutine;
        // TODO 把文件变成SSTable，然后去判断里面有没有
        // SSTable *theSSTable = convertFileToSSTable(fileRoutine);
        SSTable *theSSTable = new SSTable;
        theSSTable->convertFileToSSTabe(fileRoutine);
        if(theSSTable->findInSSTable(answer, key))
        {
            delete theSSTable;
            return true;
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

// SSTable * KVStore::convertFileToSSTable(std::string routine)
// {
// 	SSTable * theSSTable = new SSTable;

// 	return nullptr;
// }

void KVStore::checkCompaction()
{

}

void KVStore::compactSingleLevel(int levelNum)
{
    return;
}

SSTableCache::~SSTableCache()
{
    delete header;
    delete bloomFilter;
    delete indexArea;
}

SSTableCache::SSTableCache()
{
    header = nullptr;
    bloomFilter = nullptr;
    indexArea = nullptr;
}

void SSTableCache::readFileToFormCache(std::string fileName)
{
}
# 设计

## 缓存的设计

缓存的初始化
vector<vector>

（1）第0层 缓存初始化在KVStore的构造函数中；其他的时候，只要满了，就往下加层数；
（2）同时，为了避免reset之后，我使用第0层缓存时theCache[0][0] == null
    我在reset之后必须要把第0层缓存初始化一下，这样就不会出现theCache[0][0] == null的情况了

## KVStore的构造函数里面

默认生成./data
./data/level-0

如果读文件的时候，存在更多的层，那么就生成更多的层

（1）如果读文件的时候，是自动生成缓存的；
（2）所以如果没有读文件，也就没有缓存，这个时候就挂了

所以每次我往第0层搜索的时候，如果恰好整个表是空的，只有memtable里面有数据，就会搜一个空的vector元素

```cpp
bool KVStore::findInDisk1(std::string & answer, uint64_t key)
{
    // 现在只有第0层，查找的时候就遍历文件
    if(this->theCache[0].empty())
    {
        return false;
    }
```

-------------------------------------------------------------------

之前已经完成除了compact之外的操作。
现在完整捋一遍compact的过程，然后开始写compact。

相关的函数：
```c++
/**
 * 遍历整个data目录，检测每一层是否需要归并
 */
void checkCompaction()
{
    // 遍历，检查每一层是否需要归并
    for(int i = 0; i < max_level; i++)
    {
        if(cache.element > XXX)
        {
            compactSingleLevel(i);
        }
        else
        {
            break;
        }
    }
}

/**
 * 归并某一层
 * （1）问题：时间戳相等的文件
 * （2）如何调整cache的信息
 * （3）层数到达最大层，往下添一层
 */
void compactSingleLevel(int levelIndex)
{
    // level0: tiering
    /*
     * （1）选择所有文件
     * （2）统计最大key和最小key，对于下一层：选择所有重复文件
     * 
     * （1）将选中的sstable读取到内存，归并，重新进行划分*/
    if(levelIndex == 0)
    {
        // 选择第一层的所有文件
        // 根据缓存中的内容，读文件
        int number = number_in_level0;
        std::vector<SSTable * > tableList;
        for(int i = 0; i < number; i++)
        {
            SSTable * newTable = new SSTable;
            tableList.push_back(newTable);
            // 初始化SSTable
            newTable->...
            ...
            // 获得最大最小元素
            ...
        }
        
    }
    
    // other: leveling
    /*
     * （1）选择时间戳最小的若干文件
     * （2）统计最大key和最小key，对于下一层：选择所有重复文件
     * 
     * （1）将选中的sstable读取到内存，归并，重新进行划分*/
    
}
```

问题：
（1）需要全部修改：可能存在时间戳重名的文件
./data/level-0/100-1.sst
在后面加上后缀


-----------------------------------------------------------
compact要做的事情
1. 写函数：
   1. 读文件，形成缓存  ok
   2. for loop检查每一层是否需要递归 ok
   3. 归并某个特定的层
   4. 修改：生成文件的文件名
      1. 从磁盘中读取文件的时候 【这个其实好像没有必要了，文件名都是存在缓存里面了】
      2. convertMemTableToDiskFile()    ok
      3. convertMemTableToDiskFileWithoutCache() ok
   5. 修改：findInDisk，需要遍历所有文件夹 ok


```c++
void compactSingleLevel()
{

}


```

（2）修改文件名：出现重名的情况

对所有文件重新命名：
初始状态就是XXX-0.sst

--------------------------------
TODO
1. 把生成的巨大的sstable进行切割
   SSTable里面写一个函数
```c++
static cut
```
2. 切割完了，写回硬盘
3. 同时，也需要写回缓存（在这之前，需要清除以前的缓存）
清除缓存：erase
4. 还要删除原本磁盘中的SSTable文件！！！（想一想这个应该在哪里实现）
-----------------------------------
到了debug的阶段
现在应该就是1->2的compaction出现了问题
问题应该是出现在compactSingleLevel上，如果我感觉没错。

----------------------------------
应该还是缓存出了问题。
去查一下缓存读出来的文件。
header，index，bloom都能不能读？现在看来好像都有问题。

-------------------------------------
现在核心的问题就在于，cache的布隆过滤器是什么时候生成的？？？？？？


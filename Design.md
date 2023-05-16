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
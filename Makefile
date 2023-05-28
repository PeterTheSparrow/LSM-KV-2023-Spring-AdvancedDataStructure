LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

MemTable.o: Memtable.cpp Memtable.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

SSTable.o: SSTable.cpp SSTable.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

SkipList.o: SkipList.cpp SkipList.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

correctness: kvstore.o correctness.o SkipList.o MemTable.o SSTable.o
	$(LINK.o) $^ -o $@

persistence: kvstore.o persistence.o SkipList.o MemTable.o	SSTable.o

MyOwnTest.o: MyOwnTest.cpp SkipList.h SkipList.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 自己写的测试程序
mytest: MyOwnTest.o SkipList.o
	$(LINK.o) $^ -o $@	

commonTest.o: commonTest.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 测试
test: commonTest.o SkipList.o SSTable.o MemTable.o kvstore.o
	$(LINK.o) $^ -o $@

clean:
#-rm -f correctness persistence *.o
# linux下使用-rm，windows下使用-del
	del correctness persistence *.o

valgrind_correctness: correctness
	valgrind --leak-check=full ./correctness



LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o SkipList.o MemTable.o SSTable.o
	$(LINK.o) $^ -o $@

persistence: kvstore.o persistence.o SkipList.o MemTable.o	SSTable.o

MemTable.o: MemTable.cpp MemTable.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

SSTable.o: SSTable.cpp SSTable.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

SkipList.o: SkipList.cpp SkipList.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

MyOwnTest.o: MyOwnTest.cpp SkipList.h SkipList.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 自己写的测试程序
mytest: MyOwnTest.o SkipList.o
	$(LINK.o) $^ -o $@	

clean:
# linux下使用-rm，windows下使用-del
# -rm -f correctness persistence *.o
	del correctness persistence *.o


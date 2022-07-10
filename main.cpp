#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <unordered_map>
#include <map>

using namespace std;

class User {
  public:
    char s[280];
};

void print_resident_set_size(int64_t i) {
      double resident_set = 0.0;
      std::ifstream stat_stream("/proc/self/stat",std::ios_base::in);
      std::string pid, comm, state, ppid, pgrp, session, tty_nr;
      std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
      std::string utime, stime, cutime, cstime, priority, nice;
      std::string O, itrealvalue, starttime;
      unsigned long vsize;
      long rss;
      stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
              >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
              >> utime >> stime >> cutime >> cstime >> priority >> nice
              >> O >> itrealvalue >> starttime >> vsize >> rss;
      stat_stream.close();
      long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;
      resident_set = (double)rss * (double)page_size_kb / (1024 * 1024);
      cout << i << ", plate current process memory size: " << resident_set << "g" << endl;
}

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
uint32_t StrHash(const char* s, int size) {
  uint32_t h = kFinish;
  while (size > 0) {
    h = (h * kA) ^ (s[0] * kB);
    s++;
    size--;
  }
  return h;
}

class Myclass {
public:
  char s[128];
  Myclass(char *t) {
    for (int i = 0; i < 128; i++) s[i] = t[i];
  }
  // 重载等号，判断两个Myclass类型的变量是否相等
  bool operator== (const Myclass &other) const
  {
    for (int i = 0; i < 128; i++) {
      if (s[i] != other.s[i]) {
        return false;
      }
    }
    return true;
  }
};

// 实现Myclass类的hash函数
namespace std
{
    template <>
    struct hash<Myclass> {
        size_t operator()(const Myclass &k) const{
          return StrHash(k.s, 128);
        }
    };
}

// 1.4633g    1.3G
// 1.01641g   0.633G
// 0.1967g    0.0745
// 
int main() {
  print_resident_set_size(0);
  // for (int64_t i = 0; i < 5000000; i++) {
  //   // string *s = new string(280, '-');
  //   char *s = new char[280];
  // }
  // unordered_map<std::string, int64_t> mp;
  // char buf[300] = {0};
  // for (int64_t i = 0; i < 5000000; i++) {
  //   sprintf(buf, "%d", (int)i);
  //   if (i % 100000 == 0) {
  //     print_resident_set_size(i);
  //   }
  //   mp.insert({std::string(buf, 128), i});
  // }
  unordered_map<Myclass, int64_t> mp;
  char buf[300] = {0};
  for (int64_t i = 0; i < 5000000; i++) {
    sprintf(buf, "%d", (int)i);
    if (i % 100000 == 0) {
      print_resident_set_size(i);
    }
    mp.insert({Myclass(buf), i});
  }
  // unordered_map<int64_t, User> mp;
  // for (int64_t i = 0; i < 5000000; i++) {
  //   if (i % 100000 == 0) {
  //     print_resident_set_size(i);
  //   }
  //   mp.insert({i, User()});
  // }
  print_resident_set_size(5000000);
}
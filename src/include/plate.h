#include <string>
#include <vector>

const int RECORDSIZE = 8 + 128 + 128 + 8;
const int RECORDNUM = 1000000 * 50; // about 13G think about 32bit overflow
const int MINIRECORDNUM = 100000 * 50; // 1.3G
const int MAPSIZE = RECORDSIZE * MINIRECORDNUM;

class Location {
  public:
    Location(): file_id_(-1), offset_(-1) {}
    int file_id_;
    int offset_;
};

class MMapFile {
  public:
    MMapFile(): fd_(-1), start_(nullptr) {}
    MMapFile(int fd, char *start): fd_(fd), start_(start) {}
    int  fd_;
    char *start_;
};

class Item {
  public:
    Item() : in_use_(0) {
    }
    uint64_t in_use_;
    char datas_[RECORDSIZE];
};

class Plate {
  public:
    explicit Plate(const std::string &path);
    ~Plate();
    
    int Init();
    
    int append(const void *datas, Location &location);

    int get(const Location &location, void * const datas);

    int scan(void (*cb)(void *, void *),  void *context);
  
    int size();
  private:
    // sort and record all data (at least one) file by fiile name
    int gen_sorted_paths(const std::string &dir, std::vector<std::string> &paths);
    // current spapce is run out, create a new file
    int create_new_mmapFile();
    // update curr_
    void replay();

  private:

    const std::string     dir_;      // data dictory
    std::vector<MMapFile> files_;    // mmap fd
    MMapFile              *currFile_; // currFile_ = files_[files_.size() - 1]
    Item                  *curr_;    // next append offset address
};

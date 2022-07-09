#include <string>
#include <vector>
#include <fstream>

const int RECORDSIZE = 8 + 128 + 128 + 8;
const int RECORDNUM = 1000000 * 50; // about 13G think about 32bit overflow
const int MINIRECORDNUM = 100000 * 50; // 1.3G
const int MAPSIZE = (RECORDSIZE + 8) * MINIRECORDNUM; // uint64_t Item: int_use_

class Location {
  public:
    Location(): file_id_(-1), offset_(-1) {}
    Location(int file_id, int offset): file_id_(file_id), offset_(offset) {}
    int file_id_;
    int offset_;
};

class MMapFile {
  public:
    MMapFile(): path_(), fd_(-1), start_(nullptr) {}
    MMapFile(std::string path, int fd, char *start): path_(path), fd_(fd), start_(start) {}
    bool is_open() { return fd_ != -1; }
    std::string path_;
    int  fd_;
    char *start_; // if start_ = nullptr, this file is close and unmmap
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

    int scan(void (*cb)(void *user, void *location, void *context), void *context);
  
    int size();
  private:
    // sort and record all data (at least one) file by fiile name
    int gen_sorted_paths(const std::string &dir, std::vector<std::string> &paths);
    // current spapce is run out, create a new file
    int create_new_mmapFile();
    // update curr_ (only open the last mmap file)
    void replay();
    //open or close mmap a file through MMapFile object
    int openMMapFileIfNotOpen(MMapFile &file);
    int closeMMapFile(MMapFile &file);

  private:

    const std::string     dir_;      // data dictory
    std::vector<MMapFile> files_;    // mmap fd
    MMapFile              *currFile_; // currFile_ = files_[files_.size() - 1]
    Item                  *curr_;    // next append offset address
};

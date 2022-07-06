#include <string>

const int RECORDSIZE = 8 + 128 + 128 + 8;
const int RECORDNUM = 1000000 * 50;
const int MINIRECORDNUM = 100 * 50;
const int MAPSIZE = RECORDSIZE * MINIRECORDNUM;

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
    
    int append(const void *datas);

    int scan(void (*cb)(void *, void *),  void *context);
  
    int size();
  private:
    // update curr_
    void replay();

  private:
    const std::string dir_;    // data dictory
    int               fd_;      // mmap fd
    char              *start_;  // mmap start address
    Item              *curr_;   // next append offset address
};

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "spdlog/spdlog.h"
#include "plate.h"

static const char kDataFileName[] = "DATA";

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

Plate::Plate(const std::string &path) 
  : dir_(path),
  fd_(-1),
  start_(0),
  curr_(0) {}

Plate::~Plate() {
  spdlog::info("plate start deconstruct");
  if (fd_ > 0) {
    munmap(start_, MAPSIZE);
    close(fd_);
  }
}

int Plate::Init() {
  spdlog::info("plate start init");
  bool new_create = false;

  if (!FileExists(dir_) && 0 != mkdir(dir_.c_str(), 0755)) {
    return -1;
  }
  std::string path = dir_ + "/" + kDataFileName;

  // open data file  
  int fd = open(path.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      new_create = true;
      if (posix_fallocate(fd, 0, MAPSIZE) != 0) {
        spdlog::error("posix_fallocate failed");
        close(fd);
        return -1;
      }
    }
  }
  if (fd < 0) {
    return -1;
  }
  fd_ = fd;

  // mmap data file
  void* ptr = mmap(NULL, MAPSIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("MAP_FAILED");
    close(fd);
    return -1;
  }
  
  start_ = reinterpret_cast<char *>(ptr);
  if (new_create) {
    memset(ptr, 0, MAPSIZE);
    curr_ = reinterpret_cast<Item *>(start_);
  } else {
    replay();
  }
  
  return 0;
}


int Plate::append(const void *datas) {
  if (reinterpret_cast<char *>(curr_ + 1)> start_ + MAPSIZE) {
    spdlog::error("plate haven't space");
    return -1;
  }
  memcpy(curr_->datas_, datas, RECORDSIZE); // write data before write in_use flag
  reinterpret_cast<Item *>(curr_)->in_use_ = 1;
  curr_++;
  return 0;
}

int Plate::scan(void (*cb)(void *, void *),  void *context) {
  Item *cursor = reinterpret_cast<Item *>(start_);
  while (reinterpret_cast<char *>(cursor) < start_ + MAPSIZE) {
    Item *internal_record = reinterpret_cast<Item *>(cursor);
    if (internal_record->in_use_ == 0) {
      break;
    }
    cb(reinterpret_cast<void *>(cursor->datas_), context);
    cursor++;
  }
  return 0;
}

void Plate::replay() {
  spdlog::info("plate start replay");
  Item *cursor = reinterpret_cast<Item *>(start_);
  while (reinterpret_cast<char *>(cursor) < start_ + MAPSIZE) {
    Item *internal_record = reinterpret_cast<Item *>(cursor);
    if (internal_record->in_use_ == 0) {
      break;
    }
    cursor++;
  }
  curr_ = cursor;
}

int Plate::size() {
  return curr_ - reinterpret_cast<Item *>(start_);
}
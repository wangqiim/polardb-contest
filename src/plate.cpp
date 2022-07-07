#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "spdlog/spdlog.h"
#include "plate.h"

static const char kDataFileName[] = "DATA";

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

std::string DataFileName(const std::string &dir, int number) {
  std::string dataFileName = dir + "/" + kDataFileName;
  dataFileName += "_";
  char buf[100] = {0};
  std::snprintf(buf, sizeof(buf), "%08d", number);
  return dataFileName + buf;
}

std::string DataFileName(const std::string &dir, const std::string &file_name) {
  return dir + "/" + file_name;
}

Plate::Plate(const std::string &path) 
  : dir_(path),
  files_(),
  currFile_(nullptr),
  curr_(nullptr) {}

Plate::~Plate() {
  spdlog::info("plate start deconstruct");
  for (const auto &mmapFile: files_) {
    if (mmapFile.fd_ > 0) {
      munmap(mmapFile.start_, MAPSIZE);
      close(mmapFile.fd_);
    }
  }
}

int Plate::Init() {
  spdlog::info("plate start init");
  if (!FileExists(dir_) && 0 != mkdir(dir_.c_str(), 0755)) {
    return -1;
  }
  std::vector<std::string> paths;
  gen_sorted_paths(dir_, paths);
  if (paths.size() == 0) {
    spdlog::error("paths.size() must > 0");
    return -1;
  }

  for (int i = 0; i < paths.size(); i++) {
    bool new_create = false;
    // open data file  
    int fd = open(paths[i].c_str(), O_RDWR, 0644);
    if (fd < 0 && errno == ENOENT) {
      fd = open(paths[i].c_str(), O_RDWR | O_CREAT, 0644);
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
    // mmap data file
    void* ptr = mmap(NULL, MAPSIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
      spdlog::error("MAP_FAILED");
      close(fd);
      return -1;
    }
    MMapFile file(fd, reinterpret_cast<char *>(ptr));
    files_.push_back(file);
    if (new_create) {
      memset(ptr, 0, MAPSIZE);
      curr_ = reinterpret_cast<Item *>(ptr);
    }
  }
  replay();
  return 0;
}

int Plate::append(const void *datas, Location &location) {
  if (reinterpret_cast<char *>(curr_ + 1)> currFile_->start_ + MAPSIZE) {
    spdlog::info("plate currFile_ haven't space, create and mmap a new file");
    create_new_mmapFile();
  }
  location.file_id_ = files_.size() - 1;
  location.offset_ = ((char *)curr_ - currFile_->start_) / sizeof(Item);
  memcpy(curr_->datas_, datas, RECORDSIZE); // write data before write in_use flag
  reinterpret_cast<Item *>(curr_)->in_use_ = 1;
  curr_++;
  return 0;
}

int Plate::get(const Location &location, void *datas) {
  // defend program
  if (location.file_id_ >= files_.size() || (location.offset_ + 1) * sizeof(Item) > MAPSIZE) {
    spdlog::error("invalid location");
    return -1;
  }
  Item *internal_record = reinterpret_cast<Item *>(
    files_[location.file_id_].start_ + sizeof(Item) * location.offset_);
  if (internal_record->in_use_ == 0) {
    spdlog::error("get a invalid record (not in use record)");
    return -1;
  }
  datas = reinterpret_cast<void *>(internal_record->datas_);
  return 0;
}

int Plate::scan(void (*cb)(void *, void *),  void *context) {
  for (const auto &file: files_) {
    Item *cursor = reinterpret_cast<Item *>(file.start_);
    while (reinterpret_cast<char *>(cursor) < file.start_ + MAPSIZE) {
      Item *internal_record = reinterpret_cast<Item *>(cursor);
      if (internal_record->in_use_ == 0) {
        break;
      }
      cb(reinterpret_cast<void *>(cursor->datas_), context);
      cursor++;
    }
  }
  return 0;
}

void Plate::replay() {
  spdlog::info("plate start replay");
  currFile_ = &files_.back();
  Item *cursor = reinterpret_cast<Item *>(currFile_->start_);
  while (reinterpret_cast<char *>(cursor) < currFile_->start_ + MAPSIZE) {
    Item *internal_record = reinterpret_cast<Item *>(cursor);
    if (internal_record->in_use_ == 0) {
      break;
    }
    cursor++;
  }
  curr_ = cursor;
}

int Plate::size() {
  if (files_.size() == 0) {
    return 0;
  }
  int prev_num = (files_.size() - 1) *(MAPSIZE / sizeof(Item));
  return prev_num + curr_ - reinterpret_cast<Item *>(currFile_->start_);
}

int Plate::gen_sorted_paths(const std::string &dir, std::vector<std::string> &paths) {
    struct dirent *dir_ptr = NULL;
    DIR *dir_fd = opendir(dir.c_str());
    if (dir_fd == nullptr) {
      spdlog::error("open dir {} fail", dir);
      return -1;
    }
    while ((dir_ptr = readdir(dir_fd)) != NULL) {
        if (dir_ptr->d_type == DT_REG) {
            spdlog::debug("generate sorted paths, scan file: {}", dir_ptr->d_name);
            std::string name(dir_ptr->d_name);
            if (name.find(kDataFileName) == 0) {
              paths.push_back(DataFileName(dir_, dir_ptr->d_name)); 
            }
        }
    }
    std::sort(paths.begin(), paths.end());
    if (paths.size() == 0) {
      paths.push_back(DataFileName(dir_, files_.size()));
    }
    return 0;
}

int Plate::create_new_mmapFile() {
  std::string new_path = DataFileName(dir_, files_.size());

  int fd = open(new_path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd >= 0) {
    if (posix_fallocate(fd, 0, MAPSIZE) != 0) {
      spdlog::error("posix_fallocate failed");
      close(fd);
      return -1;
    }
  }
  if (fd < 0) {
    spdlog::error("create data file failed");
    return -1;
  }
  // mmap data file
  void* ptr = mmap(NULL, MAPSIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("MAP_FAILED");
    close(fd);
    return -1;
  }
  MMapFile file(fd, reinterpret_cast<char *>(ptr));
  files_.push_back(file);
  memset(ptr, 0, MAPSIZE);
  currFile_ = &files_.back();
  curr_ = reinterpret_cast<Item *>(ptr);
  return 0;
}
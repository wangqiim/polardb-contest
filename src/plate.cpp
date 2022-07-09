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
  for (auto &mmapFile: files_) {
    if (mmapFile.is_open()) {
      closeMMapFile(mmapFile);
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
    MMapFile file(paths[i], -1, nullptr);
    files_.push_back(file);
  }
  replay(); // only open the last mmap file
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

int Plate::get(const Location &location, void * const datas) {
  // defend program
  if (location.file_id_ >= files_.size() || (location.offset_ + 1) * sizeof(Item) > MAPSIZE) {
    spdlog::error("invalid location");
    return -1;
  }
  openMMapFileIfNotOpen(files_[location.file_id_]);
  Item *internal_record = reinterpret_cast<Item *>(
    files_[location.file_id_].start_ + sizeof(Item) * location.offset_);
  if (internal_record->in_use_ == 0) {
    spdlog::error("get a invalid record (not in use record)");
    return -1;
  }
  memcpy((char *)datas, internal_record->datas_, RECORDSIZE);
  if (location.file_id_ + 1 != files_.size()) { // if it is not the last mmap file, close it
    closeMMapFile(files_[location.file_id_]);
  }
  return 0;
}

int Plate::scan(void (*cb)(void *user, void *location, void *context),  void *context) {
  for (int file_id = 0; file_id < files_.size(); file_id++) {
    if (file_id != 0) {
      closeMMapFile(files_[file_id - 1]);
    }
    openMMapFileIfNotOpen(files_[file_id]);
    int offset = 0;
    while ((offset + 1) * sizeof(Item) <= MAPSIZE) {
      char *data = files_[file_id].start_ + offset * sizeof(Item);
      Item *internal_record = reinterpret_cast<Item *>(data);
      if (internal_record->in_use_ == 0) {
        break;
      }
      Location location(file_id, offset);
      cb((void *)(internal_record->datas_), (void *)&location, context);
      offset++;
    }
  }
  return 0;
}

void Plate::replay() {
  currFile_ = &files_.back();
  openMMapFileIfNotOpen(*currFile_);
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
  if (currFile_ == nullptr || !currFile_->is_open()) {
    spdlog::error("currFile is not open!!!");
    return -1;
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
  if (files_.size() == 0) {
    spdlog::error("create_new_mmapFile when thie isn't old mmap file");
    return -1;
  }
  closeMMapFile(files_.back());

  std::string new_path = DataFileName(dir_, files_.size());
  MMapFile file(new_path, -1, nullptr);
  files_.push_back(file);
  currFile_ = &files_.back();
  openMMapFileIfNotOpen(*currFile_);
  curr_ = reinterpret_cast<Item *>(currFile_->start_);
  return 0;
}

int Plate::openMMapFileIfNotOpen(MMapFile &file) {
  if (file.is_open()) {
    return 0;
  }
  bool new_create = false;
  // open data file  
  int fd = open(file.path_.c_str(), O_RDWR, 0644);
  if (fd < 0 && errno == ENOENT) {
    fd = open(file.path_.c_str(), O_RDWR | O_CREAT, 0644);
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
  file.fd_ = fd;
  file.start_ = reinterpret_cast<char *>(ptr);
  if (new_create) {
    memset(ptr, 0, MAPSIZE);
  }
  return 0;
}

int Plate::closeMMapFile(MMapFile &file) {
  if (!file.is_open()) {
    spdlog::error("close a not open file");
    return -1;
  }
  munmap(file.start_, MAPSIZE);
  close(file.fd_);
  file.fd_ = -1;
  file.start_ = nullptr;
  return 0;
}

#include <functional>
#include <sys/mman.h>
#include <unistd.h>

#include "spdlog/spdlog.h"
#include "log.h"
#include "util.h"

//--------------------- mmap file-----------------------------------
MmapWriter::MmapWriter(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1)
    , data_start_(nullptr), data_curr_(nullptr) {
  // 1. open fd; (must have been create)
  fd_ = open(filename_.c_str(), O_RDWR, 0644);
  if (fd_ < 0) {
    spdlog::error("[MmapWriter] can't open file {}", filename_);
    exit(1);
  }
  int off = (int)lseek(fd_, 0, SEEK_END);
  if (off < 0) {
    spdlog::error("[MmapWriter] lseek end failed");
    exit(1);
  }
  if (off == 0) {
    if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
      spdlog::error("[MmapWriter] posix_fallocate failed");
      exit(1);
    }
  }
  // 2. mmap
  void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("[MmapWriter] mmap failed, errno is {}", strerror(errno));
    exit(1);
  }
  if (off == 0) {
    memset(ptr, 0, mmap_size_);
    msync(ptr, mmap_size_, MS_SYNC);
  }
  data_start_ = reinterpret_cast<char *>(ptr);
  data_curr_ = data_start_;
  while (*(uint64_t *)(data_curr_ + RecordSize) != 0) {
    data_curr_ += RecordSize;
  }
}

MmapWriter::~MmapWriter() {
  munmap(data_start_, mmap_size_);
  close(fd_);
}

MmapReader::MmapReader(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1)
    , cnt_(0), data_start_(nullptr), data_curr_(nullptr) {
  Util::CreateIfNotExists(filename_);
  // 1. open fd;
  fd_ = open(filename_.c_str(), O_RDWR, 0644);
  if (fd_ < 0) {
    spdlog::error("[MmapReader] can't open file {}", filename_);
    exit(1);
  }
  int off = (int)lseek(fd_, 0, SEEK_END);
  if (off < 0) {
    spdlog::error("[MmapReader] lseek end failed");
    exit(1);
  }
  if (off == 0) {
    if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
      spdlog::error("[MmapReader] posix_fallocate failed");
      exit(1);
    }
  }
  // 2. mmap
  void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("[MmapReader] mmap failed, errno is {}", strerror(errno));
    exit(1);
  }
  if (off == 0) {
    memset(ptr, 0, mmap_size_);
    msync(ptr, mmap_size_, MS_SYNC);
  }
  data_start_ = reinterpret_cast<char *>(ptr);
  data_curr_ = data_start_;
  while (*(uint64_t *)(data_curr_ + RecordSize) != 0) {
    cnt_++;
    data_curr_ += RecordSize;
  }
  data_curr_ = data_start_;
}

MmapReader::~MmapReader() {
  munmap(data_start_, mmap_size_);
  close(fd_);
}

bool MmapReader::ReadRecord(char *&record, int len) {
  if (data_curr_ + len > data_start_ + mmap_size_) {
    spdlog::error("[ReadRecord] read overflow mmap_size error");
    exit(1);
  }
  if (static_cast<uint64_t>(data_curr_ - data_start_) / RecordSize < cnt_) {
    record = data_curr_;
    data_curr_ += len;
    return true;
  }
  return false;
}

//--------------------- pmem file-----------------------------------
MmapBufferWriter::MmapBufferWriter(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1)
    , commit_cnt_(nullptr), data_start_(nullptr), data_curr_(nullptr) {
  // 1. open fd; (must have been create)
  fd_ = open(filename_.c_str(), O_RDWR, 0644);
  if (fd_ < 0) {
    spdlog::error("[MmapBufferWriter] can't open file {}", filename_);
    exit(1);
  }
  int off = (int)lseek(fd_, 0, SEEK_END);
  if (off < 0) {
    spdlog::error("[MmapBufferWriter] lseek end failed");
    exit(1);
  }
  if (off == 0) {
    if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
      spdlog::error("[MmapBufferWriter] posix_fallocate failed");
      exit(1);
    }
  }
  // 2. mmap
  void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("[MmapBufferWriter] mmap failed, errno is {}", strerror(errno));
    exit(1);
  }
  if (off == 0) {
    memset(ptr, 0, mmap_size_);
    msync(ptr, mmap_size_, MS_SYNC);
  }
  data_start_ = reinterpret_cast<char *>(ptr);
  commit_cnt_ = reinterpret_cast<uint64_t *>(data_start_ + mmap_size - 8);
  uint64_t record_num_per_round_buffer = (mmap_size_ - 8) / RecordSize;
  data_curr_ = data_start_ + (*commit_cnt_ % record_num_per_round_buffer) * RecordSize;
}

MmapBufferWriter::~MmapBufferWriter() {
  munmap(data_start_, mmap_size_);
  close(fd_);
}

MmapBufferReader::MmapBufferReader(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1)
    , commit_cnt_(nullptr), data_start_(nullptr), data_curr_(nullptr) {
  Util::CreateIfNotExists(filename_);
  // 1. open fd;
  fd_ = open(filename_.c_str(), O_RDWR, 0644);
  if (fd_ < 0) {
    spdlog::error("[MmapBufferReader] can't open file {}", filename_);
    exit(1);
  }
  int off = (int)lseek(fd_, 0, SEEK_END);
  if (off < 0) {
    spdlog::error("[MmapBufferReader] lseek end failed");
    exit(1);
  }
  if (off == 0) {
    if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
      spdlog::error("[MmapBufferReader] posix_fallocate failed");
      exit(1);
    }
  }
  // 2. mmap
  void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (ptr == MAP_FAILED) {
    spdlog::error("[MmapBufferReader] mmap failed, errno is {}", strerror(errno));
    exit(1);
  }
  if (off == 0) {
    memset(ptr, 0, mmap_size_);
    msync(ptr, mmap_size_, MS_SYNC);
  }
  data_start_ = reinterpret_cast<char *>(ptr);
  commit_cnt_ = reinterpret_cast<uint64_t *>(data_start_ + mmap_size - 8);
  data_curr_ = data_start_;
}

MmapBufferReader::~MmapBufferReader() {
  munmap(data_start_, mmap_size_);
  close(fd_);
}

bool MmapBufferReader::ReadRecord(char *&record, int len) {
  if (data_curr_ + len > data_start_ + mmap_size_ - 8) {
    spdlog::error("[ReadRecord] read overflow mmap_size error");
    exit(1);
  }
  if (static_cast<uint64_t>(data_curr_ - data_start_) / RecordSize < *commit_cnt_) {
    record = data_curr_;
    data_curr_ += len;
    return true;
  }
  return false;
}


PmapBufferWriter::PmapBufferWriter(const std::string &filename, size_t pool_size)
    : mmap_writer_(nullptr), pool_size_(pool_size), start_(nullptr), curr_(nullptr) {

  buff_filename_ = filename + PmapBufferWriterFileNameSuffix;
  pmem_filename_ = filename;
  mmap_writer_ = new MmapBufferWriter(buff_filename_, PmapBufferWriterFileSize);

  bool create_file = !Util::FileExists(pmem_filename_);
  // 2. pmap
  void* pmemaddr = NULL;
	size_t mapped_len;
	int is_pmem;
	if ((pmemaddr = pmem_map_file(pmem_filename_.c_str(), pool_size_,
				PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
		perror("pmem_map_file");
		exit(1);
	}
  if (mapped_len != pool_size_ || is_pmem == 0) {
    spdlog::warn("[PmapReader] unexpected error happen when pmem_map_file, mapped_len: {}, is_pmem: {}", mapped_len, is_pmem);
  }
  start_ = reinterpret_cast<char *>(pmemaddr);
  const uint64_t record_num_per_round_buffer = (PmapBufferWriterSize) / RecordSize;
  // 每次刷的字节数
  const uint64_t bytes_per_round_buffer = record_num_per_round_buffer * RecordSize;
  curr_ = start_ + (mmap_writer_->GetCommitCnt() / record_num_per_round_buffer) * bytes_per_round_buffer;

  if (create_file) {
    pmem_memset_nodrain(start_, 0, pool_size_);
    pmem_drain();
  }
}

PmapBufferWriter::~PmapBufferWriter() {
  // don't need to flush buffer (mmap always there, havn't disappear)
  delete mmap_writer_;
  pmem_drain();
  pmem_unmap(start_, pool_size_);
}

PmapBufferReader::PmapBufferReader(const std::string &filename, size_t pool_size)
    : mmap_reader_(nullptr), pool_size_(pool_size)
    , start_(nullptr), curr_(nullptr), must_have_flush_cnt_(0) {

  buff_filename_ = filename + PmapBufferWriterFileNameSuffix;
  pmem_filename_ = filename;
  mmap_reader_ = new MmapBufferReader(buff_filename_, PmapBufferWriterFileSize);

  bool create_file = !Util::FileExists(pmem_filename_);
  // 2. mmap
  void* pmemaddr = NULL;
	size_t mapped_len;
	int is_pmem;
	if ((pmemaddr = pmem_map_file(pmem_filename_.c_str(), pool_size_,
				PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
		perror("pmem_map_file");
		exit(1);
	}
  if (mapped_len != pool_size_ || is_pmem == 0) {
    spdlog::warn("[PmapBufferReader] unexpected error happen when pmem_map_file, mapped_len: {}, is_pmem: {}", mapped_len, is_pmem);
  }

  start_ = reinterpret_cast<char *>(pmemaddr);
  curr_ = start_;
  if (create_file) {
    pmem_memset_nodrain(start_, 0, pool_size_);
    pmem_drain();
  }
  uint64_t record_num_per_round_buffer = PmapBufferWriterSize / RecordSize;
  if (mmap_reader_->CommitCnt() != 0 && mmap_reader_->CommitCnt() % record_num_per_round_buffer == 0) {
    // 当buffer刚刚写满时，下一次写，才会刷buffer到pmem中
    must_have_flush_cnt_ = mmap_reader_->CommitCnt() - record_num_per_round_buffer;
  } else {
    must_have_flush_cnt_ = mmap_reader_->CommitCnt() - mmap_reader_->CommitCnt() % record_num_per_round_buffer;
  }
  read_cnt_ = 0;
}

PmapBufferReader::~PmapBufferReader() {
  delete mmap_reader_;
  pmem_unmap(start_, pool_size_);
}

bool PmapBufferReader::ReadRecord(char *&record, int len) {
  if (curr_ + len > start_ + pool_size_) {
    spdlog::error("[PmapBufferReader] error");
    exit(1);
  }
  // 1. reader from pmem
  if (read_cnt_ < must_have_flush_cnt_) {
    record = curr_;
    curr_ += len;
    read_cnt_++;
    return true;
  }
  // 2. read from mmap buffer
  // 因为封装的mmap_reader作为buffer重复利用，因此mmap_reader中的commit_cnt_作为所有record的个数，
  // 因此mmap_reader_->ReadRecord的返回值不可信，可能溢出coredump, 需要在调用之前做判断
  if (read_cnt_ >= mmap_reader_->CommitCnt()) {
    return false;
  }
  mmap_reader_->ReadRecord(record, len);
  read_cnt_++;
  return true;
}

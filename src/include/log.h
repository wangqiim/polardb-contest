#include <string>
#include <libpmemlog.h>
#include "env.h"

//---------------------- write append log--------------------------
class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(PosixWritableFile* dest);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer();

  int AddRecord(const void* data, int len);
 private:
  PosixWritableFile* dest_;
};


class Reader {
 public:
  Reader(PosixSequentialFile* file);
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  ~Reader();
  bool ReadRecord(std::string &record, int len);
 private:
  PosixSequentialFile* const file_;
  bool eof_;
  std::string buf_;
  char backing_store_[kBlockSize];
  int buf_start_offset_;
  int buf_end_offset_;
};


//--------------------- pmem log -----------------------------------
// ref: https://pmem.io/pmdk/manpages/linux/master/libpmemlog/libpmemlog.7/
class PmemReader {
 public:
  PmemReader(const std::string &filename, int pool_size);
  ~PmemReader();

  int Scan(void (*cb)(const char *record, void *ctx), void *ctx);
 private:
  PMEMlogpool *plp_;
  const std::string filename_;
  bool has_walk_;
};

class PmemWriter {
 public:
  PmemWriter() = delete;
  PmemWriter(const std::string &filename, int pool_size);
  ~PmemWriter();
  PmemWriter(const PmemWriter&) = delete;
  PmemWriter& operator=(const PmemWriter&) = delete;

  int Append(const void* data, const size_t len);
 private:
  PMEMlogpool *plp_;
  const std::string filename_;
};

//--------------------- mmap file-----------------------------------
class MmapWriter {
 public:
  MmapWriter() = delete;
  MmapWriter(const std::string &filename, int mmap_size);
  ~MmapWriter();
  MmapWriter(const MmapWriter&) = delete;
  MmapWriter& operator=(const MmapWriter&) = delete;

  int Append(const void* data, const size_t len);
 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  char *start_;
  char *curr_;
};

class MmapReader {
 public:
  MmapReader(const std::string &filename, int mmap_size);
  ~MmapReader();

  bool ReadRecord(char *&record, int len);
 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  char *start_;
  char *curr_;
};

//--------------------- pmem mmap file-----------------------------------
class PmapWriter {
 public:
  PmapWriter() = delete;
  PmapWriter(const std::string &filename, size_t mmap_size);
  ~PmapWriter();
  PmapWriter(const PmapWriter&) = delete;
  PmapWriter& operator=(const PmapWriter&) = delete;

  int Append(const void* data, const size_t len);
 private:
  const std::string filename_;
  size_t mmap_size_;
  char *start_;
  char *curr_;
};

class PmapReader {
 public:
  PmapReader(const std::string &filename, size_t mmap_size);
  ~PmapReader();

  bool ReadRecord(char *&record, int len);
 private:
  const std::string filename_;
  size_t mmap_size_;
  char *start_;
  char *curr_;
};

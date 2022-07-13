#include <string>
#include <libpmemlog.h>
#include "env.h"

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

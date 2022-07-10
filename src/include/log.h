#include <string>
#include "env.h"

std::string WALFileName(const std::string &dir);

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(PosixWritableFile* dest);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer() = default;

  int AddRecord(const void* data, int len);
  
  PosixWritableFile* GetFile(); // 外部delete dest_;
 private:
  PosixWritableFile* dest_;
};


class Reader {
 public:
  Reader(PosixSequentialFile* file);
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  ~Reader() = default;
  bool ReadRecord(std::string &record, int len);
 private:
  PosixSequentialFile* const file_;
  bool eof_;
  std::string buf_;
  char backing_store_[kBlockSize];
  int buf_start_offset_;
  int buf_end_offset_;
};
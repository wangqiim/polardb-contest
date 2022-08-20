#include <xmmintrin.h>
#include <string>
#include <libpmemlog.h>
#include "env.h"

//--------------------- mmap file-----------------------------------
class MmapWriter {
 public:
  MmapWriter() = delete;
  MmapWriter(const std::string &filename, int mmap_size);
  ~MmapWriter();
  MmapWriter(const MmapWriter&) = delete;
  MmapWriter& operator=(const MmapWriter&) = delete;

  int Append(const void* data, const size_t len);

  int GetCommitCnt() { return *commit_cnt_; }
  int Bytes() { return data_curr_ - data_start_; }
  char* Data() { return data_start_; }
  void Reset() { data_curr_ = data_start_; }
  
  size_t MaxSlot() const { return (mmap_size_ - 8) / RecordSize; }
  // 预取第slot个记录的头指针，每次顺便预取一下commit_cnt_指针
  void WarmUp(const size_t slot) { 
    _mm_prefetch((const void *)(data_start_ + (slot * RecordSize)), _MM_HINT_T0);
    _mm_prefetch((const void *)commit_cnt_, _MM_HINT_T0);
  }

 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  char *start_;
  uint64_t *commit_cnt_; // commit_cnt_ = (uint64_t *)mmap_start_ptr
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
};

class MmapReader {
 public:
  MmapReader(const std::string &filename, int mmap_size);
  ~MmapReader();

  bool ReadRecord(char *&record, int len);

  uint64_t CommitCnt() { return *commit_cnt_; }
  char* Data() { return data_start_; }
  
 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  char *start_;
  uint64_t *commit_cnt_; // commit_cnt_ = (uint64_t *)mmap_start_ptr
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
};

//--------------------- pmem Buffer Writer-----------------------------------
class PmapBufferWriter {
 public:
  PmapBufferWriter() = delete;
  PmapBufferWriter(const PmapBufferWriter&) = delete;
  PmapBufferWriter& operator=(const PmapBufferWriter&) = delete;
  
  PmapBufferWriter(const std::string &filename, size_t pool_size);
  ~PmapBufferWriter();

  int Append(const void* data, const size_t len);

  // 对于pmem要warm整个mmap_writer_(buffer)
  void WarmUp() {
    for (size_t i = 0; i < mmap_writer_->MaxSlot(); i++) {
      mmap_writer_->WarmUp(i);
    }
  }
 private:
  std::string buff_filename_;
  std::string pmem_filename_;

  MmapWriter *mmap_writer_; // buffer writer
  size_t pool_size_;
  char *start_;
  char *curr_;
};

class PmapBufferReader {
 public:
  PmapBufferReader(const std::string &filename, size_t pool_size);
  ~PmapBufferReader();

  bool ReadRecord(char *&record, int len);
 private:
  std::string buff_filename_;
  std::string pmem_filename_;

  MmapReader *mmap_reader_; // buffer writer
  size_t pool_size_;
  char *start_;
  char *curr_;

  // when read, if have read cnt < must_have_flush_cnt_, read from pmem
  // else read from buffer (mmap_reader_)
  uint64_t must_have_flush_cnt_; 
  uint64_t read_cnt_;
};

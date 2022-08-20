#include <xmmintrin.h>
#include <string>
#include <libpmem.h>
#include "rte_memcpy.h"
#include "def.h"

//--------------------- mmap file-----------------------------------
class MmapWriter {
 public:
  MmapWriter() = delete;
  MmapWriter(const std::string &filename, int mmap_size);
  ~MmapWriter();
  MmapWriter(const MmapWriter&) = delete;
  MmapWriter& operator=(const MmapWriter&) = delete;

  int Append(const void* data) {
    if (MmapSize > cnt_ * OSPageSize) {
      _mm_prefetch(data_start_ + cnt_ * OSPageSize, _MM_HINT_NTA);
    }
    memcpy(data_curr_, data, 256);
    memcpy(data_curr_ + 256, (const char *)data + 256, 16);
    memcpy(data_curr_ + 272, (const char *)data + 272, 8);
    *(uint64_t *)(data_curr_ + 272) = CommitFlag;
    data_curr_ += RecordSize;
    cnt_++;
    return 0;
  }
  
  size_t MaxSlot() const { return (mmap_size_ - 8) / RecordSize; }
  // 预取第slot个记录的头指针，每次顺便预取一下commit_cnt_指针
  void WarmUp(const size_t slot) { 
    _mm_prefetch((const void *)(data_start_ + (slot * RecordSize)), _MM_HINT_T0);
  }

 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  int cnt_;
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
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
  int cnt_;
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
};

//--------------------- pmem Buffer Writer-----------------------------------
class MmapBufferWriter {
 public:
  MmapBufferWriter() = delete;
  MmapBufferWriter(const std::string &filename, int mmap_size);
  ~MmapBufferWriter();
  MmapBufferWriter(const MmapBufferWriter&) = delete;
  MmapBufferWriter& operator=(const MmapBufferWriter&) = delete;

  int Append(const void* data) {
    if (data_curr_ + RecordSize > data_start_ + mmap_size_ - 8) {
      return -1;
    }
    memcpy(data_curr_, data, 256);
    memcpy(data_curr_ + 256, (const char *)data + 256, 16);
    *commit_cnt_ = *commit_cnt_ + 1;
    data_curr_ += RecordSize;
    return 0;
  }

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
  uint64_t *commit_cnt_; // commit_cnt_ = (uint64_t *)mmap_start_ptr
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
};

class MmapBufferReader {
 public:
  MmapBufferReader(const std::string &filename, int mmap_size);
  ~MmapBufferReader();

  bool ReadRecord(char *&record, int len);

  uint64_t CommitCnt() { return *commit_cnt_; }
  char* Data() { return data_start_; }
  
 private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  uint64_t *commit_cnt_; // commit_cnt_ = (uint64_t *)mmap_start_ptr
  char *data_start_; // data_start_ = (char *)mmap_start_ptr + 8
  char *data_curr_;
};

class PmapBufferWriter {
 public:
  PmapBufferWriter() = delete;
  PmapBufferWriter(const PmapBufferWriter&) = delete;
  PmapBufferWriter& operator=(const PmapBufferWriter&) = delete;
  
  PmapBufferWriter(const std::string &filename, size_t pool_size);
  ~PmapBufferWriter();

  int Append(const void* data) {
    if (mmap_writer_->Append(data) == 0) {
      return 0;
    }
    // flush buffer
    pmem_memcpy(curr_, mmap_writer_->Data(), mmap_writer_->Bytes(), PMEM_F_MEM_NODRAIN|PMEM_F_MEM_NONTEMPORAL|PMEM_F_MEM_WC);
    curr_ += mmap_writer_->Bytes();
    mmap_writer_->Reset();

    // must success!! (ret == 0)
    mmap_writer_->Append(data);
    return 0;
  }

  // 对于pmem要warm整个mmap_writer_(buffer)
  void WarmUp() {
    for (size_t i = 0; i < mmap_writer_->MaxSlot(); i++) {
      mmap_writer_->WarmUp(i);
    }
  }
 private:
  std::string buff_filename_;
  std::string pmem_filename_;

  MmapBufferWriter *mmap_writer_; // buffer writer
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

  MmapBufferReader *mmap_reader_;
  size_t pool_size_;
  char *start_;
  char *curr_;

  // when read, if have read cnt < must_have_flush_cnt_, read from pmem
  // else read from buffer (mmap_reader_)
  uint64_t must_have_flush_cnt_; 
  uint64_t read_cnt_;
};

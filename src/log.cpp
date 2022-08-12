#include <functional>
#include <sys/mman.h>
#include "spdlog/spdlog.h"
#include "log.h"
#include <unistd.h>
#include <libpmem.h>
#include "util.h"

Writer::Writer(PosixWritableFile* dest) : dest_(dest) {}

Writer::~Writer() {
  delete dest_;
}

int Writer::AddRecord(const void* data, int len) {
  // int ret = dest_->Append(data, len);
  // if (ret == 0) {
  //   ret = dest_->Flush();
  // }
  // return ret;
  return dest_->WriteUnbuffered((const char*)data, len);
}

Reader::Reader(PosixSequentialFile* file)
    : file_(file), eof_(false), buf_(), backing_store_()
    , buf_start_offset_(0), buf_end_offset_(0) {}

Reader::~Reader() {
  delete file_;
}

bool Reader::ReadRecord(std::string &record, int len) {
  if (len != RecordSize) {
    spdlog::error("ReadRecord error, len[{}] != RecordSize[{}]", len, RecordSize);
  }
  record.clear();
  // 底层一次read一个kBlockSize的buf
  // 每次先从buf里取记录,取不到则读取下一个kBlockSize的块到buf
  if (!eof_) {
    int cur_record_len = 0;
    if (buf_start_offset_ + RecordSize > buf_end_offset_) {
      // 先读上一个buf尾部不完整的record
      cur_record_len = buf_end_offset_ - buf_start_offset_;
      record.append(buf_.data() + buf_start_offset_, cur_record_len);
      int status = file_->Read(kBlockSize, buf_, backing_store_);
      if (status != 0) {
        return false;
      }
      buf_start_offset_ = 0;
      buf_end_offset_ = buf_.size();
      if (buf_.size() == 0) {
        eof_ = true;
      }
    }
    if (RecordSize - cur_record_len > buf_end_offset_ - buf_start_offset_) {
      if (cur_record_len != 0) {
        spdlog::info("can't read a complete record");
      } // else eof!
      return false;
    }
    record.append(buf_.data() + buf_start_offset_, RecordSize - cur_record_len);
    buf_start_offset_ += RecordSize - cur_record_len;
  } else {
    return false;
  }
  return true;
}

//--------------------PmemReader--------------------------
PmemReader::PmemReader(const std::string &filename, int pool_size)
    :plp_(nullptr), filename_(filename), has_walk_(false) {
	/* create the pmemlog pool or open it if it already exists */
	plp_ = pmemlog_create(filename.c_str(), pool_size, 0666);
	if (plp_ == nullptr) {
    plp_ = pmemlog_open(filename.c_str());
  } else {
    spdlog::debug("init create log[{}] success!", filename);
  }
	if (plp_ == nullptr) {
    spdlog::error("can't create and open pmemlog, filename: {}", filename_);
		exit(1);
	}

	size_t nbyte = pmemlog_nbyte(plp_);
  spdlog::debug("log holds {} bytes\n", nbyte);
}

PmemReader::~PmemReader() {
  pmemlog_close(plp_);
}

class WalkHelper {
  public:
    WalkHelper(void (*cb)(const char *record, void *ctx), void *ctx)
        : cb_(cb), ctx_(ctx) {}

    std::function<void(const char *record, void *ctx)> cb_;
    void *ctx_;
};

int printit(const void *buf, size_t len, void *arg) {
  WalkHelper *helper = (WalkHelper *)arg;
  const char *datas = (const char*)buf;
  for (size_t i = 0; i < len; i += RecordSize) {
    helper->cb_(datas, helper->ctx_);
    datas += RecordSize;
  }
	return 0;
}

int PmemReader::Scan(void (*cb)(const char *record, void *ctx), void *ctx) {
  if (has_walk_) {
    spdlog::error("pmemlog need init once, only can be walk once.");
    return -1;
  }
  WalkHelper helper(cb, ctx);
  pmemlog_walk(plp_, 0, printit, &helper);
  has_walk_ = true;
  return 0;
}

//--------------------PmemWriter--------------------------

PmemWriter::PmemWriter(const std::string &filename, int pool_size)
    :plp_(nullptr), filename_(filename) {
	/* create the pmemlog pool or open it if it already exists */
	plp_ = pmemlog_create(filename.c_str(), pool_size, 0666);
	if (plp_ == nullptr) {
    plp_ = pmemlog_open(filename.c_str());
  }
	if (plp_ == nullptr) {
    spdlog::error("can't create and open pmemlog, filename: {}", filename_);
		exit(1);
	}
}

PmemWriter::~PmemWriter() {
  pmemlog_close(plp_);
}

int PmemWriter::Append(const void* data, const size_t len) {
	if (pmemlog_append(plp_, data, len) < 0) {
    spdlog::error("can't Append data to pmemlog, filename: {}", filename_);
		exit(1);
	}
  return 0;
}

//--------------------- mmap file-----------------------------------
MmapWriter::MmapWriter(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1), start_(nullptr)
    , commit_cnt_(nullptr), data_start_(nullptr), data_curr_(nullptr) {
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
  }
  start_ = reinterpret_cast<char *>(ptr);
  commit_cnt_ = reinterpret_cast<uint64_t *>(start_);
  data_start_ = start_ + 8;
  uint64_t record_num_per_round_buffer = (mmap_size_ - 8) / RecordSize;
  data_curr_ = data_start_ + (*commit_cnt_ % record_num_per_round_buffer) * RecordSize;
}

MmapWriter::~MmapWriter() {
  munmap(start_, mmap_size_);
  close(fd_);
}

int MmapWriter::Append(const void* data, const size_t len) {
  if (data_curr_ + len > start_ + mmap_size_) {
    return -1;
  }
  memcpy(data_curr_, data, len);
  *commit_cnt_ = *commit_cnt_ + 1;
  data_curr_ += len;
  return 0;
}

MmapReader::MmapReader(const std::string &filename, int mmap_size)
    : filename_(filename), mmap_size_(mmap_size), fd_(-1), start_(nullptr)
    , commit_cnt_(nullptr), data_start_(nullptr), data_curr_(nullptr) {
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
  }
  start_ = reinterpret_cast<char *>(ptr);
  commit_cnt_ = reinterpret_cast<uint64_t *>(start_);
  data_start_ = start_ + 8;
  data_curr_ = data_start_;
}

MmapReader::~MmapReader() {
  munmap(start_, mmap_size_);
  close(fd_);
}

bool MmapReader::ReadRecord(char *&record, int len) {
  if (data_curr_ + len > start_ + mmap_size_) {
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

//--------------------- pmem file-----------------------------------
PmapBufferWriter::PmapBufferWriter(const std::string &filename, size_t pool_size)
    : mmap_writer_(nullptr), pool_size_(pool_size), start_(nullptr), curr_(nullptr) {

  buff_filename_ = filename + PmapBufferWriterFileNameSuffix;
  pmem_filename_ = filename;
  mmap_writer_ = new MmapWriter(buff_filename_, PmapBufferWriterFileSize);

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

int PmapBufferWriter::Append(const void* data, const size_t len) {
  if (mmap_writer_->Append(data, len) == 0) {
    return 0;
  }
  // flush buffer
  pmem_memcpy_nodrain(curr_, mmap_writer_->Data(), mmap_writer_->Bytes());
  curr_ += mmap_writer_->Bytes();
  mmap_writer_->Reset();

  // must success!! (ret == 0)
  mmap_writer_->Append(data, len);
  return 0;
}

PmapBufferReader::PmapBufferReader(const std::string &filename, size_t pool_size)
    : mmap_reader_(nullptr), pool_size_(pool_size)
    , start_(nullptr), curr_(nullptr), must_have_flush_cnt_(0) {

  buff_filename_ = filename + PmapBufferWriterFileNameSuffix;
  pmem_filename_ = filename;
  mmap_reader_ = new MmapReader(buff_filename_, PmapBufferWriterFileSize);

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

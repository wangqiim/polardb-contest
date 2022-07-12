#include <functional>
#include "spdlog/spdlog.h"
#include "log.h"

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
    spdlog::info("init create log[{}] success!", filename);
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

#include <queue>
#include <mutex>
#include <unistd.h>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <string.h>
#include <xmmintrin.h>
#include <sys/mman.h>
#include <libpmem.h>

#include "spdlog/spdlog.h"

#include "def.h"
#include "util.h"

const uint64_t PmemSize = 16 * (1 << 30); // 16G 搓搓有余
// ------ log_buffer_mgr.hpp -------
const uint64_t LogBufferMgrSize = 6.4 * (1024ULL * 1024ULL * 1024ULL); // pagecache: 6.4G?? (32 * 0.2)
const uint64_t LogBufferMgrHeaderSize = 8ULL; // LogbufferMgrHeader记录目前已经落盘(pmem)了多少条记录
const uint64_t LogBufferMgrBody = LogBufferMgrSize - LogBufferMgrHeaderSize;

const uint64_t LogBufferHeaderSize = 8ULL; // 当前buffer已经commit了多少条记录
const uint64_t LogBufferBodySize = 280ULL * 256ULL * 100ULL; // 7000KB  (280 * 256 = 70KB) LogBufferSize = LCM(280, 256), 是倍数即可，当每个buffer足够大时，不是倍数应该影响也不大
const uint64_t LogBufferSize = LogBufferHeaderSize + LogBufferBodySize;
const uint64_t LogBufferFreeNumPerBufferMgr = LogBufferMgrBody / LogBufferSize; // todo(wq): 尽量不要让mmap(LogBufferMgrSize)有剩余

const uint32_t FlushingNome = 0xFFFFFFFFUL;

class MmapWrapper {
public:
    // 构造时保证创建文件并且alloc大小, 如果是第一次创建memset(0)，并且warmup整块mmap内存
  MmapWrapper(const std::string &filename, int mmap_size)
							: filename_(filename), mmap_size_(mmap_size), fd_(-1)
							, start_(nullptr) {
		Util::CreateIfNotExists(filename_);
		// 1. open fd; (must have been create)
		fd_ = open(filename_.c_str(), O_RDWR, 0644);
		if (fd_ < 0) {
				spdlog::error("[MmapWrapper] can't open file {}", filename_);
				exit(1);
		}
		int off = (int)lseek(fd_, 0, SEEK_END);
		if (off < 0) {
				spdlog::error("[MmapWrapper] lseek end failed");
				exit(1);
		}
		if (off == 0) {
				if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
				spdlog::error("[MmapWrapper] posix_fallocate failed");
				exit(1);
				}
		}
		// 2. mmap
		void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
		if (ptr == MAP_FAILED) {
				spdlog::error("[MmapWrapper] mmap failed, errno is {}", strerror(errno));
				exit(1);
		}
		if (off == 0) {
				memset(ptr, 0, mmap_size_);
		}
		start_ = reinterpret_cast<char *>(ptr);
		warmUpAll();
	}

  ~MmapWrapper() {
    munmap(start_, mmap_size_);
    close(fd_);
  }
  
char *GetOffset(uint64_t offset) { return start_ + offset; }

private:
  void warmUpAll() { 
    for (uint64_t offset = 0; offset < mmap_size_; offset += 64) {
        _mm_prefetch((const void *)(start_ + offset), _MM_HINT_T0);
    }
  }

private:
  const std::string filename_;
  int mmap_size_;
  int fd_;
  char *start_;
};

class PmemWrapper {
 public:
  PmemWrapper(const std::string &filename, uint64_t pool_size)
				: filename_(filename), pool_size_(pool_size), fd_(-1)
				, start_(nullptr) {
		// 1. create file if not exist
		bool create_file = !Util::FileExists(filename_);
		// 2. pmap
		void* pmemaddr = NULL;
		size_t mapped_len;
		int is_pmem;
		if ((pmemaddr = pmem_map_file(filename_.c_str(), pool_size_,
					PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
			perror("pmem_map_file");
			exit(1);
		}
		if (mapped_len != pool_size_ || is_pmem == 0) {
			spdlog::warn("[PmemWrapper] unexpected error happen when pmem_map_file, mapped_len: {}, is_pmem: {}", mapped_len, is_pmem);
		}
		start_ = reinterpret_cast<char *>(pmemaddr);
		if (create_file) {
			pmem_memset_nodrain(start_, 0, pool_size_);
			pmem_drain();
		}
	}

  ~PmemWrapper() {
		pmem_drain();
	  pmem_unmap(start_, pool_size_);
	}

	int Copy(char *dst, const char *src, uint64_t len = LogBufferBody) {
  	pmem_memcpy_nodrain(dst, src, len);
	}

	char *GetOffset(uint64_t offset) { return start_ + offset; }

 private:
  const std::string filename_;
  uint64_t pool_size_;
  int fd_;
  char *start_;
};

class LogBuffer;

class LogBufferMgrHeader {
public:
	LogBufferMgrHeader(uint32_t flushed_cnt, uint32_t flushing_no)
		: flushed_cnt_(flushed_cnt), flushing_no_(flushing_no) { }
	void AtomicSet(uint32_t flushed_cnt, uint32_t flushing_no) {
		uint64_t tmp;
		*(uint32_t *)tmp = flushed_cnt;
		*((uint32_t *)tmp + 1) = flushing_no;
		*(uint64_t *)this = tmp; // 原子写入，防止崩溃
	}
	uint32_t flushed_cnt_; // 此处指针，因为记录第一个记录对应的flush_cnt
	uint32_t flushing_no_; // 此处指针，因为commit_cnt_也要落盘
};
// LogBufferMgr中mmap的存储结构
// ________________________________________________________________________________________________________________
// | flushed_cnt(4bytes), flushing_no_(4bytes) | commit_cnt_(8bytes) | commit_cnt_(8bytes) | commit_cnt_(8bytes) |......
//
// LogBufferMgr中pmem的存储结构  __________________________________________________________________
// 全是record，无其他字段,       | record(172bytes) | record(172bytes) | record(172bytes) | ......
class LogBufferMgr {
public:
	LogBufferMgr(const char *mmap_filename, const char *pmem_filename,
							uint64_t log_buffer_size = LogBufferMgrSize, uint64_t pmem_size = PmemSize) {
		mmap_wrapper_ = new MmapWrapper(mmap_filename, log_buffer_size);
		pmem_wrapper_ = new PmemWrapper(mmap_filename, PmemSize);

		header_ = reinterpret_cast<LogBufferMgrHeader *>(mmap_wrapper_->GetOffset(0));
		char *start = mmap_wrapper_->GetOffset(LogBufferMgrHeaderSize);
		char *end = mmap_wrapper_->GetOffset(log_buffer_size);
		for (char *curr = start; curr + log_buffer_size <= end; curr += LogBufferSize) {
			// 重启时全部置为可分配，被拿走消费的时候发现写满了再归还
			LogBuffer *log_buffer = reinterpret_cast<LogBuffer *>(curr);
			free_log_buffer_.push(log_buffer);
		}
	}

	~LogBufferMgr() {
		delete mmap_wrapper_;
		delete pmem_wrapper_;
	}

	// 阻塞调用： 获取一个空闲页用作buffer
	LogBuffer* GetFreeLogBuffer() {
			std::unique_lock<std::mutex> guard(free_log_buffer_mtx_);
			while (free_log_buffer_.empty()) {
					free_log_buffer_cv_.wait(guard);
			}
			LogBuffer* log_buffer = free_log_buffer_.front();
			free_log_buffer_.pop();
			return log_buffer;
	}

	// 阻塞调用, 归还一个脏页
	void GiveBackDirtyBuffer(LogBuffer* log_buffer) {
			std::unique_lock<std::mutex> guard(dirty_log_buffer_mtx_);
			dirty_log_buffer_.push(log_buffer);
			dirty_log_buffer_cv_.notify_one(); // todo(wq): notify_one is ok?
	}

	void StartFlushRun() {
        // todo(wq): mgr析构时需要join这个线程吗???
        std::thread(LogBufferMgr::backGroudFlush, this);
    }

	char* GetPmemStart() { return pmem_wrapper_->GetOffset(0); }

	LogBufferMgrHeader* Header() { return header_; }

	LogBuffer* GetLogBufferByNo(uint32_t no) {
		return (LogBuffer *)(mmap_wrapper_->GetOffset(LogBufferMgrHeaderSize + no * LogBufferSize));
	}
private:
    // todo(wq): 无锁设计
    // 阻塞调用, 获取一个脏页，准备刷盘，被background线程调用
    LogBuffer* getDirtyLogBuffer() {
        std::unique_lock<std::mutex> guard(dirty_log_buffer_mtx_);
        while (dirty_log_buffer_.empty()) {
            dirty_log_buffer_cv_.wait(guard);
        }
        LogBuffer* log_buffer = dirty_log_buffer_.front();
        dirty_log_buffer_.pop();
        return log_buffer;
    }

    // todo(wq): 设计成无锁
    // 阻塞调用, 归还一个自由页，被background线程调用
    void giveBackFreeBuffer(LogBuffer* log_buffer) {
        std::unique_lock<std::mutex> guard(free_log_buffer_mtx_);
        free_log_buffer_.push(log_buffer);
        free_log_buffer_cv_.notify_one(); // todo(wq): notify_one is ok?
    }

    // 开启后台线程刷dirty_buffer
    void backGroudFlush() {
        for (;;) {
            // 1. 阻塞调用，获取一个DirtyLogBuffer
            LogBuffer *dirty_log_buffer = getDirtyLogBuffer();
            // 2. 刷盘
			header_->AtomicSet(header_->flushed_cnt_, Offset2No(dirty_log_buffer));
            flush(dirty_log_buffer);
			// 3. 先改logbuffer header再改logbufferMgr，这样如果再改完buffer header之后崩溃，
			// 恢复的时候，则重新刷一次该logbuffer也无妨
			header_->AtomicSet(header_->flushed_cnt_ + LogBufferFreeNumPerBufferMgr, FlushingNome);
            dirty_log_buffer->Reset();
			// 4. 将该buffer归还到free队列里
			giveBackFreeBuffer(dirty_log_buffer);
        }
    }

	// 根据mmap的起始偏移地址，推算出是第几个
	uint32_t Offset2No(LogBuffer* log_buffer) {
		return ((char *)log_buffer - mmap_wrapper_->GetOffset(LogBufferMgrHeaderSize)) / LogBufferSize;
	}

    void flush(LogBuffer *dirty_log_buffer) {
		uint64_t dst_offset = header_->flushed_cnt_ * RecordSize;
		char *dst = pmem_wrapper_->GetOffset(dst_offset);
		char *data_src = dirty_log_buffer->DataStartPtr();
		pmem_wrapper_->Copy(dst, data_src, LogBufferBodySize);
    }

private:
	MmapWrapper *mmap_wrapper_;
	PmemWrapper *pmem_wrapper_;

	LogBufferMgrHeader *header_; // 记录目前已经被刷了多少条记录了

	// todo(wq): 优化锁的粒度，比如无锁队列 + 自旋
	std::mutex free_log_buffer_mtx_;
	std::condition_variable free_log_buffer_cv_;
	std::queue<LogBuffer *> free_log_buffer_;

	std::mutex dirty_log_buffer_mtx_;
	std::condition_variable dirty_log_buffer_cv_;
	std::queue<LogBuffer *> dirty_log_buffer_; // 已经写满了，需要刷盘
};

// 为了恢复方便，mgr为每个buffer的heder不是置为0, 而是将8字节拆成2个4字节
// [uint_32, uint_32]=[该buffer的第一次写将对应总日志的第几条, 该buffer已经被写了几条记录]
class LogBuffer {
public:
    LogBuffer(char *buffer_start, uint64_t size) {
        commit_cnt_ = reinterpret_cast<uint64_t *>(buffer_start);
        start_ = buffer_start + LogBufferHeaderSize;
        curr_ = start_;
        size_ = size;
    }

    void Reset() { *commit_cnt_ = 0; };

    bool IsFull() { return *commit_cnt_ == size_; };

    uint64_t CommitCnt() { return *commit_cnt_; };
    void SetCommitCnt(uint64_t comit_cnt) {  *commit_cnt_ = comit_cnt; };

	char *DataStartPtr() { return start_; }

    // 每次默认append长度为RecordSize(172)
    // data: append的数据
    // 返回值：0: 成功
    int Append(const char *data) {
        if (IsFull()) {
            return -1;
        }
        memcpy(curr_, data, RecordSize);
        curr_ += RecordSize;
        *commit_cnt_ += 1;
		return 0;
    }

private:
	uint64_t *commit_cnt_; // 该页提交了多少条记录
	char     *start_;
	char     *curr_;
	uint64_t size_;
};

class LogWriter {
public:
    LogWriter(LogBufferMgr *log_buffer_mgr)
        : log_buffer_mgr_(log_buffer_mgr)
        , log_buffer_(log_buffer_mgr_->GetFreeLogBuffer()) {
    }
    
    int Append(const char *data) {
        if (log_buffer_->Append(data) == 0) {
            return 0;
        }
        log_buffer_mgr_->GiveBackDirtyBuffer(log_buffer_);
        log_buffer_ = log_buffer_mgr_->GetFreeLogBuffer();
        return log_buffer_->Append(data); // must success!!!
    }
private:
    LogBufferMgr *log_buffer_mgr_;
    LogBuffer    *log_buffer_;
};

class LogReader {
public:
    LogReader(LogBufferMgr *log_buffer_mgr)
        : log_buffer_mgr_(log_buffer_mgr)
		, next_no_(0)
		, log_buffer_read_cnt_(0)
        , curr_log_buffer_(log_buffer_mgr_->GetLogBufferByNo(next_no_++))
		, pmem_start_(log_buffer_mgr_->GetPmemStart())
		, header_(log_buffer_mgr_->Header())
		, read_cnt_(0) {
		// 如果flushing_no对应的log page正在刷，则将其commit_cnt置满
		LogBuffer* flushing_log_buffer = log_buffer_mgr_->GetLogBufferByNo(header_->flushing_no_);
		flushing_log_buffer->SetCommitCnt(LogBufferBodySize/RecordSize);
    }
    
    int ReadRecord(char *&record, int len = RecordSize) {
		// 1. 先读pmem
		if (read_cnt_ < header_->flushed_cnt_) {
			memcpy(record, pmem_start_, len);
			pmem_start_ += len;
			read_cnt_++;
			return 0;
		}
		// 2. 再读所有log_buffer
		if (log_buffer_read_cnt_ < curr_log_buffer_->CommitCnt()) {
			memcpy(record, curr_log_buffer_->DataStartPtr() + log_buffer_read_cnt_ * len, len);
			log_buffer_read_cnt_++;
			read_cnt_++;
			return 0;
		}
		while (log_buffer_read_cnt_ == curr_log_buffer_->CommitCnt()) {
			if (next_no_ < LogBufferFreeNumPerBufferMgr) {
				curr_log_buffer_ = log_buffer_mgr_->GetLogBufferByNo(next_no_++);
				log_buffer_read_cnt_ = 0;
			} else { // 所有的logbuffer都尝试过了，buffer里的数据被读完了
				return -1;
			}
		}
		memcpy(record, curr_log_buffer_->DataStartPtr() + log_buffer_read_cnt_ * len, len);
		log_buffer_read_cnt_++;
		read_cnt_++;
		return 0;
    }
private:
    LogBufferMgr *log_buffer_mgr_;
	
	uint32_t  next_no_;
	uint32_t  log_buffer_read_cnt_;
    LogBuffer *curr_log_buffer_;

	char *pmem_start_;
	LogBufferMgrHeader *header_;
	uint32_t read_cnt_;
};

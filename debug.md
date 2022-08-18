`def.h`
```h
#pragma once

constexpr const int RecordSize = 8 + 128 + 128 + 8;

// ------ log.h ----------
const char WALFileNameSuffix[] = "WAL";

// ------ engine.h -------
const int WritePerClient = 1000; 
const int ClientNum = 50;

const int WaitChangeFinishSecond = 1;
const int FenceSecond = 3;

enum Phase{Hybrid=0, WriteOnly, ReadOnly};
```

`log_buffer_mgr.hpp`
```h
// 调参: 本地运行需要调小
const uint64_t PmemSize = 0.1 * (1024ULL * 1024ULL * 1024ULL); // 16G 搓搓有余
// ------ log_buffer_mgr.hpp -------
const uint64_t LogBufferMgrSize = 0.001 * (1024ULL * 1024ULL * 1024ULL); // pagecache: 6.4G?? (32 * 0.2)
const uint64_t LogBufferMgrHeaderSize = 4ULL + 4ULL; // LogbufferMgrHeader记录目前已经落盘(pmem)了多少条记录，以及正在落盘哪个bufferlog
const uint64_t LogBufferMgrBody = LogBufferMgrSize - LogBufferMgrHeaderSize;

const uint64_t LogBufferHeaderSize = 8ULL; // 当前buffer log已经commit了多少条记录
const uint64_t LogBufferBodySize = 272ULL * 256ULL * 0.25 * 0.25; // (272 * 256) LogBufferSize = LCM(272, 256), 是倍数即可，当每个buffer足够大时，不是倍数应该影响也不大
const uint64_t LogBufferSize = LogBufferHeaderSize + LogBufferBodySize;

const uint64_t LogBufferFreeNumPerBufferMgr = LogBufferMgrBody / LogBufferSize; 

// unused
const uint64_t LogBufferMgrRemainSize = LogBufferMgrBody % LogBufferSize; // todo(wq): 尽量不要让mmap(LogBufferMgrSize)有剩余

const uint32_t FlushingNone = 0x00000000UL;
```
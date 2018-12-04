/// This file is generated from generate.py.
/// Re-generate it if you upgrade to a new version of RocksDB.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBStatisticsTickerType {
    BlockCacheMiss = 0,
    BlockCacheHit = 1,
    BlockCacheAdd = 2,
    BlockCacheAddFailures = 3,
    BlockCacheIndexMiss = 4,
    BlockCacheIndexHit = 5,
    BlockCacheIndexAdd = 6,
    BlockCacheIndexBytesInsert = 7,
    BlockCacheIndexBytesEvict = 8,
    BlockCacheFilterMiss = 9,
    BlockCacheFilterHit = 10,
    BlockCacheFilterAdd = 11,
    BlockCacheFilterBytesInsert = 12,
    BlockCacheFilterBytesEvict = 13,
    BlockCacheDataMiss = 14,
    BlockCacheDataHit = 15,
    BlockCacheDataAdd = 16,
    BlockCacheDataBytesInsert = 17,
    BlockCacheBytesRead = 18,
    BlockCacheBytesWrite = 19,
    BloomFilterUseful = 20,
    BloomFilterFullPositive = 21,
    BloomFilterFullTruePositive = 22,
    PersistentCacheHit = 23,
    PersistentCacheMiss = 24,
    SimBlockCacheHit = 25,
    SimBlockCacheMiss = 26,
    MemtableHit = 27,
    MemtableMiss = 28,
    GetHitL0 = 29,
    GetHitL1 = 30,
    GetHitL2AndUp = 31,
    CompactionKeyDropNewerEntry = 32,
    CompactionKeyDropObsolete = 33,
    CompactionKeyDropRangeDel = 34,
    CompactionKeyDropUser = 35,
    CompactionRangeDelDropObsolete = 36,
    CompactionOptimizedDelDropObsolete = 37,
    CompactionCancelled = 38,
    NumberKeysWritten = 39,
    NumberKeysRead = 40,
    NumberKeysUpdated = 41,
    BytesWritten = 42,
    BytesRead = 43,
    NumberDbSeek = 44,
    NumberDbNext = 45,
    NumberDbPrev = 46,
    NumberDbSeekFound = 47,
    NumberDbNextFound = 48,
    NumberDbPrevFound = 49,
    IterBytesRead = 50,
    NoFileCloses = 51,
    NoFileOpens = 52,
    NoFileErrors = 53,
    StallL0SlowdownMicros = 54,
    StallMemtableCompactionMicros = 55,
    StallL0NumFilesMicros = 56,
    StallMicros = 57,
    DbMutexWaitMicros = 58,
    RateLimitDelayMillis = 59,
    NoIterators = 60,
    NumberMultigetCalls = 61,
    NumberMultigetKeysRead = 62,
    NumberMultigetBytesRead = 63,
    NumberFilteredDeletes = 64,
    NumberMergeFailures = 65,
    BloomFilterPrefixChecked = 66,
    BloomFilterPrefixUseful = 67,
    NumberOfReseeksInIteration = 68,
    GetUpdatesSinceCalls = 69,
    BlockCacheCompressedMiss = 70,
    BlockCacheCompressedHit = 71,
    BlockCacheCompressedAdd = 72,
    BlockCacheCompressedAddFailures = 73,
    WalFileSynced = 74,
    WalFileBytes = 75,
    WriteDoneBySelf = 76,
    WriteDoneByOther = 77,
    WriteTimedout = 78,
    WriteWithWal = 79,
    CompactReadBytes = 80,
    CompactWriteBytes = 81,
    FlushWriteBytes = 82,
    NumberDirectLoadTableProperties = 83,
    NumberSuperversionAcquires = 84,
    NumberSuperversionReleases = 85,
    NumberSuperversionCleanups = 86,
    NumberBlockCompressed = 87,
    NumberBlockDecompressed = 88,
    NumberBlockNotCompressed = 89,
    MergeOperationTotalTime = 90,
    FilterOperationTotalTime = 91,
    RowCacheHit = 92,
    RowCacheMiss = 93,
    ReadAmpEstimateUsefulBytes = 94,
    ReadAmpTotalReadBytes = 95,
    NumberRateLimiterDrains = 96,
    NumberIterSkip = 97,
    BlobDbNumPut = 98,
    BlobDbNumWrite = 99,
    BlobDbNumGet = 100,
    BlobDbNumMultiget = 101,
    BlobDbNumSeek = 102,
    BlobDbNumNext = 103,
    BlobDbNumPrev = 104,
    BlobDbNumKeysWritten = 105,
    BlobDbNumKeysRead = 106,
    BlobDbBytesWritten = 107,
    BlobDbBytesRead = 108,
    BlobDbWriteInlined = 109,
    BlobDbWriteInlinedTtl = 110,
    BlobDbWriteBlob = 111,
    BlobDbWriteBlobTtl = 112,
    BlobDbBlobFileBytesWritten = 113,
    BlobDbBlobFileBytesRead = 114,
    BlobDbBlobFileSynced = 115,
    BlobDbBlobIndexExpiredCount = 116,
    BlobDbBlobIndexExpiredSize = 117,
    BlobDbBlobIndexEvictedCount = 118,
    BlobDbBlobIndexEvictedSize = 119,
    BlobDbGcNumFiles = 120,
    BlobDbGcNumNewFiles = 121,
    BlobDbGcFailures = 122,
    BlobDbGcNumKeysOverwritten = 123,
    BlobDbGcNumKeysExpired = 124,
    BlobDbGcNumKeysRelocated = 125,
    BlobDbGcBytesOverwritten = 126,
    BlobDbGcBytesExpired = 127,
    BlobDbGcBytesRelocated = 128,
    BlobDbFifoNumFilesEvicted = 129,
    BlobDbFifoNumKeysEvicted = 130,
    BlobDbFifoBytesEvicted = 131,
    TxnPrepareMutexOverhead = 132,
    TxnOldCommitMapMutexOverhead = 133,
    TxnDuplicateKeyOverhead = 134,
    TxnSnapshotMutexOverhead = 135,
    NumberMultigetKeysFound = 136,
}
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBStatisticsHistogramType {
    DbGet = 0,
    DbWrite = 1,
    CompactionTime = 2,
    SubcompactionSetupTime = 3,
    TableSyncMicros = 4,
    CompactionOutfileSyncMicros = 5,
    WalFileSyncMicros = 6,
    ManifestFileSyncMicros = 7,
    TableOpenIoMicros = 8,
    DbMultiget = 9,
    ReadBlockCompactionMicros = 10,
    ReadBlockGetMicros = 11,
    WriteRawBlockMicros = 12,
    StallL0SlowdownCount = 13,
    StallMemtableCompactionCount = 14,
    StallL0NumFilesCount = 15,
    HardRateLimitDelayCount = 16,
    SoftRateLimitDelayCount = 17,
    NumFilesInSingleCompaction = 18,
    DbSeek = 19,
    WriteStall = 20,
    SstReadMicros = 21,
    NumSubcompactionsScheduled = 22,
    BytesPerRead = 23,
    BytesPerWrite = 24,
    BytesPerMultiget = 25,
    BytesCompressed = 26,
    BytesDecompressed = 27,
    CompressionTimesNanos = 28,
    DecompressionTimesNanos = 29,
    ReadNumMergeOperands = 30,
    BlobDbKeySize = 31,
    BlobDbValueSize = 32,
    BlobDbWriteMicros = 33,
    BlobDbGetMicros = 34,
    BlobDbMultigetMicros = 35,
    BlobDbSeekMicros = 36,
    BlobDbNextMicros = 37,
    BlobDbPrevMicros = 38,
    BlobDbBlobFileWriteMicros = 39,
    BlobDbBlobFileReadMicros = 40,
    BlobDbBlobFileSyncMicros = 41,
    BlobDbGcMicros = 42,
    BlobDbCompressionMicros = 43,
    BlobDbDecompressionMicros = 44,
    FlushTime = 45,
    HistogramEnumMax = 46,
}

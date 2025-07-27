// Copyright (c) 2024-present, Topling, Inc.  All rights reserved.
// Created by leipeng, fully rewrite by leipeng 2024-12-18
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "options/db_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include <logging/logging.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <utilities/write_batch_with_index/write_batch_with_index_internal.h>
#include <topling/side_plugin_factory.h>
namespace ROCKSDB_NAMESPACE {
using namespace terark;
struct PairCheckWBWIFactory;
template<class T>
T& ConstCast(const T& x) { return const_cast<T&>(x); }
struct PairCheckWBWI : public WriteBatchWithIndex {
  PairCheckWBWI() : WriteBatchWithIndex(Slice()) // default cons placeholder with Slice
  {
  }
  WriteBatchWithIndex *a, *b;
  PairCheckWBWIFactory* m_fac;
  ~PairCheckWBWI() noexcept override;
  const Comparator* GetUserComparator(uint32_t cf_id) const final {
    auto ac = a->GetUserComparator(cf_id);
    auto bc = a->GetUserComparator(cf_id);
    ROCKSDB_VERIFY_EQ(ac, bc);
    return ac;
  }
  WriteBatch* GetWriteBatch() final {
    return a->GetWriteBatch();
  }
  void Clear() final {
    a->Clear();
    b->Clear();
  }
  // OOB: Out-Of-Bound
  // rocksdb use WriteBatchInternal to write OOB data by GetWriteBatch(),
  // we can not catch that OOB data, so use SyncBatchAB() to sync it.
  void SyncBatchAB() {
    WriteBatch* wa = a->GetWriteBatch();
    WriteBatch* wb = b->GetWriteBatch();
    size_t a_data_size = wa->GetDataSize();
    size_t b_data_size = wb->GetDataSize();
    auto& a_mut = ConstCast(wa->Data());
    auto& b_mut = ConstCast(wb->Data());
    if (b_data_size < a_data_size) {
      size_t len = a_data_size - b_data_size;
      b_mut.append(a_mut.data() + b_data_size, len);
    }
    if (b_data_size >= 13) { // 13 is header(12) + noop(1)
      // for prepared txn, rocksdb will update the header
      if (memcmp(a_mut.data(), b_mut.data(), 13) != 0) {
        memcpy(b_mut.data(), a_mut.data(), 13);
      }
    }
  }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  using WriteBatchWithIndex::Put;
  Status Put(ColumnFamilyHandle* cfh, const Slice& key, const Slice& value) final {
    SyncBatchAB();
    Status sa = a->Put(cfh, key, value);
    Status sb = b->Put(cfh, key, value);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status Put(const Slice& key, const Slice& value) final {
    SyncBatchAB();
    Status sa = a->Put(key, value);
    Status sb = b->Put(key, value);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 80100
  using WriteBatchWithIndex::PutEntity;
  Status PutEntity(ColumnFamilyHandle* cfh, const Slice& key,
                   const WideColumns& columns) override {
    SyncBatchAB();
    Status sa = a->PutEntity(cfh, key, columns);
    Status sb = b->PutEntity(cfh, key, columns);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
#endif
  using WriteBatchWithIndex::Delete;
  Status Delete(ColumnFamilyHandle* cfh, const Slice& key) final {
    SyncBatchAB();
    Status sa = a->Delete(cfh, key);
    Status sb = b->Delete(cfh, key);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status Delete(const Slice& key) final {
    SyncBatchAB();
    Status sa = a->Delete(key);
    Status sb = b->Delete(key);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  using WriteBatchWithIndex::SingleDelete;
  Status SingleDelete(ColumnFamilyHandle* cfh, const Slice& key) final {
    SyncBatchAB();
    Status sa = a->SingleDelete(cfh, key);
    Status sb = b->SingleDelete(cfh, key);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status SingleDelete(const Slice& key) final {
    SyncBatchAB();
    Status sa = a->SingleDelete(key);
    Status sb = b->SingleDelete(key);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  using WriteBatchWithIndex::Merge;
  Status Merge(ColumnFamilyHandle* cfh, const Slice& key, const Slice& value) final {
    SyncBatchAB();
    Status sa = a->Merge(cfh, key, value);
    Status sb = b->Merge(cfh, key, value);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status Merge(const Slice& key, const Slice& value) final {
    SyncBatchAB();
    Status sa = a->Merge(key, value);
    Status sb = b->Merge(key, value);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status PutLogData(const Slice& blob) final {
    SyncBatchAB();
    Status sa = a->PutLogData(blob);
    Status sb = b->PutLogData(blob);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  void SetSavePoint() final {
    SyncBatchAB();
    a->SetSavePoint();
    b->SetSavePoint();
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
  }
  Status RollbackToSavePoint() final {
    SyncBatchAB();
    Status sa = a->RollbackToSavePoint();
    Status sb = b->RollbackToSavePoint();
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  Status PopSavePoint() final {
    SyncBatchAB();
    Status sa = a->PopSavePoint();
    Status sb = b->PopSavePoint();
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    size_t a_data_size = a->GetDataSize();
    size_t b_data_size = b->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    return sa;
  }
  void SetMaxBytes(size_t max_bytes) final {
    SyncBatchAB();
    a->SetMaxBytes(max_bytes);
    b->SetMaxBytes(max_bytes);
  }
  size_t GetDataSize() const final {
    const_cast<PairCheckWBWI*>(this)->SyncBatchAB();
    return a->GetDataSize();
  }
  size_t SubBatchCnt() final {
    const_cast<PairCheckWBWI*>(this)->SyncBatchAB();
    size_t sa = a->SubBatchCnt();
    size_t sb = b->SubBatchCnt();
    ROCKSDB_VERIFY_EQ(sa, sb);
    return sa;
  }

  Status GetFromBatch(ColumnFamilyHandle* cfh, const DBOptions& options,
                      const Slice& key, std::string* value) override {
    const_cast<PairCheckWBWI*>(this)->SyncBatchAB();
    std::string va, vb;
    Status sa = a->GetFromBatch(cfh, options, key, &va);
    Status sb = b->GetFromBatch(cfh, options, key, &vb);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    ROCKSDB_VERIFY_EQ(va.size(), vb.size());
    ROCKSDB_VERIFY(va == vb);
    return a->GetFromBatch(cfh, options, key, value);
  }

  // semantically same with WriteBatchWithIndexInternal::GetFromBatch
  WBWIIterator::Result
  GetFromBatchRaw(DB* db, ColumnFamilyHandle* cfh, const Slice& key,
                  MergeContext* mgcontext, std::string* value, Status* s)
  override {
    const_cast<PairCheckWBWI*>(this)->SyncBatchAB();
    MergeContext ma, mb;
    std::string va, vb;
    Status sa, sb;
    auto ra = a->GetFromBatchRaw(db, cfh, key, &ma, &va, &sa);
    auto rb = b->GetFromBatchRaw(db, cfh, key, &mb, &vb, &sb);
    ROCKSDB_VERIFY_EQ(ra, rb);
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    ROCKSDB_VERIFY_EQ(va.size(), vb.size());
    ROCKSDB_VERIFY(va == vb);
    auto& oa = ma.GetOperands();
    auto& ob = mb.GetOperands();
    for (size_t i = 0; i < oa.size(); i++) {
      ROCKSDB_VERIFY_EQ(oa[i].size(), ob[i].size());
      ROCKSDB_VERIFY(oa[i] == ob[i]);
    }
    return a->GetFromBatchRaw(db, cfh, key, mgcontext, value, s);
  }

  Status GetFromBatchAndDB(DB* db, const ReadOptions& ro,
                           ColumnFamilyHandle* cfh, const Slice& key,
                           PinnableSlice* pinnable_val, ReadCallback* callback)
  override {
    const_cast<PairCheckWBWI*>(this)->SyncBatchAB();
    // callback may be stateful and can not be called multi times.
    // just forward to a!
    return a->GetFromBatchAndDB(db, ro, cfh, key, pinnable_val, callback);
  }

  WBWIIterator* NewIterator(ColumnFamilyHandle* column_family) final;
  WBWIIterator* NewIterator() final;
  Iterator* NewIteratorWithBase(ColumnFamilyHandle*, Iterator* base,
                                const ReadOptions*) final;
  Iterator* NewIteratorWithBase(Iterator* base) final; // default cf
  struct Iter;
};

struct PairCheckWBWI::Iter : public WBWIIterator, boost::noncopyable {
  WBWIIterator *ia, *ib;
  Iter(WBWIIterator* a, WBWIIterator* b) : ia(a), ib(b) {}
  ~Iter();
  Status status() const final {
    Status sa = ia->status();
    Status sb = ib->status();
    ROCKSDB_VERIFY_EQ(sa.code(), sb.code());
    return sa;
  }
  bool Valid() const final {
    bool va = ia->Valid();
    bool vb = ib->Valid();
    ROCKSDB_VERIFY_EQ(va, vb);
    return va;
  }
  WriteEntry Entry() const final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    WriteEntry wa = ia->Entry();
    WriteEntry wb = ib->Entry();
    ROCKSDB_VERIFY_EQ(wa.type, wb.type);
    ROCKSDB_VERIFY(wa.key == wb.key);
    ROCKSDB_VERIFY(wa.value == wb.value);
    return wa;
  }
  Slice user_key() const final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    Slice ua = ia->user_key();
    Slice ub = ib->user_key();
    ROCKSDB_VERIFY(ua == ub);
    return ua;
  }
  void Next() final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    ia->Next();
    ib->Next();
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  void Prev() final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    ia->Prev();
    ib->Prev();
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  void Seek(const Slice& userkey) final {
    ia->Seek(userkey);
    ib->Seek(userkey);
    bool a_valid = ia->Valid();
    bool b_valid = ib->Valid();
    if (a_valid != b_valid) {
      // for debug, call Seek multi times is ok
      ia->Seek(userkey);
      ib->Seek(userkey);
    }
    if (a_valid) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  void SeekForPrev(const Slice& userkey) final {
    ia->SeekForPrev(userkey);
    ib->SeekForPrev(userkey);
    bool a_valid = ia->Valid();
    bool b_valid = ib->Valid();
    if (a_valid != b_valid) {
      // for debug, call SeekForPrev multi times is ok
      ia->SeekForPrev(userkey);
      ib->SeekForPrev(userkey);
    }
    if (a_valid) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  void SeekToFirst() final {
    ia->SeekToFirst();
    ib->SeekToFirst();
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  void SeekToLast() final {
    ia->SeekToLast();
    ib->SeekToLast();
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
  }
  // Moves the iterator to first entry of the previous key.
  bool PrevKey() final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    bool ra = ia->PrevKey();
    bool rb = ib->PrevKey();
    ROCKSDB_VERIFY_EQ(ra, rb);
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
    return ra;
  }
  // Moves the iterator to first entry of the next key.
  bool NextKey() final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    bool ra = ia->PrevKey();
    bool rb = ib->PrevKey();
    ROCKSDB_VERIFY_EQ(ra, rb);
    ROCKSDB_VERIFY_EQ(ia->Valid(), ib->Valid());
    if (ia->Valid()) {
      Slice ua = ia->user_key(), va = ia->value();
      Slice ub = ib->user_key(), vb = ib->value();
      ROCKSDB_VERIFY(ua == ub);
      ROCKSDB_VERIFY(va == vb);
    }
    return ra;
  }
  bool EqualsKey(const Slice& key) const final {
    ROCKSDB_VERIFY(ia->Valid());
    ROCKSDB_VERIFY(ib->Valid());
    bool ra = ia->EqualsKey(key);
    bool rb = ib->EqualsKey(key);
    ROCKSDB_VERIFY_EQ(ra, rb);
    return ra;
  }
  Result FindLatestUpdate(const Slice& key, MergeContext* mgctx) final {
    MergeContext mb;
    auto ra = ia->FindLatestUpdate(key, mgctx);
    auto rb = ia->FindLatestUpdate(key, &mb);
    ROCKSDB_VERIFY_EQ(ra, rb);
    auto& oa = mgctx->GetOperands();
    auto& ob = mb.GetOperands();
    for (size_t i = 0; i < oa.size(); i++) {
      ROCKSDB_VERIFY_EQ(oa[i].size(), ob[i].size());
      ROCKSDB_VERIFY(oa[i] == ob[i]);
    }
    return ra;
  }
  Result FindLatestUpdate(MergeContext* mgctx) final {
    MergeContext mb;
    auto ra = ia->FindLatestUpdate(mgctx);
    auto rb = ia->FindLatestUpdate(&mb);
    ROCKSDB_VERIFY_EQ(ra, rb);
    auto& oa = mgctx->GetOperands();
    auto& ob = mb.GetOperands();
    for (size_t i = 0; i < oa.size(); i++) {
      ROCKSDB_VERIFY_EQ(oa[i].size(), ob[i].size());
      ROCKSDB_VERIFY(oa[i] == ob[i]);
    }
    return ra;
  }
};

WBWIIterator* PairCheckWBWI::NewIterator(ColumnFamilyHandle* cfh) {
  SyncBatchAB();
  auto ia = a->NewIterator(cfh);
  auto ib = b->NewIterator(cfh);
  return new Iter{ia, ib};
}
WBWIIterator* PairCheckWBWI::NewIterator() {
  SyncBatchAB();
  auto ia = a->NewIterator();
  auto ib = b->NewIterator();
  return new Iter{ia, ib};
}
Iterator* PairCheckWBWI::NewIteratorWithBase(
    ColumnFamilyHandle* cfh, Iterator* base,
    const ReadOptions* ro) {
  SyncBatchAB();
  auto ia = dynamic_cast<BaseDeltaIterator*>(a->NewIteratorWithBase(cfh, base, ro));
  auto ib = dynamic_cast<BaseDeltaIterator*>(b->NewIteratorWithBase(cfh, base, ro));
  auto wbwiii = new Iter{ia->GetDeltaIter().release(), ib->GetDeltaIter().release()};
  auto cmp = ia->GetComparator();
  ia->GetBaseIter().release();
  ib->GetBaseIter().release();
  delete static_cast<Iterator*>(ib);
  delete static_cast<Iterator*>(ia);
  return new BaseDeltaIterator(cfh, base, wbwiii, cmp, ro);
}
Iterator* PairCheckWBWI::NewIteratorWithBase(Iterator* base) {
  SyncBatchAB();
  // default column family's comparator
  auto ia = dynamic_cast<BaseDeltaIterator*>(a->NewIteratorWithBase(base));
  auto ib = dynamic_cast<BaseDeltaIterator*>(b->NewIteratorWithBase(base));
  auto wbwiii = new Iter{ia->GetDeltaIter().release(), ib->GetDeltaIter().release()};
  auto cmp = ia->GetComparator();
  ia->GetBaseIter().release();
  ib->GetBaseIter().release();
  delete static_cast<Iterator*>(ib);
  delete static_cast<Iterator*>(ia);
  return new BaseDeltaIterator(nullptr, base, wbwiii, cmp);
}
struct PairCheckWBWIFactory final : public WBWIFactory {
  std::shared_ptr<WBWIFactory> a, b;
  PairCheckWBWIFactory(const json& js, const SidePluginRepo& r) { Update({}, js, r); }
  WriteBatchWithIndex*
  NewWriteBatchWithIndex(const Comparator* cmp, bool overwrite_key
 #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
  , size_t prot
 #endif
  ) final {
    WriteBatchWithIndex *wa, *wb;
    wa = a->NewWriteBatchWithIndex(cmp, overwrite_key, prot);
    wb = b->NewWriteBatchWithIndex(cmp, overwrite_key, prot);
    size_t a_data_size = wa->GetDataSize();
    size_t b_data_size = wb->GetDataSize();
    ROCKSDB_VERIFY_EQ(a_data_size, b_data_size);
    auto p = new PairCheckWBWI;
    p->a = wa;
    p->b = wb;
    p->m_fac = this;
    return p;
  }
  const char *Name() const noexcept final { return "PairCheckWBWI"; }
  void Update(const json&, const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_OPT_FACT(js, a);
    ROCKSDB_JSON_OPT_FACT(js, b);
  }
  std::string ToString(const json& d, const SidePluginRepo& repo) const {
    bool html = JsonSmartBool(d, "html");
    json djs;
    ROCKSDB_JSON_SET_FACX(djs, a, wbwi_factory);
    ROCKSDB_JSON_SET_FACX(djs, b, wbwi_factory);
    return JsonToString(djs, d);
  }
};
PairCheckWBWI::Iter::~Iter() noexcept {
  delete ia;
  delete ib;
}
PairCheckWBWI::~PairCheckWBWI() noexcept {
  delete a;
  delete b;
}
ROCKSDB_REG_Plugin("PairCheckWBWI", PairCheckWBWIFactory, WBWIFactory);
ROCKSDB_REG_EasyProxyManip("PairCheckWBWI", PairCheckWBWIFactory, WBWIFactory);

} // ROCKSDB_NAMESPACE

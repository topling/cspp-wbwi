// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// Created by leipeng, fully rewrite by leipeng 2021-05-12
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "options/db_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include <rocksdb/utilities/write_batch_with_index.h>
#include <utilities/write_batch_with_index/write_batch_with_index_internal.h>
#include <topling/side_plugin_factory.h>
#include <terark/fsa/cspptrie.inl>
#include <terark/io/DataIO_Basic.hpp>
#include <terark/num_to_str.hpp>
const char* git_version_hash_info_cspp_wbwi();
namespace ROCKSDB_NAMESPACE {
using namespace terark;
// prepend bytewise(bigendian) cf_id on userkey
static fstring InitLookupKey(void* alloca_ptr, uint32_t cf_id, Slice userkey) {
  fstring lookup_key((char*)alloca_ptr, 4 + userkey.size_);
  aligned_save(alloca_ptr, BIG_ENDIAN_OF(cf_id));
  memcpy((char*)alloca_ptr + 4, userkey.data_, userkey.size_);
  return lookup_key;
}
#define DefineLookupKey(var, cf_id, userkey) \
  const fstring var = InitLookupKey(alloca(4 + userkey.size_), cf_id, userkey)
struct CSPP_WBWI_Factory;
struct CSPP_WBWI : public WriteBatchWithIndex {
  using Elem = uint32_t;
  struct VecNode {
    uint32_t num;
    uint32_t pos;
  };
  CSPP_WBWI_Factory*    m_fac;
  mutable MainPatricia  m_trie;
  mutable Patricia::SingleWriterToken m_wtoken;
  ReadableWriteBatch    m_batch;
  bool     m_overwrite_key;
  uint32_t m_live_iter_num = 0;
  size_t   m_last_entry_offset = 0;
  size_t   m_last_sub_batch_offset = 0;
  size_t   m_sub_batch_cnt = 1;
  CSPP_WBWI(CSPP_WBWI_Factory*, bool overwrite_key);
  ~CSPP_WBWI() noexcept override;
  void SetLastEntryOffset() {
    m_last_entry_offset = m_batch.GetDataSize();
  }
  const Comparator* GetUserComparator(uint32_t cf_id) const final {
      return BytewiseComparator();
  }
  void AddOrUpdateIndexCFH(ColumnFamilyHandle* cfh, WriteType type) {
    if (cfh) {
      ROCKSDB_ASSERT_F(!cfh->GetComparator() ||
               IsForwardBytewiseComparator(cfh->GetComparator()),
          "Name() = %s", cfh->GetComparator()->Name());
      AddOrUpdateIndex(cfh->GetID(), type);
    } else {
      AddOrUpdateIndex(0, type);
    }
  }
  void AddOrUpdateIndex(uint32_t cf_id, WriteType type) {
    size_t offset = m_last_entry_offset;
    Slice raw_entry = Slice(m_batch.Data()).substr(offset);
    Slice userkey;
    bool success __attribute__((unused)) =
        ReadKeyFromWriteBatchEntry(&raw_entry, &userkey, cf_id != 0);
    assert(success);
    DefineLookupKey(lookup_key, cf_id, userkey);
    VecNode vn = {0,0};
    if (m_trie.insert(lookup_key, &vn, &m_wtoken)) {
      vn.num = 1;
      vn.pos = m_trie.mem_alloc(sizeof(Elem));
      *(Elem*)m_trie.mem_get(vn.pos) = Elem(offset);
      m_wtoken.mutable_value_of<VecNode>() = vn;
    }
    else { // dup key, append on vector or overwirte last vector elem
      vn = m_wtoken.value_of<VecNode>();
      auto vec = (Elem*)m_trie.mem_get(vn.pos);
      if (LIKELY(m_last_sub_batch_offset <= vec[vn.num-1])) {
        m_last_sub_batch_offset = m_last_entry_offset;
        m_sub_batch_cnt++;
      }
      if (m_overwrite_key && kMergeRecord != type) {
        vec[vn.num-1] = Elem(offset); // overwrite
      }
      else {
        if (vn.num & (vn.num-1)) { // is not power of 2, has space
          vec[vn.num] = Elem(offset);
          m_wtoken.mutable_value_of<VecNode>().num = vn.num + 1;
        }
        else {
          size_t oldlen = sizeof(Elem) * vn.num;
          size_t newlen = sizeof(Elem) * vn.num * 2;
          size_t newpos = m_trie.mem_alloc3(vn.pos, oldlen, newlen);
          vn.pos = (uint32_t)newpos;
          vec = (Elem*)m_trie.mem_get(newpos);
          vec[vn.num++] = Elem(offset);
          m_wtoken.mutable_value_of<VecNode>() = vn;
        }
      }
    }
  }
  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() final { return &m_batch; }
  void Clear() {
    m_batch.Clear();
    ClearIndex();
  }
  void ClearIndex();
  static WriteType WriteTypeOf(ValueType op) {
    switch (op) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        return kPutRecord;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        return kDeleteRecord;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        return kSingleDeleteRecord;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        return kMergeRecord;
      case kTypeLogData:
        return kLogDataRecord;
      case kTypeBeginPrepareXID:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeCommitXIDAndTimestamp:
      case kTypeRollbackXID:
      case kTypeNoop:
        //return kXIDRecord;
      default:
        return kUnknownRecord;
    }
  }
  Status ReBuildIndex() {
    Status s;
    ClearIndex();
    if (m_batch.Count() == 0) {
      // Nothing to re-index
      return s;
    }
    size_t offset = WriteBatchInternal::GetFirstOffset(&m_batch);
    Slice input(m_batch.Data());
    input.remove_prefix(offset);
    size_t found = 0;
    while (s.ok() && !input.empty()) {
      Slice key, value, blob, xid;
      uint32_t cf_id = 0;  // default
      char tag = 0;
      m_last_entry_offset = input.data() - m_batch.Data().data();
      s = ReadRecordFromWriteBatch(&input, &tag, &cf_id, &key,
                                    &value, &blob, &xid);
      if (!s.ok()) {
        break;
      }
      switch (tag) {
        case kTypeColumnFamilyValue:
        case kTypeValue:
          found++;
          AddOrUpdateIndex(cf_id, kPutRecord);
          break;
        case kTypeColumnFamilyDeletion:
        case kTypeDeletion:
          found++;
          AddOrUpdateIndex(cf_id, kDeleteRecord);
          break;
        case kTypeColumnFamilySingleDeletion:
        case kTypeSingleDeletion:
          found++;
          AddOrUpdateIndex(cf_id, kSingleDeleteRecord);
          break;
        case kTypeColumnFamilyMerge:
        case kTypeMerge:
          found++;
          AddOrUpdateIndex(cf_id, kMergeRecord);
          break;
        case kTypeLogData:
        case kTypeBeginPrepareXID:
        case kTypeBeginPersistedPrepareXID:
        case kTypeBeginUnprepareXID:
        case kTypeEndPrepareXID:
        case kTypeCommitXID:
        case kTypeCommitXIDAndTimestamp:
        case kTypeRollbackXID:
        case kTypeNoop:
          break;
        default:
          return Status::Corruption("unknown WriteBatch tag in ReBuildIndex",
                                    ToString(static_cast<unsigned int>(tag)));
      }
    }
    if (s.ok() && found != m_batch.Count()) {
      s = Status::Corruption("WriteBatch has wrong count");
    }
    return s;
  }
  struct OneRecord {
    ValueType tag;
    WriteType type;
    uint32_t cf_id;
    Slice key, value, blob, xid;
  };
  OneRecord ReadRecord(size_t offset) const {
    Slice input = Slice(m_batch.Data()).substr(offset);
    OneRecord r;
    static_assert(sizeof(r.tag) == 1);
    Status s = ReadRecordFromWriteBatch(&input, (char*)&r.tag, &r.cf_id, &r.key,
                                        &r.value, &r.blob, &r.xid);
    TERARK_VERIFY_S(s.ok(), "%s", s.ToString());
    r.type = WriteTypeOf(r.tag);
    return r;
  }
  using WriteBatchWithIndex::Put;
  Status Put(ColumnFamilyHandle* cfh, const Slice& key, const Slice& value) final {
    SetLastEntryOffset();
    auto s = m_batch.Put(cfh, key, value);
    if (s.ok()) {
      AddOrUpdateIndexCFH(cfh, kPutRecord);
    }
    return s;
  }
  Status Put(const Slice& key, const Slice& value) final {
    SetLastEntryOffset();
    auto s = m_batch.Put(key, value);
    if (s.ok()) {
      AddOrUpdateIndex(0, kPutRecord);
    }
    return s;
  }
  using WriteBatchWithIndex::Delete;
  Status Delete(ColumnFamilyHandle* cfh, const Slice& key) final {
    SetLastEntryOffset();
    auto s = m_batch.Delete(cfh, key);
    if (s.ok()) {
      AddOrUpdateIndexCFH(cfh, kDeleteRecord);
    }
    return s;
  }
  Status Delete(const Slice& key) final {
    SetLastEntryOffset();
    auto s = m_batch.Delete(key);
    if (s.ok()) {
      AddOrUpdateIndex(0, kDeleteRecord);
    }
    return s;
  }
  using WriteBatchWithIndex::SingleDelete;
  Status SingleDelete(ColumnFamilyHandle* cfh, const Slice& key) final {
    SetLastEntryOffset();
    auto s = m_batch.SingleDelete(cfh, key);
    if (s.ok()) {
      AddOrUpdateIndexCFH(cfh, kSingleDeleteRecord);
    }
    return s;
  }
  Status SingleDelete(const Slice& key) final {
    SetLastEntryOffset();
    auto s = m_batch.SingleDelete(key);
    if (s.ok()) {
      AddOrUpdateIndex(0, kSingleDeleteRecord);
    }
    return s;
  }
  using WriteBatchWithIndex::Merge;
  Status Merge(ColumnFamilyHandle* cfh, const Slice& key, const Slice& value) final {
    SetLastEntryOffset();
    auto s = m_batch.Merge(cfh, key, value);
    if (s.ok()) {
      AddOrUpdateIndexCFH(cfh, kMergeRecord);
    }
    return s;
  }
  Status Merge(const Slice& key, const Slice& value) final {
    SetLastEntryOffset();
    auto s = m_batch.Merge(key, value);
    if (s.ok()) {
      AddOrUpdateIndex(0, kMergeRecord);
    }
    return s;
  }
  Status PutLogData(const Slice& blob) final {
    return m_batch.PutLogData(blob);
  }
  void SetSavePoint() final { m_batch.SetSavePoint(); }
  Status RollbackToSavePoint() final {
    Status s = m_batch.RollbackToSavePoint();
    if (s.ok()) {
      m_sub_batch_cnt = 1;
      m_last_sub_batch_offset = 0;
      s = ReBuildIndex();
    }
    return s;
  }
  Status PopSavePoint() final { return m_batch.PopSavePoint(); }
  void SetMaxBytes(size_t max_bytes) final { m_batch.SetMaxBytes(max_bytes); }
  size_t GetDataSize() const final { return m_batch.GetDataSize(); }
  WBWIIterator* NewIterator(ColumnFamilyHandle* column_family) final;
  WBWIIterator* NewIterator() final;
  Iterator* NewIteratorWithBase(ColumnFamilyHandle*, Iterator* base,
                                const ReadOptions*) final;
  Iterator* NewIteratorWithBase(Iterator* base) final; // default cf
  struct Iter;
};
struct CSPP_WBWI::Iter : public WBWIIterator, boost::noncopyable {
  Patricia::Iterator* m_iter;
  CSPP_WBWI*  m_tab;
  uint32_t    m_cf_id = 0;
  int         m_idx = -1;
  int         m_num = 0;
  size_t      m_last_entry_offset;
  const Elem* m_vec = nullptr;
  OneRecord   m_rec;
  explicit Iter(CSPP_WBWI*, uint32_t cf_id);
  ~Iter() noexcept override;
  uint32_t iter_cf_id() const {
    auto bigendian_cf_id = *(const uint32_t*)m_iter->word().data();
    return BIG_ENDIAN_OF(bigendian_cf_id);
  }
  void SetFirstEntry() {
    auto vn = m_iter->value_of<VecNode>();
    m_idx = 0;
    m_num = vn.num;
    m_vec = (Elem*)m_tab->m_trie.mem_get(vn.pos);
    m_rec = m_tab->ReadRecord(m_vec[0]);
    assert(iter_cf_id() == m_cf_id);
    assert(m_iter->word().substr(4) == m_rec.key);
  }
  void SetLastEntry() {
    auto vn = m_iter->value_of<VecNode>();
    m_idx = vn.num - 1;
    m_num = vn.num;
    m_vec = (Elem*)m_tab->m_trie.mem_get(vn.pos);
    m_rec = m_tab->ReadRecord(m_vec[m_idx]);
    assert(iter_cf_id() == m_cf_id);
    assert(m_iter->word().substr(4) == m_rec.key);
  }
  Status status() const final { return Status::OK(); }
  bool Valid() const final { return m_idx >= 0; }
  template<bool UpdateRecordCache>
  terark_forceinline void CheckUpdates() {
    TERARK_ASSERT_GE(m_idx, 0);
    if (UNLIKELY(m_last_entry_offset != m_tab->m_last_entry_offset)) {
      // CSPP_WBWI has changed
      m_last_entry_offset = m_tab->m_last_entry_offset;
      const fstring src = m_iter->word();
      const fstring key((char*)memcpy(alloca(src.n), src.p, src.n), src.n);
      ROCKSDB_VERIFY(m_iter->seek_lower_bound(key));
      ROCKSDB_VERIFY(m_iter->word() == key);
      auto vn = m_iter->value_of<VecNode>();
      ROCKSDB_ASSERT_LE(m_num, int(vn.num));
      m_vec = (Elem*)m_tab->m_trie.mem_get(vn.pos);
      m_num = vn.num;
      if (UpdateRecordCache)
        m_rec = m_tab->ReadRecord(m_vec[m_idx]);
    }
  }
  WriteEntry Entry() const final {
    TERARK_ASSERT_BT(m_idx, 0, m_num);
    const_cast<Iter*>(this)->CheckUpdates<true>();
    return {m_rec.type, m_rec.key, m_rec.value};
  }
  void Next() final {
    TERARK_ASSERT_GE(m_idx, 0);
    CheckUpdates<false>();
    if (++m_idx == m_num) {
      if (UNLIKELY(!m_iter->incr() || iter_cf_id() != m_cf_id)) {
        m_idx = -1;
        return; // fail
      }
      auto vn = m_iter->value_of<VecNode>();
      m_idx = 0;
      m_num = vn.num;
      m_vec = (Elem*)m_tab->m_trie.mem_get(vn.pos);
    }
    m_rec = m_tab->ReadRecord(m_vec[m_idx]);
  }
  void Prev() final {
    TERARK_ASSERT_GE(m_idx, 0);
    CheckUpdates<false>();
    if (m_idx-- == 0) {
      if (UNLIKELY(!m_iter->decr() || iter_cf_id() != m_cf_id))
        return; // fail
      auto vn = m_iter->value_of<VecNode>();
      m_idx = vn.num - 1;
      m_num = vn.num;
      m_vec = (Elem*)m_tab->m_trie.mem_get(vn.pos);
    }
    m_rec = m_tab->ReadRecord(m_vec[m_idx]);
  }
  void Seek(const Slice& userkey) final {
    m_last_entry_offset = m_tab->m_last_entry_offset;
    DefineLookupKey(lookup_key, m_cf_id, userkey);
    if (UNLIKELY(!m_iter->seek_lower_bound(lookup_key))) {
      m_idx = -1;
      return; // fail
    }
    if (iter_cf_id() == m_cf_id)
      SetFirstEntry();
    else
      m_idx = -1;
  }
  void SeekForPrev(const Slice& userkey) final {
    m_last_entry_offset = m_tab->m_last_entry_offset;
    DefineLookupKey(lookup_key, m_cf_id, userkey);
    if (UNLIKELY(!m_iter->seek_rev_lower_bound(lookup_key))) {
      m_idx = -1;
      return; // fail
    }
    if (m_iter->word() == lookup_key)
      SetFirstEntry();
    else if (iter_cf_id() == m_cf_id)
      SetLastEntry();
    else
      m_idx = -1;
  }
  void SeekToFirst() final {
    m_last_entry_offset = m_tab->m_last_entry_offset;
    uint32_t big_cf_id = BIG_ENDIAN_OF(m_cf_id);
    fstring lookup_key((char*)&big_cf_id, 4);
    if (UNLIKELY(!m_iter->seek_lower_bound(lookup_key))) {
      m_idx = -1;
      return; // fail
    }
    if (UNLIKELY(*(uint32_t*)m_iter->word().data() != big_cf_id)) {
      m_idx = -1;
      return; // fail
    }
    SetFirstEntry();
  }
  void SeekToLast() final {
    m_last_entry_offset = m_tab->m_last_entry_offset;
    uint32_t big_next_cf_id = BIG_ENDIAN_OF(m_cf_id+1);
    fstring lookup_key((char*)&big_next_cf_id, 4);
    if (UNLIKELY(!m_iter->seek_rev_lower_bound(lookup_key))) {
      m_idx = -1;
      return; // fail
    }
    if (UNLIKELY(*(uint32_t*)m_iter->word().data() == big_next_cf_id)) {
      if (!m_iter->decr()) {
        m_idx = -1;
        return; // fail
      }
    }
    if (iter_cf_id() != m_cf_id) {
      ROCKSDB_ASSERT_LT(iter_cf_id(), m_cf_id);
      m_idx = -1;
      return; // fail
    }
    SetLastEntry();
  }
  // Moves the iterator to first entry of the previous key.
  void PrevKey() final {
    CheckUpdates<false>();
    TERARK_ASSERT_GE(m_idx, 0);
    if (UNLIKELY(!m_iter->decr() || iter_cf_id() != m_cf_id)) {
      m_idx = -1;
      return; // fail
    }
    SetFirstEntry();
  }
  // Moves the iterator to first entry of the next key.
  void NextKey() final {
    CheckUpdates<false>();
    TERARK_ASSERT_GE(m_idx, 0);
    if (UNLIKELY(!m_iter->incr() || iter_cf_id() != m_cf_id)) {
      m_idx = -1;
      return; // fail
    }
    SetFirstEntry();
  }
  bool EqualsKey(const Slice& key) const final {
    TERARK_ASSERT_GE(m_idx, 0);
    const_cast<Iter*>(this)->CheckUpdates<true>();
    return m_rec.key == key;
  }
};
WBWIIterator* CSPP_WBWI::NewIterator(ColumnFamilyHandle* cfh) {
  return new Iter(this, GetColumnFamilyID(cfh));
}
WBWIIterator* CSPP_WBWI::NewIterator() {
  return new Iter(this, 0);
}
Iterator* CSPP_WBWI::NewIteratorWithBase(
    ColumnFamilyHandle* cfh, Iterator* base,
    const ReadOptions* ro) {
  auto wbwiii = new Iter(this, GetColumnFamilyID(cfh));
  auto ucmp = GetColumnFamilyUserComparator(cfh);
  return new BaseDeltaIterator(cfh, base, wbwiii, ucmp, ro);
}
Iterator* CSPP_WBWI::NewIteratorWithBase(Iterator* base) {
  // default column family's comparator
  auto wbwiii = new Iter(this, 0);
  return new BaseDeltaIterator(nullptr, base, wbwiii, BytewiseComparator());
}
void JS_CSPP_WBWI_AddVersion(json& djs, bool html) {
  auto& ver = djs["version"];
  const char* git_ver = git_version_hash_info_cspp_wbwi();
  if (html) {
    std::string topling_rocks = HtmlEscapeMin(strstr(git_ver, "commit ") + strlen("commit "));
    auto headstr = [](const std::string& s, auto pos) {
      return terark::fstring(s.data(), pos - s.begin());
    };
    auto tailstr = [](const std::string& s, auto pos) {
      return terark::fstring(&*pos, s.end() - pos);
    };
    auto topling_rocks_sha_end = std::find_if(topling_rocks.begin(), topling_rocks.end(), &isspace);
    terark::string_appender<> oss_rocks;
    oss_rocks|"<pre>"
             |"<a href='https://github.com/topling/cspp-wbwi/commit/"
             |headstr(topling_rocks, topling_rocks_sha_end)|"'>"
             |headstr(topling_rocks, topling_rocks_sha_end)|"</a>"
             |tailstr(topling_rocks, topling_rocks_sha_end)
             |"</pre>";
    ver = static_cast<std::string&&>(oss_rocks);
  } else {
    ver = git_ver;
  }
}
ROCKSDB_ENUM_CLASS(HugePageEnum, uint8_t, kNone = 0, kMmap = 1, kTransparent = 2);
struct CSPP_WBWI_Factory final : public WriteBatchWithIndexFactory {
  intptr_t m_mem_cap = 64LL << 10;
  size_t cumu_num = 0, cumu_iter_num = 0;
  size_t live_num = 0, live_iter_num = 0;
  uint64_t cumu_used_mem = 0;
  CSPP_WBWI_Factory(const json& js, const SidePluginRepo& r) { Update({}, js, r); }
  WriteBatchWithIndex* NewWriteBatchWithIndex(
      const Comparator* default_comparator, bool overwrite_key) final {
    if (default_comparator) {
      ROCKSDB_VERIFY_F(IsForwardBytewiseComparator(default_comparator),
        "%s", default_comparator->Name());
    }
    return new CSPP_WBWI(this, overwrite_key);
  }
  const char *Name() const noexcept final { return "CSPP_WBWI_Factory"; }
//-----------------------------------------------------------------
  void Update(const json&, const json& js, const SidePluginRepo&) {
    size_t mem_cap = m_mem_cap;
    ROCKSDB_JSON_OPT_SIZE(js, mem_cap);
    m_mem_cap = std::max<intptr_t>(mem_cap, 64LL<<10);
  }
  std::string ToString(const json& d, const SidePluginRepo&) const {
    size_t mem_cap = m_mem_cap;
    auto avg_used_mem = cumu_num ? cumu_used_mem / cumu_num : 0;
    json djs;
    ROCKSDB_JSON_SET_SIZE(djs, mem_cap);
    ROCKSDB_JSON_SET_PROP(djs, cumu_num);
    ROCKSDB_JSON_SET_PROP(djs, live_num);
    ROCKSDB_JSON_SET_PROP(djs, cumu_iter_num);
    ROCKSDB_JSON_SET_PROP(djs, live_iter_num);
    ROCKSDB_JSON_SET_PROP(djs, live_iter_num);
    ROCKSDB_JSON_SET_SIZE(djs, avg_used_mem);
    ROCKSDB_JSON_SET_SIZE(djs, cumu_used_mem);
    JS_CSPP_WBWI_AddVersion(djs, JsonSmartBool(d, "html"));
    return JsonToString(djs, d);
  }
};
CSPP_WBWI::Iter::Iter(CSPP_WBWI* tab, uint32_t cf_id) {
  m_tab = tab;
  m_iter = tab->m_trie.new_iter();
  m_cf_id = cf_id;
  m_last_entry_offset = tab->m_last_entry_offset;
  as_atomic(tab->m_live_iter_num).fetch_add(1, std::memory_order_relaxed);
}
CSPP_WBWI::Iter::~Iter() noexcept {
  m_iter->dispose();
  auto factory = m_tab->m_fac;
  as_atomic(factory->live_iter_num).fetch_sub(1, std::memory_order_relaxed);
  as_atomic(m_tab->m_live_iter_num).fetch_sub(1, std::memory_order_relaxed);
}
CSPP_WBWI::CSPP_WBWI(CSPP_WBWI_Factory* f, bool overwrite_key)
    : WriteBatchWithIndex(Slice()) // default cons placeholder with Slice
    , m_trie(sizeof(VecNode), f->m_mem_cap, Patricia::SingleThreadStrict) {
  m_overwrite_key = overwrite_key;
  m_fac = f;
  m_wtoken.acquire(&m_trie);
  as_atomic(f->live_num).fetch_add(1, std::memory_order_relaxed);
  as_atomic(f->cumu_num).fetch_add(1, std::memory_order_relaxed);
}
CSPP_WBWI::~CSPP_WBWI() noexcept {
  m_wtoken.release();
  TERARK_ASSERT_EZ(m_live_iter_num);
  as_atomic(m_fac->live_num).fetch_sub(1, std::memory_order_relaxed);
}
void CSPP_WBWI::ClearIndex() {
  m_wtoken.release();
  m_wtoken.~SingleWriterToken();
  m_trie.~MainPatricia();
  new (&m_trie) MainPatricia(sizeof(VecNode), m_fac->m_mem_cap, Patricia::SingleThreadStrict);
  new (&m_wtoken) Patricia::SingleWriterToken();
  m_wtoken.acquire(&m_trie);
  m_last_entry_offset = 0;
  m_last_sub_batch_offset = 0;
  m_sub_batch_cnt = 1;
}
ROCKSDB_REG_JSON_REPO_CONS("CSPP_WBWI", CSPP_WBWI_Factory, WriteBatchWithIndexFactory);
ROCKSDB_REG_EasyProxyManip("CSPP_WBWI", CSPP_WBWI_Factory, WriteBatchWithIndexFactory);
WriteBatchWithIndexFactory* NewCSPP_WBWIForPlain(const std::string& jstr) {
  json js = json::parse(jstr);
  const SidePluginRepo repo;
  return new CSPP_WBWI_Factory(js, repo);
}
} // ROCKSDB_NAMESPACE

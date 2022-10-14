# cspp-wbwi

**CSPP** | **C**rash **S**afe **P**arallel **P**atricia trie
---------|-------------------
**WBWI** | **W**rite**B**atch**W**ithIndex

**CSPP_WBWI** is 10x+ faster than **SkipList**.

**CSPP_WBWI** just support **BytewiseComparator**.

# configuration
wbwi is defined in sideplugin namespace `WBWIFactory`.

class name is **CSPP_WBWI**.

name | type | default | description
-----|------|---------|---
allow_fallback|bool|false|when comparator is not BytewiseComparator fallback to SkipList
trie_reserve_cap|uint|64K|initial reserved capacity bytes for CSPP trie index
data_reserve_cap|uint|64K|initial reserved capacity bytes for `WriteBatch` internal buffer

# sample(json)
## definition
```json
  "WBWIFactory": {
    "cspp": {
      "class": "CSPP_WBWI",
      "params": {
        "trie_reserve_cap": "128K",
        "data_reserve_cap": "512K"
      }
    },
    "skiplist": {
      "class": "SkipList",
      "params": {}
    }
  }
```
## referencing
Referenced by `DBOptions`:

```json
 "DBOptions": {
    "dbopt": {
      "db_log_dir": "/nvme-shared/mytopling/log",
      "wbwi_factory": "${cspp}",
      "statistics": "${stat}"
    }
  },
 ```

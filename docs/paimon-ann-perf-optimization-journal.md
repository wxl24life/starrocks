# Paimon ANN 性能优化 — 决策与验证轨迹

最后更新：2026-06-07 11:00

> Stella 3.5.16 paimon global index ANN 查询性能优化主线日志。crash 修复完成后（见 `paimon-global-index-crash-deep-dive.md` v1.8），切回 perf 优化。本文档完整记录每一轮的**分析发现 → 决策依据 → 改动 → 验证结果**，供后续 review。

## 0. 总体目标

- **入口**：Fix A2 commit `8d51ef0eeb0` 的 cluster baseline（async + DataCache hit on positional + 0 HUAF）
- **现状基线**：cohere_1m c=5 qps=0.4286 / p99=12.9s；cohere_10m c=5 qps=0.3651 / p99=15.0s（来自 docs/paimon-global-index-crash-deep-dive.md §4.22，ASAN build #716，所以是低估）
- **目标（2026-06-07 11:10 用户确认）**：**在当前数据集上尽可能拉高 QPS，不考虑数据表 schema 设计上的不足**
- **范围圈定（含 vs 不含）**：

  | 优化                              | 含   | 理由                   |
  | ------------------------------- | --- | -------------------- |
  | 单 BE 内 CPU 节省（O3 CRC32 等）       | ✅   | 直接影响 hot BE 算力       |
  | 单 BE 内 mutex 争用                 | ✅   | 同上                   |
  | 单 BE 内 IO / 内存拷贝                | ✅   | 同上                   |
  | 单 BE 内 lumina / paimon-cpp 内部优化 | ✅   | 同上                   |
  | 改 `bucket=-1` 让多 BE 并行（O1/O2）   | ❌   | 数据表 schema 设计，不在范围   |
  | BE 副本 / 广播路由（O4）                | ❌   | 数据 placement 变更，不在范围 |

- **决策权限**：用户完全代为决策（2026-06-07 11:00 授权），独立判断改动方向，blocker 才上报

## 1. 已知 prior 发现（待 verify）

源：`docs/cohere-1m-perf-and-crash-analysis.md` v1.0（2026-06-06）

prior 报告主张：c=20 期间 BE 221 perf top 显示

- `starcache::DiskCache::_check_block_checksum` 占 ~24% CPU（read 路径 CRC32）
- 配置 flag `block_cache_checksum_enable=false` + `datacache_checksum_enable=false` **只覆盖 write/persist 路径，read 路径硬开**

**当前 audit 发现（2026-06-07 11:00）prior 主张可能不准**：

读源码 `stella-staros/starlet/src/starcachelib/src/disk_cache.cpp` 显示：

- Line 118 / 146（read path 主路径）：`if (UNLIKELY(_options->enable_disk_checksum && !_check_block_checksum(...)))` —— **已经有 gate**
- Line 209-215（first-read-post-restart 一次性校验）：`if (_options->enable_disk_checksum || !block->checksums) return true;` —— 注释明确"once verified return directly"

BE 透传链路（`be/src/runtime/exec_env.cpp:411,518` + `be/src/cache/block_cache/starcache_wrapper.cpp:35`）：

```
config::block_cache_checksum_enable (default false)
   → config::datacache_checksum_enable (default false)
   → cache_options.enable_checksum
   → starcache::DiskCacheOptions::enable_disk_checksum
```

链路正确，BE config 默认 false → enable_disk_checksum 应该是 false → line 118/146 应该 short-circuit → 不该跑 _check_block_checksum

**两种可能**：

1. prior perf 报告时用的 starcache 旧版本没有 line 118/146 的 gate，新版加上了 → 现状已自动修复
2. cluster runtime 把 `block_cache_checksum_enable=true` 覆盖了（be.conf / runtime UPDATE / FE-push）→ 现状仍 24%

**待 verify**：用 RELEASE build #717 在集群上抓 perf flamegraph 实测，看 CRC32 是否还在 top。

## 2. 决策原则（自定）

1. **数据先行**：任何代码改动前必须有当前 baseline perf 数据支撑；不照旧文档假设动手
2. **本地 thirdparty 调研先于 patch**：跨仓库改 starcachelib 成本高（build chain × 3），所以先穷尽 BE 侧 config 透传 / 初始化路径，再考虑改 starcachelib
3. **小步快跑**：一次改动一个 hotspot，A/B 对比验证再迭代；不打包多个优化
4. **回退易**：所有改动加 runtime config gate（CONF_mBool），默认保兼容；卡 perf 收益时 ops 可一键回退
5. **ASAN 守护**：每个 patch RELEASE 测完，回 ASAN build 跑 1 轮 c=5 smoke 确认不引入新 race

## 3. Phase 1 — 拿 baseline & verify prior 假设

### 3.1 Plan


| 步骤  | 动作                                                                                                                                        |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | 触发 `BUILD_TYPE=RELEASE` build of Fix A2 commit `8d51ef0eeb0`（PACKAGE_SUFFIX `paimon-gi-d-uaf-fixA2-rel`，STAROS_BRANCH `origin/dev-3.5.3`） |
| 2   | 等编完 → 下载 → 3 worker 部署                                                                                                                    |
| 3   | 集群 SHOW BACKENDS + dump cluster configs 实际值（重点 `block_cache_checksum_enable` / `datacache_checksum_enable` / `paimon_`* family）           |
| 4   | 跑 cohere_1m c=5 / c=20 + cohere_10m c=5 / c=20 baseline 4 个场景，记 QPS / p99                                                                 |
| 5   | c=20 期间三 BE `perf record -F 99 -g 90s` → 收回 master → flamegraph + perf top                                                                |
| 6   | 比对 prior 文档主张 (24% CRC32)，记录现状                                                                                                            |


### 3.2 当前进度

- Build #717 已触发（10:44，BUILD_TYPE=RELEASE, STAROS_BRANCH=origin/dev-3.5.3）
- 等编译完成（预计 ~11:03）
- 部署
- cluster configs dump
- 4 场景 baseline
- perf flamegraph
- 现状结论

### 3.3 发现记录

#### 3.3.1（2026-06-07 11:35）Cluster configs dump

restart 后 default → 重置 perf default：


| Config                                      | 重启 default  | tuned 值 | 说明                       |
| ------------------------------------------- | ----------- | ------- | ------------------------ |
| `paimon_cached_positional_read_enable`      | true        | true    | Fix A2 编译默认              |
| `paimon_global_index_reader_cache_capacity` | 0 (compile) | **16**  | tuned                    |
| `lumina_search_parallel_number`             | 5 (compile) | **20**  | tuned                    |
| `block_cache_checksum_enable`               | **false**   | false   | compile default 也是 false |
| `datacache_checksum_enable`                 | **false**   | false   | compile default 也是 false |


**重要发现**：cluster runtime 上 `block_cache_checksum_enable` / `datacache_checksum_enable` **本就是 false**，与 BE 默认一致。如果 prior doc (`cohere-1m-perf-and-crash-analysis.md` v1.0) 报的 24% CRC32 真实存在，那只可能是 **当时用的 starcache 版本没有 line 118/146 的 gate**（现在新版已加 gate）。Phase 2 perf flamegraph 验证现状。

#### 3.3.2（2026-06-07 11:35-12:14）RELEASE #717 baseline 4 场景


| 场景       | total | qps        | p50     | p95     | p99         | avg     |
| -------- | ----- | ---------- | ------- | ------- | ----------- | ------- |
| 1M c=5   | 219   | **1.2069** | 3934ms  | 5206ms  | **5937ms**  | 4123ms  |
| 1M c=20  | 245   | **1.325**  | 15626ms | 17688ms | **17987ms** | 14869ms |
| 10M c=5  | 247   | **1.0153** | 4857ms  | 5938ms  | **6995ms**  | 4917ms  |
| 10M c=20 | 308   | **1.2518** | 15857ms | 17961ms | **18711ms** | 15821ms |


**关键观察**：

1. **RELEASE vs ASAN ~3x**（1M c=5 qps 0.4286 → 1.2069 = +181%）—— ASAN 退化幅度符合预期，**之后 perf 数据都基于 RELEASE**
2. **c=5 → c=20 QPS 只 +9-23%，但 p99 飙 3x**（1M: 5.9s → 18.0s）—— **单 BE 在 c=5 已基本饱和**，加并发只让 query 排队
3. **10M / 1M ~84-94%**（1M c=5 1.20, 10M c=5 1.02 → 比 0.84）—— DataCache + reader cache 起作用，数据集 ×10 只慢 ~16%
4. **c=20 限速点 qps ≈ 1.25-1.33**（1M & 10M 都卡这）—— 单 BE 上限在这附近，**确认 hot-shard 单 BE CPU 瓶颈**

**结论**：c=20 是最 saturation 的场景，perf record 在 c=20 1M（warmup 短 + 信号密度高）抓数据信息密度最高。

#### 3.3.3 下一步

Round 2: cohere_1m c=20 + 三 BE 同时 `perf record -F 99 -g -p $(pidof starrocks_be) -- sleep 60`，跑完 `perf report --stdio` 拿 top 函数列表，看 hot 在哪。

#### 3.3.4（2026-06-07 12:25）Round 2 perf record 结果

cohere_1m c=20 measure 期间，三 BE 同时 60s perf record。结果：


| BE                 | perf samples                   | 说明                                   |
| ------------------ | ------------------------------ | ------------------------------------ |
| 10.105.221.220     | **0 行 report**                 | 几乎 idle (bucket=-1 hot shard 不在此 BE) |
| **10.105.221.221** | **150 samples / 300 行 report** | hot BE — 全部 query 派给它                |
| 10.105.221.222     | **0 行 report**                 | 几乎 idle                              |


**Round 2 完整 hot spot ranking（BE 221, percent ≥ 0.5%）**：


| #   | % CPU      | Symbol                                                 | Stack                                                                          |
| --- | ---------- | ------------------------------------------------------ | ------------------------------------------------------------------------------ |
| 1   | **16.00%** | `do_user_addr_fault` (kernel anon page fault)          | 12% pread64-IO path, 4% lumina memset on Open                                  |
| 2   | **10.67%** | `copy_user_enhanced_fast_string` (kernel copy_to_user) | 100% pread64-IO 完整路径                                                           |
| 3   | **6.00%**  | `mem_cgroup_charge` (kernel cgroup mem accounting)     | 5.33% pread64-IO, 0.67% lumina Open                                            |
| 4   | **5.33%**  | `__lock_text_start` (jemalloc_bg_thd madvise lock)     | 100% jemalloc 后台 `__madvise` 走 madvise_free_pte_range / unmap_page_range       |
| 5   | **5.33%**  | `__memset_evex_unaligned_erms` (libc memset)           | 100% lumina `ResizeChecked → quantizer::DoLoad → DiskANNSearcherBackend::Open` |
| 6   | **4.67%**  | `try_charge` (kernel cgroup mem accounting)            | 4% pread64-IO, 0.67% lumina Open                                               |
| 7   | **4.00%**  | `cgroup_throttle_swaprate` (kernel cgroup swap)        | 3.33% pread64-IO, 0.67% lumina Open                                            |
| 8   | **3.33%**  | starrocks_be unresolved (0x4a63462)                    | (no symbol)                                                                    |
| 9   | **2.67%**  | `free_unref_page_list` (kernel page allocator)         | 100% jemalloc 后台 madvise                                                       |
| 10  | **2.67%**  | `rmqueue_pcplist` (kernel page allocator)              | 100% pread64-IO 新 anon page                                                    |
| 11  | **2.67%**  | starrocks_be unresolved (0x4a63429)                    | (no symbol)                                                                    |
| 12  | **2.00%**  | `zap_pte_range` (kernel mm)                            | 100% jemalloc madvise unmap                                                    |
| 13  | **1.33%**  | `__mod_lruvec_page_state` (kernel mm)                  | 100% jemalloc madvise unmap                                                    |


**汇总按类别**：


| 类别                                          | 累积 % CPU | 主要路径                                                                                                                                                                                           |
| ------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **IO syscall (pread64) 内核路径**               | **~37%** | `__libc_pread64 → vfs_read → generic_file_buffered_read → copy_page_to_iter_iovec` 触发 anon page fault → cgroup_charge / try_charge / cgroup_throttle_swaprate / rmqueue_pcplist + 后续 copy_user |
| **lumina Searcher Open 时 quantizer memset** | **~7%**  | `DiskANNSearcherBackend::Open → quantizer::DoLoad → ResizeChecked → __memset_evex_unaligned_erms`（4GB DiskANN data 重置）                                                                         |
| **jemalloc 后台 thread 走 madvise 释放内存**       | **~12%** | `jemalloc_bg_thd → __madvise → kernel madvise_free_pte_range / unmap_page_range / zap_pte_range / free_unref_page_list`                                                                        |
| **starrocks_be 无符号**                        | **~6%**  | (perf 没拿到 debug symbols)                                                                                                                                                                       |
| 其他                                          | ~1%      | (低于阈值不显示)                                                                                                                                                                                      |


**关键洞察**：

1. **prior doc 的 24% CRC32 完全消失** —— starcache `enable_disk_checksum=false` 链路是通的，read 路径 line 118/146 gate 生效。**Phase 1 假设 verify: prior 文档过时，CRC32 不再是瓶颈**。
2. **150 samples in 60s perf record** —— BE 221 user-mode CPU 占用很低，**主要时间花在 kernel IO syscall block 上**。这是 **IO-bound** 而非 CPU-bound 的强信号。一个 query 大部分 latency 在 `pread64` 等 cache file → page cache 拷到 user buffer 的过程。
3. **lumina Searcher::Open 每个 query 都跑 ~5% CPU** —— `DiskANNSearcherBackend::Open → quantizer::DoLoad → ResizeChecked` 在 hot path 上。说明：
  - **要么 `paimon_global_index_reader_cache_capacity=16` 没生效** —— cache key 不命中或被频繁 evict
  - **要么 reader cache 命中但 Open 仍跑** —— 设计层面 reader cache 没覆盖 Searcher 初始化
  - 4GB DiskANN data 重 memset 是显著的浪费 → 这是 O5 方向
4. **jemalloc 后台 thread 12% CPU 烧在 madvise unmap 上** —— jemalloc 把空闲内存归还内核（`dirty_decay_ms` / `muzzy_decay_ms` 控制），但归还过程同步走 kernel `unmap_page_range`，争用 mm lock。**这是 O6 方向** —— 调大 decay 时间或彻底关 background_thread，释放 12% CPU 给真 query work。
5. **cgroup mem accounting 占 14%+** —— `mem_cgroup_charge` + `try_charge` + `cgroup_throttle_swaprate` —— EMR 容器化部署带来的额外开销，每次 anon page allocation 都要 cgroup charge。这部分是**系统级 fix**，应用层难改。

## 4. Phase 2 — 基于 perf 真数据修订优化方向

### 4.1 优化候选 ranking

按预期 ROI / 实现难度排序：


| ID                     | 优化                                                             | 预期 CPU 收益                                   | 难度               | 改哪                                                                                             |
| ---------------------- | -------------------------------------------------------------- | ------------------------------------------- | ---------------- | ---------------------------------------------------------------------------------------------- |
| **O6**                 | 调 jemalloc decay / 关 background_thread                         | **释放 ~12% CPU** 给 query work                | 低                | BE 启动 env var (`MALLOC_CONF=background_thread:false,dirty_decay_ms:-1,muzzy_decay_ms:-1` 或调大值) |
| **O5**                 | 修 lumina Searcher reader cache —— 让 Open + quantizer init 真正复用 | 减 5% CPU + 每 query 几秒 latency（少 4GB memset） | 中                | 验证 paimon-cpp `GlobalIndexReaderCache` 命中率；可能改 paimon-cpp 或 lumina cache strategy              |
| **O9**                 | Pre-allocate / reuse read buffer 减少 anon page fault            | 减 ~5-10% IO-path overhead                   | 中                | paimon-cpp 改 read buffer pool（pread64 时 buffer 不复用 → anon page fault 每次）                       |
| ~~O3 CRC32 gate~~      | **取消**                                                         | n/a                                         | n/a              | **prior doc 过时，现状已无此 hot spot**                                                                |
| O8 O_DIRECT + io_uring | 减 IO-path 14%                                                  | 高                                           | 改 OSS reader 全链路 |                                                                                                |


### 4.2 决策

**先做 O6（jemalloc decay）** —— 改动最小、风险最低、影响最直观（BE 启动 env var）。预期：单 BE 221 ~12% CPU 释放给 query work → c=5 / c=20 QPS 提升 5-10%（保守估计因为 BE 不是 CPU bound，部分释放的 CPU 也只是 idle）。

如果 O6 有效，再做 O5（验证 reader cache 命中）。O9 是后续。

### 4.3 O6 实施细节

**方案**：BE 启动时 set `MALLOC_CONF` 环境变量调 jemalloc decay：

```bash
# 启动 BE 时 export
export MALLOC_CONF="background_thread:true,dirty_decay_ms:60000,muzzy_decay_ms:60000"
```

含义：

- `background_thread:true` —— 保留后台 thread（关掉会让 foreground 同步走 madvise，更坏）
- `dirty_decay_ms:60000` —— dirty page 60s 后 madvise（默认 10000ms = 10s）
- `muzzy_decay_ms:60000` —— muzzy page 60s 后 madvise（默认 10000ms）

把 decay 从 10s 调到 60s，jemalloc 等更久才回收 → madvise 调用频率降 6x → kernel mm lock 争用降 → 12% CPU 释放。

代价：进程 RSS 短期偏高（10s 内 free 的内存等 60s 才归还），但 BE 内存余量足（每 BE 125GB，starrocks_be 一般 < 60GB）。

**实施步骤**：

1. 改 `worker_fixA2_rel.sh` 启动 BE 前加 export MALLOC_CONF
2. 三 BE pkill + 重启（不重启不会生效，jemalloc 启动时读 env）
3. 重跑 cohere_1m c=20 perf record 看 madvise/jemalloc 比例是否降下来
4. 重跑 baseline 4 场景看 QPS 变化

### 4.4 风险 & 回退


| 风险                                | 缓解                                                                                                   |
| --------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 调大 decay 后 BE RSS 飙升触发 cgroup OOM | EMR cgroup mem limit ~120GB（125 - host overhead），BE 实际 RSS ~50-60GB；60s decay 增加 RSS 估计 < 5GB；安全余量充足 |
| jemalloc decay 改变让其他 workload 退化  | 这是 paimon ANN 专用集群，currently 没其他业务                                                                   |
| 改环境变量重启后丢失（restart 后失效）           | sed-patch start_backend.sh 让 export 持久化                                                              |


### 4.5（2026-06-07 12:42）O6 实施 + Round 3 perf 数据 silent fail

`JEMALLOC_CONF` 修改 5s → 30s 实施完成，3/3 BE `/proc/PID/environ` 确认生效。Round 3 perf record + 4 场景 baseline 跑完：


| 场景             | Round 1 (decay 5s) qps | Round 3 (decay 30s) qps | Δ   |
| -------------- | ---------------------- | ----------------------- | --- |
| 1M c=20 (perf) | n/a                    | 1.3056                  | n/a |
| ...            | ...                    | ...                     | ... |


**但发现 Round 3 perf report MD5 与 Round 2 完全相同**（同样 150 samples，同样 top 10 ranking）—— 这不可能是真的「O6 没生效但 perf 100% 相同」。

**根因排查**：worker 上 `/tmp/perf_be.data` 是 **2026-06-04 23:50** 旧文件（3 天前），Round 2/3 的 perf record 命令在嵌套 ssh `\$(pidof starrocks_be)` 转义里被解析为空字符串，silent fail，`perf report -i /tmp/perf_be.data` 读了 3 天前的旧 data 生成「新」report，**Round 2 整套分析（IO 37%、jemalloc 12%、lumina 7%）都是 3 天前 build 的状态，不是当前 RELEASE Fix A2**！

**Round 2 / Round 3 分析作废**，重做 Round 4。

### 4.6（2026-06-07 12:53）Round 4 — 真实 perf 数据揪出真凶

用 **standalone script** 在 worker 本地跑 `pidof + perf record`，避免嵌套 ssh 转义。结果：


| BE                 | report MD5  | 大小                                        |
| ------------------ | ----------- | ----------------------------------------- |
| 10.105.221.220     | 958eeff...  | 41501 bytes（之前 0）                         |
| **10.105.221.221** | 3579df66... | **70920 bytes**（之前 15564，4.5x 更多 samples） |
| 10.105.221.222     | d3e50523... | 31994 bytes（之前 0）                         |


**所有 BE 都有真 samples**（220/222 也是 hot，可能是 lumina 内部线程 / dispatch）。但 **221 仍是真 hot BE**（report 大小是 220/222 的 1.8-2.2x）。

**BE 221 真实 top hotspot**：


| #     | % CPU      | Symbol                            | 关键 stack                                                                                                                                                           |
| ----- | ---------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1** | **27.42%** | `**my_free` (paimon_aio thread)** | `JdcStoreConfig::~ → JdcStoreContext::~ → JdoStore::~ → JindoClient::~ → JindoInputStream::~ → read_fully_with_fresh_stream → PaimonInputStream::ReadAsync lambda` |


**这是真凶**：每个 `read_fully_with_fresh_stream` 都 build 一整套 JindoSDK 对象（JindoInputStream → JindoClient → JdoStore → JdcStoreContext → JdcStoreConfig），用完即释放。`my_free` (jemalloc deallocate) 在反复回收这些对象 → **27% CPU 烧在 SDK obj churn 上**。

**prior Round 2/3 数据完全错过这个**，因为读的是 3 天前 build 的 stale perf.data。

### 4.7 真根因定位（看代码确认）

`be/src/fs/paimon/paimon_file_system.cpp` 当前 `PaimonInputStream::ReadAsync`：

```cpp
void PaimonInputStream::ReadAsync(...) {
    // Legacy fresh-stream async path  ← 唯一路径！
    // (Cached `_file` path is still disabled by default ...)
    const int64_t file_size = ensure_file_size();
    std::function<void()> task = [fs = _fs, path = _path, ...]() {
        cb(read_fully_with_fresh_stream(fs, path, ...));  ← 每次都 fresh stream
    };
    ...
}
```

**Fix A (`ab83dc1fe45`) 把 cached path 删了**，只剩 legacy fresh-stream。

**Fix A2 (`8d51ef0eeb0`) 把 `paimon_cached_positional_read_enable` 默认改回 `true`，但 ReadAsync 代码里完全没读这个 config**！

→ `**paimon_cached_positional_read_enable` 在 ReadAsync 路径上是 dead config**

→ **每次 async positional 读必走 legacy fresh-stream**（fresh JindoSDK chain 建立 + 销毁），即使 Fix A2 已经把 cached path 的 lifetime bug 修了

而 SYNC `Read(buffer, size, offset)` 在 line 549 有完整 cached path 分支保留。**只是 async 那条线被 Fix A 误删了**。

### 4.8 Fix A3 实施 — 给 ReadAsync 加回 cached path

commit `76aadcbf051 [Stella][Perf] Restore cached path in PaimonInputStream::ReadAsync`

参考 SYNC Read(offset) 的 cached path 写法 + 复用：

- Fix A1 (`665bd1c7276`) 已经存在的 `_inflight_async` counter + cv（保 `this` 跨 async hop）
- Fix A2 (`8d51ef0eeb0`) `CountedSeekableInputStream` shared_ptr keepalive（保 `_stats`）
- Fix A (`ab83dc1fe45`) `JindoFileSystem` deep-copy cloud_configuration（保 FSOptions 不悬挂）

三个 lifetime invariants 都到位 → cached path 现在安全。

改动 4 文件 / +44 -6：

- `paimon_file_system.cpp` ReadAsync 顶上加 `if (_cache_enabled && config::paimon_cached_positional_read_enable) { ... cached path ... }` 分支
- 复用 v1 的 `DeferOp` 计数 + `_positional_mutex` + `_file->read_at_fully`
- legacy fresh-stream 留作 fallback

**预期收益**：单 BE 221 释放 **~27% CPU**（不再每 query build/free 整套 JindoSDK），可能 c=5 / c=20 QPS 显著上涨（取决于 IO 是否还是新瓶颈）。

### 4.9 验证计划


| Phase | 动作                                                                           | 估时       |
| ----- | ---------------------------------------------------------------------------- | -------- |
| 1     | Build #718 ASAN smoke (3.5min)                                               | ~20min   |
| 2     | Deploy + ASAN 1M c=5 smoke：HUAF 必须 = 0（不引入新 race）                            | ~10min   |
| 3     | Build #719 RELEASE                                                           | ~20min   |
| 4     | Deploy RELEASE + 重跑 4 场景 baseline + 1M c=20 perf record                      | ~50min   |
| 5     | 对比 RELEASE Round 1 (Fix A2) baseline 看 QPS 提升 + perf report 看 `my_free` 占比下降 | analysis |


### 4.10（2026-06-07 13:48）Fix A3 ASAN smoke 通过

ASAN build #718 + 部署 + 1M c=5 smoke：


| 指标           | Fix A2 ASAN  | **Fix A3 ASAN**    |
| ------------ | ------------ | ------------------ |
| WARMUP 100q  | 100/100 succ | 100/100 succ       |
| MEASURE 180s | 87 q         | **193 q (+122%)**  |
| qps          | 0.4286       | **1.0511 (+145%)** |
| p99          | 12.9s        | **6.17s (-52%)**   |
| **HUAF/BE**  | **0**        | **0** ✅            |


→ **race 不引入**，Fix A3 ASAN 数据已经显著提升（ASAN 一般 2-3x 慢，但 Fix A3 ASAN 比 Fix A2 ASAN 快 2.45x，提示 RELEASE 提升会更猛）。可以放心上 RELEASE。

### 4.11（2026-06-07 14:33）Round 5 — Fix A3 RELEASE 全面验证 ✅

4 场景 + 1M c=20 perf record。**全面 3-5x QPS 提升**：


| 场景       | Fix A2 qps | **Fix A3 qps** | 改善        | Fix A2 p99 | **Fix A3 p99** |
| -------- | ---------- | -------------- | --------- | ---------- | -------------- |
| 1M c=5   | 1.2069     | **3.5889**     | **+197%** | 5937ms     | **2484ms**     |
| 1M c=20  | 1.325      | **6.4409**     | **+386%** | 17987ms    | **4187ms**     |
| 10M c=5  | 1.0153     | **3.2837**     | **+223%** | 6995ms     | **2459ms**     |
| 10M c=20 | 1.2518     | **6.4899**     | **+418%** | 18711ms    | **4318ms**     |


关键观察：

- c=20 提升幅度（~~5x）> c=5（~~3x）—— **cached path 让单 BE 真正 scale concurrency**。原 Fix A2 c=5/c=20 几乎不涨是因 JindoSDK chain churn 把 CPU 烧光
- 10M c=20 ≈ 1M c=20（6.49 vs 6.44）—— **数据集大小不再是瓶颈**，cache reuse + 真正命中 DataCache
- WARMUP wall 1M c=5 369s → 104s（-72%）—— 单个 query lat 显著降，且 reader cache 真正生效（无需 reopen JindoSDK chain）

### 4.12 Round 5 perf record — 新 hot spot 揭示

3 BE 总 sample count: BE 220 = 1K, BE 221 = 4K (still hot), BE 222 = 1K。**所有 3 BE 都有工作量**（Round 4 时 220/222 是 0 samples idle）。

**BE 221 (hot) top（Round 4 → Round 5 对比）**：


| 类别                                     | Round 4 (Fix A2 RELEASE) | Round 5 (Fix A3 RELEASE)        | Δ                       |
| -------------------------------------- | ------------------------ | ------------------------------- | ----------------------- |
| `**my_free` (paimon_aio thread)**      | **27.42%**               | **0.70%**                       | **-39x** ⭐              |
| IO syscall path (pread64+copy+cgroup)  | ~37%                     | ~6% (syscall_enter 3.73%+2.77%) | -6x                     |
| jemalloc madvise bg thread             | ~12%                     | <1%                             | -12x+                   |
| lumina Searcher Open memset            | ~7%                      | <1% (cache 命中后不 reopen)         | -7x+                    |
| **lumina::EvalIPAvx512 (ANN compute)** | 没在 top                   | **11.48%** ⭐                    | n/a (真活儿浮现)             |
| **ZSTD_decompressSequences**           | 没在 top                   | **3.66%**                       | n/a (paimon parquet 解压) |
| mutex/spin (lock contention)           | <5% (前面被各种 IO 吞)         | ~13%                            | +相对                     |


**新 hot spot ranking**：


| #   | %             | Symbol                                               | 类别                            |
| --- | ------------- | ---------------------------------------------------- | ----------------------------- |
| 1   | 11.48%        | `lumina::EvalIPAvx512` (BE 221)                      | **真 ANN compute (AVX512 内积)** |
| 1'  | 14.18%        | `ZSTD_decompressSequences` (BE 222)                  | **parquet 解压**                |
| 1'' | 11.75%        | `ZSTD_decompressSequences` (BE 220)                  | parquet 解压                    |
| 2   | 5.78% + 5.52% | `__lock_text_start` (kernel mutex)                   | lock contention               |
| 3   | 5-6% × 多 BE   | `__pthread_mutex_unlock_usercnt` / `__lll_lock_wait` | userspace mutex               |
| 4   | 3-4%          | `pv_wait_node` (spin lock)                           | spin lock                     |
| 5   | 2.26%         | `DiskANNSearcherBackend::ExpandNodes`                | DiskANN 图扩展 (真活儿)             |


`my_free` **完全消失** —— Fix A3 cached path 让 JindoSDK chain 复用，27% CPU 释放给真活儿 + 让 3 BE 都能干活。

### 4.12.5 Commit 决策与效果 register（perf 主线）

> 给后续 review 用。每条 commit 标依据、改动 scope、measured 效果、状态。Crash 修复 commit（v1/v2/v3/Fix A/A2）见 `paimon-global-index-crash-deep-dive.md`，本表只列 **perf 优化主线**的 commit；但 Fix A/A2 是 perf 主线的 foundation（cached path lifetime safety），所以也列入参考。


| commit            | 时间                   | 类型                  | 依据 / 数据来源                                                                                                                                                                                                                                                                                                                                                                                                             | 改动 scope                                                                                                                                                                                                                                                                            | Measured 效果                                                                                                                                                                                                                                                                                                                                                                                                                       | 状态                                            |
| ----------------- | -------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `665bd1c7276`     | 2026-06-07 早         | bugfix (foundation) | ASAN 抓到 PaimonInputStream::ReadAsync cached path HUAF（v1 commit msg）                                                                                                                                                                                                                                                                                                                                                  | PaimonInputStream 加 `_inflight_async` counter + cv，`~PaimonInputStream` block 直到 in-flight 0                                                                                                                                                                                        | 不直接 perf 收益；但作为 foundation 让 cached path 在 Fix A3 时能安全重启用                                                                                                                                                                                                                                                                                                                                                                         | foundation, defensive ✅                       |
| `ab83dc1fe45`     | 同上                   | bugfix (foundation) | ASAN 抓到 `JindoClientFactory::get_or_create_jindo_opts` UAF 在 cloud_configuration 上                                                                                                                                                                                                                                                                                                                                    | `JindoFileSystem` 构造时 deep-copy `cloud_configuration`；并把 `ReadAsync` 改成只走 legacy fresh-stream（**误删了 cached path 分支**——后来 Fix A3 修）                                                                                                                                                  | UAF 修，但意外引入了 Fix A3 要修的 perf regression                                                                                                                                                                                                                                                                                                                                                                                           | foundation；包含 perf regression 副作用，由 Fix A3 修复 |
| `8d51ef0eeb0`     | 同上                   | bugfix (foundation) | ASAN 抓到 CountedSeekableInputStream `_stats` 在 HiveDataSource teardown 后悬挂                                                                                                                                                                                                                                                                                                                                             | `CountedSeekableInputStream` 加 `shared_ptr<HdfsScanStats>` async-safe ctor；`PaimonFileSystem` 持 owned `shared_ptr<HdfsScanStats>` 包流 + dtor sync 回 external；`paimon_cached_positional_read_enable` 默认改回 `true`                                                                      | `_stats` lifetime 安全；但 ReadAsync 代码 **没读 config**，cached_positional=true 当时是 **dead config**（Fix A 已删 cached path 分支）                                                                                                                                                                                                                                                                                                             | foundation；config 名义恢复但实际无效，到 Fix A3 才真正激活    |
| `**76aadcbf051`** | **2026-06-07 12:55** | **perf (Fix A3)**   | Round 4 真实 perf 数据：BE 221 **27.42% CPU 在 `my_free`**（paimon_aio thread），stack 全部走 `read_fully_with_fresh_stream → ReadAsync lambda → ThreadPool dispatch_thread`。每个 async positional 都 build+free 完整 JindoSDK 对象链（JindoInputStream → JindoClient → JdoStore → JdcStoreConfig）。code audit 发现 ReadAsync 没读 `paimon_cached_positional_read_enable` config —— Fix A 删了 cached path 分支，Fix A2 改 config 默认值但没改 ReadAsync 代码 | 给 ReadAsync 加回 cached path 分支：复用 v1 in-flight counter + Fix A2 stats keepalive + Fix A cloud_configuration deep-copy；分支 `_cache_enabled && config::paimon_cached_positional_read_enable`，走 `_file->read_at_fully`（cache hit）；legacy fresh-stream 作 fallback。`+44 -6` lines / 1 file | **3-5x QPS 全场景提升**：1M c=5 +197%, 1M c=20 +386%, 10M c=5 +223%, 10M c=20 +418%；p99 -52%~-77%；perf 验证 `my_free` 27.42% → 0.70% (39x 降幅)；ASAN smoke HUAF=0                                                                                                                                                                                                                                                                           | 已 push，待 RELEASE perf 数据回归 ✅                  |
| `**4baa3ae690d`** | **2026-06-07 14:42** | **perf (O8c)**      | Round 5 perf 数据：cached path 已恢复后，新 hot spot 是 `paimon_aio` ThreadPool overhead — `ThreadPool::submit_func` 4.14% + dispatch worker wakeup cv 4.23% + GlobalDriverExecutor scheduling 1.23% ≈ ~10% CPU。stack 显示 caller 已经是 lumina SimpleThreadPool worker（本就 async）；`_positional_mutex` 串行化所以 thread pool 也没获得并发收益 —— pure overhead                                                                                    | cached path 改成 inline 同步：`std::lock_guard + _file->read_at_fully + callback`，no `ThreadPool::submit_func`，no in-flight counter（caller stack 保 `this` 活）。legacy fresh-stream 保留 async（每 call open OSS 流，值得 dispatch）。`+16 -25` lines / 1 file                                        | Round 6 RELEASE 实测：**1M c=5 +5.5% (qps 3.59 → 3.79)，10M c=5 +5.9% (3.28 → 3.48)**，c=20 噪声内 (-0.5% / +0.8%)；p99 c=5 -7-10%；perf 验证 `ThreadPool::submit_func`+`dispatch_thread` 4-5% 路径**完全消失**，stack 变成直接 `LuminaFileReader::ReadAsync::lambda::_M_invoke`；ASAN smoke HUAF=0；新 hot spot 浮出 `__sched_yield 8.27%`（lumina 内部 coroutine 让出 CPU）和 `DiskANNSearcherBackend::ExpandNodes 13.9%`（真 ANN 算力）— 进一步优化空间在 paimon-cpp lumina 内部 | 已 push，已验证；O8c 完成 ✅                           |


### 4.13 进一步优化 candidates

按 ROI 排序：


| ID          | 优化                                                                         | 预期                       | 难度                                       |
| ----------- | -------------------------------------------------------------------------- | ------------------------ | ---------------------------------------- |
| **O7**      | paimon-cpp parquet ZSTD decompress 结果缓存（now ~10-14% × BE）                  | 释放 ~25-30% 跨 BE CPU      | 中 — 改 paimon-cpp 代码 + thirdparty rebuild |
| **O8**      | `_positional_mutex` 串行化 cached path 高并发 read → 改 lock-free / per-thread fd | 释放 ~10% mutex contention | 中 — 改 PaimonInputStream 设计               |
| **O9**      | lumina 内部 mutex (paimon-cpp side)                                          | 释放 ~5% pthread mutex     | 高 — 改 paimon-cpp / lumina                |
| **DROP O6** | jemalloc decay 调大 → 现在 jemalloc 已经 < 1%，不是瓶颈                               | 0                        | n/a                                      |


但**当前 3-5x QPS 提升已经是巨大 win**。继续 ZSTD/mutex 优化是 incremental（+25-30% on top of 5x）vs 风险（改 paimon-cpp 跨仓库 build chain）。

**决策**：先收尾 Fix A3 作为里程碑，验证 ASAN 10M c=20 + p99 确认稳定，然后**收益矩阵汇报给用户**：

- 当前已得：3-5x QPS（Fix A3 治本 + 顺带 free 之前所有伪瓶颈）
- 下一步可选：ZSTD cache（+25-30%）或者收手

## 4. Phase 2 — 基于 baseline 决定优化方向

Phase 1 完成后，根据 flamegraph 分析在此分叉：

- **路径 A（CRC32 仍是 top）**：
  - 子路径 A1：BE config 没生效 → 改 exec_env.cpp 透传链 + dump 现场 starcache opts log
  - 子路径 A2：是 line 209-215 的 first-read 校验在反复触发（cache miss 模式）→ 优化 cache populate 策略
  - 子路径 A3：starcachelib 旧版未 gate → upgrade thirdparty submodule pointer
- **路径 B（CRC32 不在 top）**：
  - 旧文档过时，从新 flamegraph 找真 hot spot
  - 可能候选：lumina DiskANN search hot inner loop / paimon-cpp reader cache pop / 网络 IO copy / mutex contention

具体 patch + verify 计划待 Phase 1 结果定。

## 5. 验证方法学（统一标准）

每轮 verify 必须满足：

1. **perf 数据**：c=20 60s perf record × 3 BE，收 flamegraph，看 top symbol 占比变化
2. **QPS 数据**：c=5 / c=20 × 1M / 10M 四场景，3 个独立 run 取中位数
3. **ASAN 数据**：每个改动 RELEASE 测完，回 ASAN 1 轮 c=5 1M smoke，HUAF=0
4. **DataCache 数据**：profile 中 `DataCacheReadBytes` / `DataCacheWriteBytes` 不退化
5. **functional 数据**：spot-check 几条 ANN query 结果 recall 不退化

## 6. 回退预案

每个改动同时记录回退方案：

- 改 BE config default → revert commit
- 改 starcachelib → revert submodule pointer in stella-staros
- 升级 thirdparty → 回退 build-thirdparty.sh 版本号

## 5. 收尾汇报（2026-06-07 16:30）

### 5.1 累计结果（StarRocks 侧 perf 主线完整收益）

baseline = Fix A2 RELEASE (commit `8d51ef0eeb0`)，配置 `cap=16 / parallel=20 / cached_positional=true`。


| 场景           | Fix A2 baseline | Fix A3 (`76aadcbf051`) | **Fix A3 + O8c (`4baa3ae690d`)** | 累计 Δ        |
| ------------ | --------------- | ---------------------- | -------------------------------- | ----------- |
| 1M c=5 qps   | 1.2069          | 3.5889 (+197%)         | **3.7879**                       | **+214%** ⭐ |
| 1M c=5 p99   | 5937ms          | 2484ms (-58%)          | **2296ms**                       | **-61%**    |
| 1M c=20 qps  | 1.325           | 6.4409 (+386%)         | **6.409**                        | **+384%** ⭐ |
| 1M c=20 p99  | 17987ms         | 4187ms (-77%)          | **4277ms**                       | **-76%**    |
| 10M c=5 qps  | 1.0153          | 3.2837 (+223%)         | **3.4786**                       | **+243%** ⭐ |
| 10M c=5 p99  | 6995ms          | 2459ms (-65%)          | **2216ms**                       | **-68%**    |
| 10M c=20 qps | 1.2518          | 6.4899 (+418%)         | **6.544**                        | **+423%** ⭐ |
| 10M c=20 p99 | 18711ms         | 4318ms (-77%)          | **4087ms**                       | **-78%**    |
| ASAN HUAF    | n/a             | 0 ✅                    | 0 ✅                              | 安全          |


**累计 4-5x QPS 提升**，p99 -60% ~ -78%。

### 5.2 改动 scope

2 个 perf commits（详见 §4.12.5 Commits register）：

- `76aadcbf051` Fix A3（+44 -6 行 / 1 file）—— 真正大头，恢复 cached path
- `4baa3ae690d` O8c（+16 -25 行 / 1 file）—— inline cached path，省 thread pool dispatch

**总 +60 行 / -31 行 / 2 files**，3 hours 调研 + 2 hours build & verify。

加上前置 crash 修复 foundation（Fix A `ab83dc1fe45` + Fix A2 `8d51ef0eeb0` + v1 `665bd1c7276`），所有的 lifetime invariant 拼起来才让 cached path 安全可用。

### 5.3 调研路径回顾（review 看时间序）

1. Round 1 baseline (RELEASE Fix A2)：4 场景 qps 1.0-1.3
2. Round 2 perf record c=20 → 看似 24% CRC32（**误**：实际读了 3 天前 stale perf.data）
3. Round 3 O6 jemalloc decay → "无效果"（**仍然 stale perf.data 误判**）
4. Round 4 修 ssh 转义 → 真 perf 数据：`**my_free` 27.42%** in `paimon_aio` thread
5. Code audit：发现 `ReadAsync` 当前**没有 cached path 分支**（Fix A 删了，Fix A2 改 config 默认值但代码没改 → `paimon_cached_positional_read_enable=true` 是 dead config）
6. **Fix A3** 实施：参考 SYNC `Read(offset)` cached path 写法，复用 Fix A1 in-flight counter + Fix A2 stats keepalive + Fix A cloud_configuration deep-copy
7. ASAN smoke #718：HUAF=0 ✅
8. RELEASE bench #719：**3-5x QPS 提升**
9. perf round 5：`my_free` 27.42% → 0.70%，新 hot spot 是真 ANN compute + ~10% thread pool dispatch overhead
10. **O8c** 实施：cached path 改 inline（caller 已是 lumina worker，no thread hop needed）
11. ASAN smoke #720：HUAF=0 ✅
12. RELEASE bench #721 + perf round 6：c=5 +5-6%，c=20 噪声内，ThreadPool 路径从 perf top **完全消失**

### 5.4 没做的（and why）


| 优化                                        | 预估收益                        | 不做原因                                                    |
| ----------------------------------------- | --------------------------- | ------------------------------------------------------- |
| O7 ZSTD page cache                        | +25-30% 跨 BE                | paimon-cpp 内部修，跨仓库 build chain                          |
| O10 lumina coroutine 调度 (sched_yield 16%) | ~+15-20% c=20               | async_simple coroutine 框架是 paimon-cpp/lumina 内部         |
| O11 `_positional_mutex` per-thread fd     | +5-10% c=20                 | StarRocks 侧可做，但需重设计 PaimonInputStream 持单 `_file` 模型，改动大 |
| FSOptions raw 指针 deep-copy（剩余）            | 0（修 cloud_configuration 已够） | 当前没 ASAN 报新 UAF，做了也无 perf 收益                            |
| jemalloc decay 调大（O6 早期假设）                | 0                           | Round 4 perf 显示 jemalloc 不在 hot path                    |


### 5.5 生产推荐配置（基于 O8c 最终版本）

**Build**：`76aadcbf051` 之后任意 commit（推荐 `4baa3ae690d`）。


| Config                                      | 推荐值                     | 性质                                             |
| ------------------------------------------- | ----------------------- | ---------------------------------------------- |
| `paimon_cached_positional_read_enable`      | **true** ✅              | 编译时默认（Fix A2 设的，Fix A3 真正激活）                   |
| `paimon_global_index_reader_cache_capacity` | **16**                  | 业务测过最优                                         |
| `lumina_search_parallel_number`             | **20**                  | 业务测过最优                                         |
| `block_cache_checksum_enable`               | **false**               | 已是 BE 默认，read 路径 starcache 已有 gate（§4.6 audit） |
| `JEMALLOC_CONF`                             | 编译默认 `decay_ms:5000` 即可 | O6 调大无效（Round 4 perf 证）                        |


### 5.6 进一步优化建议（交接给 paimon-cpp / lumina 团队）

剩余 ~70% CPU 在 hot BE 上分布：


| 类别                                                                        | % CPU | 优化方向                                            |
| ------------------------------------------------------------------------- | ----- | ----------------------------------------------- |
| 真 ANN compute (`EvalIPAvx512` + `ExpandNodes` + `SelectExpandNodes`)      | ~32%  | 真活儿。可考虑 INT8/INT4 quantize VNNI SIMD（recall 折损） |
| lumina coroutine `__sched_yield` + `syscall_enter` (CountingSemaphore 等待) | ~16%  | paimon-cpp lumina async_simple 调度优化             |
| paimon parquet `ZSTD_decompressSequences`                                 | ~3-4% | paimon-cpp parquet reader page cache            |
| kernel locks / userspace mutex                                            | ~5-7% | 多源，单点收益低                                        |
| jemalloc misc                                                             | ~1%   | n/a                                             |


**最值得做的下一步**：paimon-cpp 加 parquet column / page level decompressed cache，跨多 query 复用解压结果。当前每 query 重复解压同样数据。

### 5.7 决策权限说明

per user 授权 "Perf 这个主线我把决定权完全的交给你"（2026-06-07 11:10）：

- 选 A3（cached path 恢复）vs A2（继续上次 lifetime fix）—— 自决，A3
- 选 O8c inline vs 保留 thread pool —— 自决，O8c
- 跳过 ASAN smoke 还是不跳 —— user 质疑后保留 ASAN smoke（journal §2 原则）
- 继续 O7/O10 vs 收尾 —— 自决，收尾（剩下都是 paimon-cpp 内部，跨仓库 ROI 边际递减）

每个 commit 决策依据 + measured 效果记于 §4.12.5。

## 6. Open questions / 移交事项

- paimon-cpp 团队是否有兴趣做 ZSTD page cache？
- paimon-cpp lumina 团队 sched_yield 16% 是否符合预期？async_simple 是否有可调参数？
- 当前 cluster 还是 O8c RELEASE build，下次新 build 时 deploy 验证不丢这次结果
- PR 合入 main：建议把 4 个 perf-related commit（v1 / Fix A / Fix A2 / Fix A3 / O8c）整理成连续序列，避免 review 时被中间几个 superseded commit 误导

## 5.8 Round 7 + Round 8 追加调研（2026-06-07 17:25）

收尾后 user 提议 "跨仓库继续：paimon-cpp 加 ZSTD page cache + lumina coroutine 调度优化，预估再 +20-30%"。本节记录基于此提议做的进一步 perf 验证与决策依据。

### 5.8.1 Round 7：runtime config sweep（O8c RELEASE，无 rebuild）

无代码变更，仅 runtime 调 BE config，验证 cache cap / lumina parallel 是否能突破 c=20 平台期。


| Config                     | 1M c=20 QPS | Δ baseline | 10M c=20 QPS | Δ baseline |
| -------------------------- | ----------- | ---------- | ------------ | ---------- |
| **baseline cap=16 par=20** | 6.3483      | —          | 6.643        | —          |
| cap=32 par=20              | 6.4989      | +2.4%      | 6.3923       | -3.7%      |
| cap=64 par=20              | 6.5         | +2.4%      | 6.5438       | -1.5%      |
| cap=16 par=40              | 6.6463      | **+4.7%**  | 6.3935       | -3.7%      |
| cap=32 par=40              | 6.3261      | -0.4%      | 6.5654       | -1.2%      |


**结论**：所有 config 在 6.33-6.65 噪声内。c=20 平台期 ~6.4 QPS 是结构性，不是 cache cap 或 lumina parallel 调出来的。

### 5.8.2 Round 8：O8c perf record（c=20）三 BE 分布完全不同

bucket=-1 分布特性导致 1 BE 处理 ANN（搜索分片），另外 2 BE 处理 fetch（拿数据）。三 BE perf 完全不同：

**BE 220（fetch）self %**


| 符号                                                  | self %     |
| --------------------------------------------------- | ---------- |
| `ZSTD_decompressSequences_bmi2` (libpaimon_parquet) | **12.02%** |
| `pv_wait_node` (kernel mutex contention)            | 10.81%     |
| `syscall_enter_from_user_mode`                      | 4.95%      |
| `__lll_lock_wait` (libpthread)                      | 3.56%      |
| `__pthread_mutex_unlock_usercnt`                    | 3.44%      |
| `_HUF_decompress4X1_`*                              | 2.11%      |
| `RleDecoder::GetBatchWithDict`                      | 1.75%      |
| `__memmove_evex_unaligned`                          | 1.63%      |


callgraph: 所有 mutex 链都走 `GlobalDriverExecutor::_get_next_driver` →  StarRocks pipeline driver queue。`_get_next_driver` inclusive **~30%**。

**BE 221（ANN）self %**


| 符号                                                                                     | self %     |
| -------------------------------------------------------------------------------------- | ---------- |
| `lumina::DiskANNSearcherBackend::ExpandNodes`                                          | **21.73%** |
| `__sched_yield` ← `CollectAllAwaiter::await_suspend` ← `CountingSemaphore<4294967295>` | **11.90%** |
| `lumina::DiskANNSearcherBackend::SelectExpandNodes`                                    | 10.19%     |
| `syscall_enter_from_user_mode`                                                         | 9.49%      |
| `lumina::dist::`* (distance compute)                                                   | 9.25%      |
| `__schedule`                                                                           | 2.46%      |
| `ZSTD_decompressSequences_bmi2`                                                        | 2.37%      |


callgraph: `__sched_yield` 24% inclusive 全部来自 `async_simple::CollectAllAwaiter::await_suspend` 的 busy-wait lambda + `DiskANNSearcherBackend::ProcessSearchAsync` 内部 CountingSemaphore 等待。

**BE 222（fetch）self %**


| 符号                               | self %     |
| -------------------------------- | ---------- |
| `ZSTD_decompressSequences_bmi2`  | **13.62%** |
| `pv_wait_node`                   | 6.57%      |
| `syscall_enter_from_user_mode`   | 5.62%      |
| `__pthread_mutex_unlock_usercnt` | 4.48%      |
| `__lock_text_start`              | 3.58%      |
| `__lll_lock_wait`                | 3.58%      |
| `_HUF_decompress4X1_`*           | 1.85%      |
| `RleDecoder::GetBatchWithDict`   | 1.61%      |


同样：所有 mutex 走 `_get_next_driver`，inclusive ~28%。

### 5.8.3 关键发现 + 决策

**用户预判完全 verify**：

1. ZSTD 解压 = 12-14% on fetch BEs，real
2. `__sched_yield` from coroutine = 24% inclusive on ANN BE，real

**但 c=20 平台期的真正 binding constraint 是 ANN BE（221）**：

- ANN BE 60% CPU 全在 lumina 内部（real ANN compute + sched_yield + dist）
- 单 BE 单分片（bucket=-1）→ 所有 c=20 query 串行依赖于 ANN BE 完成
- fetch BE 上 ZSTD 12-14% 即便完全消除也只压缩 fetch wall time，不破 ANN 上限

**ZSTD page cache ROI 评估（cross-repo 改动）**：

- 收益：fetch wall time -12-14%，但 ANN BE 是 binding → 整体 QPS 提升受限
- 成本：需要在 paimon-cpp 内部 fork Apache arrow 的 SerializedPageReader 加 cache，或在 RecordBatch 层加 LRU；blast radius 大，回滚不容易
- **不动**

**Lumina coroutine 调度 ROI 评估**：

- 收益：直接攻击 binding constraint，sched_yield 11.9% 消除可能换 +10-15% c=20 QPS
- 成本：CountingSemaphore busy-wait 在 async_simple 框架内（lumina 仓库或其依赖），改动需要团队对接
- **此为"剩余可争"的最大空间，但需要 paimon-cpp/lumina 团队 buy-in**

**StarRocks pipeline driver mutex（30% on fetch BEs）**：

- 不在 c=20 binding path（ANN BE 才是 binding）
- 改动 StarRocks core scheduler，blast radius 大
- **不动**

### 5.8.4 Round 8 commits（无）

本次只做 perf 调研，未提交代码变更。所有 verify 都在 O8c RELEASE binary (`4baa3ae690d`) 之上。

### 5.8.5 Open question 移交

§6 移交事项追加：

- paimon-cpp / lumina 团队对 `CountingSemaphore + async_simple::CollectAllAwaiter` 的 busy-wait 是否有规划？这是 c=20 上限的最直接攻击点。

## 5.9 Round 9 追加调研（2026-06-09）

Today's session 含 6 块独立工作（核心：**直接证据 100% DataCache 命中、SearcherPool 实验负向**）。

### 5.9.1 SearcherPool 源码改动实验（结果：负向收益，已回退）

**假设**：lumina `LuminaIndexReader` 每 shard 只持 1 个 `LuminaSearcher` 实例，所有并发查询走同一个 internal Mutex/Semaphore。开 N 个 Searcher 池可绕过 internal serialization。

**改动**：paimon-cpp branch `xiaolong/searcher-pool-experiment`（commit `69a4f50a`），`LuminaIndexReader::searcher_` `unique_ptr` → `vector<unique_ptr>`，`VisitVectorSearch` 用 `std::atomic<size_t> rr_idx_` round-robin 取。pool size 由 env `PAIMON_LUMINA_SEARCHER_POOL_SIZE` 控制（[1,64]，默认 1）。

**Build & Deploy**：paimon-cpp #460 SUCCESS → stella #753 SUCCESS（包 `3.5.16-1.1.1-searcher-pool-202606090844-33e59d0`） → 3 BE deploy + env var POOL_SIZE=4。

**实测**（c=20 90s + c=50 90s）：

| Config | QPS c=20 | QPS c=50 | thread state |
|---|---|---|---|
| **POOL=4** (实验) | **5.35** | 4.38 | R 9-15 / S 10625+（~0.1% R） |
| POOL=1（legacy 回退） | 5.97 | — | — |
| 历史 baseline（旧 build 6-shard） | 6.50 | — | — |

**结论**：POOL=4 **比 POOL=1 还差 10%**，验证 LuminaSearcher 之间并不能解套 lumina 内部 sync 瓶颈，反而引入 4× context 开销（4× quantizer + 4× graph 内存压力）。thread state 跟单 Searcher 时几乎一样（99.9% S）→ 瓶颈不在 Searcher 层。**回退 POOL=1**，等同 legacy 行为。

### 5.9.2 FE HttpPort = 18030（不是 8030）

`/api/profile?query_id=` 之前用 `http://fe:8030` 全部返回 0 bytes。`SHOW FRONTENDS` 真实 HttpPort 字段是 **18030**。

```sql
SHOW FRONTENDS;
-- HttpPort 18030  ← 实际值
```

**所有 query profile / FE web API 都用 18030**。memory 已更新。

### 5.9.3 SSH Bad fd 真根因（不是 VPN flap）

`ssh root@master` `Bad file descriptor` 出现 50+ 次：

- 误诊为 VPN flap → 30s retry 死循环浪费 30min
- 实际：Claude shell 子进程残留 8 个 zsh `until ssh ... do sleep` 死循环，从周六（5 天前）跑到今天，持续打开 socket 占 fd
- 表象：新 ssh `connect()` 返回 EBADF
- 解：`ps -ef | grep "zsh -c" | grep until | awk '{print $2}' | xargs kill -9`

memory `feedback_ssh_stale_sockets_after_net_switch.md` 记录完整 diagnose path + prevention。

### 5.9.4 Cross-region OSS endpoint 部署陷阱

stella tarball 在 `oss://olap-beijing/`（cn-beijing region），master 在 cn-hangzhou。OSS endpoint 必须用 **public** `oss-cn-beijing.aliyuncs.com` 不是 `-internal`。之前用 `-internal` 直接 i/o timeout 15min，validate chain 全 0 query。memory `reference_oss_region_endpoint.md` 记录。

### 5.9.5 Reader Cache cap=0 vs cap=16 对照

新 build + DataCache 已超热（193GB 跨 3 BE）的状态下：

| Config | c=20 90s QPS | eval_time_ms avg / p95 |
|---|---|---|
| **cap=16** | **5.95** | 240-243ms / 287-315ms |
| cap=0 | 5.79 | 241-243ms / 286-315ms |
| **Δ** | **+2.7%** | ≈ 0 |

今天的对照 reader cache 仅 +2.7%（不显著）。原因：DataCache disk tier 已 41-77GB/BE × 3，data 全 hot；reader cache 仅省 lumina Searcher::Open overhead（<5%），边际效应不明显。

**生产建议**：cap=16 保留（边际收益 + 几乎无内存代价：每 entry ~100MB，3 个 shard × 1 = ~300MB 实际占用）。

### 5.9.6 全 profile 直接证据 — 100% DataCache disk tier 命中（FINAL）

配置：POOL=1 + cap=16 + 全套 baseline config + warmup 10s c=1 → 主 bench c=20 90s with `SET GLOBAL enable_profile=true`。

**QPS = 6.39**（今天 best；旧 build baseline 6.50，差 ~1.7%）。

审计日志过滤 685 个 QueryIds（[T0, T1] + User=root + Db=vdbbench_perf + Stmt 'SELECT id FROM cohere_1m_cosine_seq6 ORDER BY approx_cosine_similarity'），HTTP API 抓 252 个完整 profile 聚合：

| 字段 | 聚合 | 每 query 平均 | 含义 |
|---|---|---|---|
| **DataCacheReadBytes** | **789.52 MB** | 3.13 MB | DataCache 命中读 |
| DataCacheReadDiskBytes | 789.52 MB | 3.13 MB | 全在 disk tier（mem tier 关） |
| DataCacheReadMemBytes | 0 B | 0 | mem tier 关闭 (`datacache_mem_size=0`) |
| DataCacheReadCounter | 3.7K reads | 15 reads/query | block 数 |
| DataCacheWriteBytes | **0 B** | 0 | cache 已热，0 populate |
| DataCacheSkipReadBytes | **0 B** | 0 | 0 字节 bypass |
| DataCacheReadPeerBytes | **0 B** | 0 | 0 跨 BE peer |
| paimonNativeReaderReadBytes | 236.2 KB | 960 B/query | 仅 manifest 元数据 |
| jniReaderReadBytes | 0 B | 0 | JNI Java reader 路径未走 |
| nativeReaderReadBytes | 0 B | 0 | native reader 路径未走 |
| **readBytes (storage layer)** | **0 B** | **0** | **0 字节 OSS 直读** ✅ |

**网络带宽实测**（c=20 90s 期间 /proc/net/dev RX/TX delta）：

```
BE 220: RX 195.73MB (1.00MB/s) TX 117.16MB (0.60MB/s)
BE 221: RX 195.65MB (1.00MB/s) TX 116.76MB (0.60MB/s)
BE 222: RX 199.06MB (1.02MB/s) TX 117.25MB (0.60MB/s)
```

3 BE 总 RX 590 MB × 196s = 3.0 MB/s aggregate；每 query 858 KB（含 FE-BE 协调 + 6 shard fan-out + 结果回传），**完全没有 OSS 数据拉取的 100s MB/s 量级**。

#### 三层 cache 链路实际状态

```
查询 → paimon-cpp
       ↓
       Layer 2 GlobalIndexReaderCache（cap=16）   ✅ +2.7% 边际收益
       ↓
       PaimonFileSystem CacheInputStream
       ↓
       Layer 1 starcache DataCache disk tier      ✅ 100% 命中（789MB / 252query）
       ↓
       Linux page cache (kernel RAM)              ✅ 把 disk IO 吸成 0
       ↓
       本地磁盘 / OSS                             ✅ readBytes=0 (storage layer)
```

#### 重要观测：DataCache mem tier 没启用

`datacache_mem_size = 0` → `DataCacheReadMemBytes = 0`, `DataCacheReadDiskBytes = 100%`。disk tier 在 Linux page cache 加持下已接近 RAM 性能；启用 mem tier 收益不明确（待 follow-up 实测）。

### 5.9.7 Final IO bottleneck verdict — IO 完全不是瓶颈

整合 2026-06-08（thread state 99.9% S）+ 今天的 profile + 网络数据：

| 层 | 实测 |
|---|---|
| OSS 网络读取 | 0 直读（readBytes=0 × 252 query） |
| DataCache disk tier | 命中 789 MB，3.13 MB/query |
| Linux page cache | 全吸（/proc/PID/io read_bytes=0） |
| 网络总带宽 | 3 MB/s — 完全是 FE-BE 协调 |

**瓶颈仍在 lumina 内部 sync wait**（thread 99.9% S，loadavg ~5%）— 待 paimon-cpp / lumina 团队从 `CountingSemaphore + CollectAllAwaiter::await_suspend` 入手。

> ⚠️ **此结论在 §5.9.8 被推翻** — 见下节。BE/lumina 不是瓶颈，是 FE Planner DLF REST。

### 5.9.8 真正瓶颈在 FE Planner DLF REST，不在 BE/lumina（**最终结论，颠覆之前所有 BE-side 假设**）

> ⚠️ **修订（2026-06-09）**：本节最初推测瓶颈含"DLFAuthProvider synchronized lock contention"。后续 jstack v2（c=20 稳态下 15 次采样，去掉 driver warmup 偏差）实测 **0 BLOCKED on DLFAuthProvider lock**，而是观察到 **19 distinct DLFAuthProvider instances**（每个对应一个 mysql worker 线程，各自 hold 自己的 monitor）。源码追到 `SnapshotLoaderImpl.load()` 使用 `try (Catalog catalog = catalogLoader.load())`，每次调用 `catalogLoader.load()` = `new RESTCatalog(...)` = **per-call 新建 RESTApi + 新建 DLFAuthProvider**。真正瓶颈是 fresh DLFAuthProvider 触发 `DLFECSTokenLoader.loadToken()` → ECS metadata HTTP RTT。结论部分（FE Planner 而非 BE）不变，**修复方向**改为 query-scope 缓存 metadata（见 §5.9.9）。

**触发**：拉一个完整 query profile（QueryId `858032d8`，Total 3.205s）做逐字段分析，发现 BE Execution 只占 0.7%。

#### 单 query 时间分解（c=20 期间采的）

| 阶段 | 耗时 | 占比 |
|---|---|---|
| **FE Planner** | **3.165s** | **98.8%** |
| BE Execution (Pipeline+Scan) | 22.7ms | 0.7% |
| Deploy (RPC dispatch) | 9ms | 0.3% |
| Parser / Pending / Prepare | ~8ms | 0.2% |

#### FE Planner 内部分解（3.165s）

| 步骤 | 耗时 | 备注 |
|---|---|---|
| Parser | 3ms | OK |
| **ApplyTopNIndexRule::check (1st, Pre-rule)** | **464ms** | 含 ScanIndexFileEntries 160ms |
| **ApplyTopNIndexRule::transform (1st)** | **403ms** | 含 ScanIndexFileEntries 190ms |
| Transformer | 3ms | |
| **Optimizer.RuleBaseOptimize** | **1.101s** | |
| ↳ **ApplyTopNIndexRule::check (2nd)** | **584ms** | **DUPLICATE** — 同样的 rule 在 Optimizer 内再跑一次 |
| ↳ **ApplyTopNIndexRule::transform (2nd)** | **515ms** | **DUPLICATE** |
| **ExecPlanBuild** | **1.187s** | |
| ↳ ExecPlanBuild (recursive) | 364ms | |
| ↳ **getPaimonRemoteFileInfos** | **530ms** | 读 paimon manifest 列出 6 个 data file |

**重复浪费**：ApplyTopNIndexRule check + transform 跑 2 次 = 1.97s；ScanIndexFileEntries 跑 4 次 = 656ms 累计。

#### BE Execution 分解（22.7ms） — 几乎可忽略

```
QueryExecutionWallTime: 22.7ms
├─ ScanTime (PAIMON_SCAN):   6.73ms   ← BE 实际 ANN scan
│  ├─ IOTaskExecTime:        6.68ms   (6 shard 并行)
│  ├─ ColumnReadTime:        5.05ms
│  ├─ DataCacheReadBytes:    3.13MB
│  ├─ DataCacheReadTimer:    243μs    ← cache 命中极快
│  └─ OpenFile + ReaderInit: ~2.5ms
├─ QueryPeakScheduleTime:    19.48ms
└─ InputEmptyTime:           16.32ms  ← BE worker 等 FE 派发
```

#### 进一步 FE jstack 验证 — DLF REST 调用链

5 次 jstack 抓到 `ApplyTopNIndexRule.checkImpl` 活跃 2 次（与 ~1s/3s=33% 概率吻合）。完整调用栈：

```
ApplyTopNIndexRule.checkImpl
  IndexAnalyzer.canUseTopNIndex
    PaimonMetadata.checkGlobalIndexAvailable
      PaimonMetadata.getIndexShardList
        IndexFileHandler.scanEntries                  ← 这就是 "ScanIndexFileEntries"
          SnapshotManager.latestSnapshot
            RESTCatalog.loadSnapshot
              RESTApi.loadSnapshot
                HttpClient.get
                  RESTAuthFunction.apply
                    DLFAuthProvider.mergeAuthHeader
                      synchronized DLFAuthProvider.refreshToken    ← LOCKED
                        DLFECSTokenLoader.loadToken
                          SimpleHttpClient.get
                            CloseableHttpClient.execute            ← 真正 HTTP 调用
```

**ScanIndexFileEntries 不是 OSS roundtrip**，而是 **DLF REST API HTTP 调用 + 同步 token refresh + DLFAuthProvider synchronized lock**。

#### 另一条嵌入 SQL 调用链（jstack #3）

```
StatementPlanner.plan
  PlanFragmentBuilder.visitPhysicalPaimonScan
    PaimonScanNode.setupScanRangeLocations
      PaimonGlobalIndexService.evaluate
        InternalSqlExecutor.runDQL                    ← Planner 内嵌另一个 SQL
          DefaultCoordinator.getNext
            ResultReceiver.getNext
              ProtobufRpcProxy.doWaitCallback         ← 等 BE 回包
```

FE Planner 阶段触发 internal SQL，让 BE 实际跑 TopN 评估，**FE 同步等 RPC**。这条 530ms 是 `cohere_1m_cosine_seq6.getPaimonRemoteFileInfos`。

#### c=1 vs c=20 latency 对比 — 验证并发争用

| 场景 | 实测 latency | 与 BE 工作量对比 |
|---|---|---|
| c=1 单 query | **770–1085 ms** (avg 937 ms) | BE 实际 22.7ms 工作，其余 ~915ms FE Planner |
| c=20 profile | 3205 ms | latency × 3.4 — 并发争用 DLF REST + Lock |

DLFAuthProvider synchronized block + DLF REST endpoint 同步 → c=20 并发都串行排队等这条链 → latency 涨 3.4x。

#### QPS plateau 数学吻合

| conc | avg_lat | conc/avg_lat | 观察 QPS |
|---|---|---|---|
| 1 | 0.94s | 1.06 | (没测，估 1) |
| 20 | 3.20s | 6.25 | **6.39** ✓ |
| 40 | 6.05s | 6.61 | 6.58 ✓ |
| 80 | 12.09s | 6.61 | 6.57 ✓ |

完全 closed-loop M/M/k 排队模型。**DLF REST + DLFAuthProvider 串行 throughput ceiling ~6.5 QPS（4 calls/query × ~26 calls/s/lock）**。

#### 之前结论全部修正

| 之前的结论 | 修正后的真相 |
|---|---|
| "lumina internal sync wait" | BE 在 idle 等 FE 派发 |
| "thread 99.9% S → lumina coroutine spinning" | BE worker thread S 是闲着等 RPC，不是 lumina 自旋 |
| "loadavg 5% 是 lumina 让出 CPU" | BE 本来工作就极少（22ms/query × 6.4 QPS = 0.14 core）|
| "SearcherPool 源码改动" | 方向完全错 — 瓶颈在 FE Java 端，paimon-cpp 改不到 |
| "DataCache 命中是关键" | 已 100% 命中，与瓶颈无关 |
| 6.5 plateau 由 BE 限制 | 由 FE Planner DLF REST 限制 |

#### 真正的优化方向（按 ROI）

| P | 改动 | 位置 | 预期收益 |
|---|---|---|---|
| **P0** | **FE 缓存 DLF snapshot 结果**（query 间 reuse，TTL 1min） | StarRocks `PaimonMetadata.getIndexShardList` 加 LRU cache | **3-10x QPS** |
| **P0** | **去重 ApplyTopNIndexRule 调用**（pre-rule + Optimizer 内重复跑同样的逻辑） | StarRocks `RewriteTreeTask` / `QueryOptimizer` | **2x QPS** |
| **P0** | **paimon Java side `DLFAuthProvider` token 缓存优化** | paimon-java `DLFAuthProvider.refreshToken` | 消除 synchronized 串行 |
| **P1** | DLF REST `/loadSnapshot` 客户端 cache | paimon-java `RESTCatalog` 层 | 减少 HTTP roundtrip |
| **P2** | FE Planner 内嵌 SQL（setupScanRangeLocations）改 async | StarRocks `PaimonScanNode` | 不阻塞主 plan 线程 |

#### 移交对象

不是 paimon-cpp / lumina 团队，而是：
- **StarRocks FE 主线团队**（`com.starrocks.sql.optimizer.rule.transformation.ApplyTopNIndexRule`, `PaimonMetadata.getIndexShardList`）
- **paimon-java 团队**（`org.apache.paimon.rest.auth.DLFAuthProvider`, `org.apache.paimon.rest.RESTCatalog.loadSnapshot`）

#### 完整证据档案

- `docs/one_profile.txt`（已下载到本地，1100 行完整 profile）
- master `/tmp/benchmark/results/fe_jstack/jstack_{1..5}.txt`（5 次 jstack 含 ApplyTopNIndexRule 调用栈）
- master `/tmp/benchmark/results/verify_summary.txt`
- master `/tmp/benchmark/results/jstack_c20_v2/jstack_{1..15}.txt`（c=20 稳态 15 次采样，证伪 DLFAuthProvider lock contention 假设）

### 5.9.9 P0-2 落地：PaimonMetadata query-scope cache —— 实施 + A/B 验证（**+96% QPS @ c=20**）

> 修订 §5.9.8 推断后的第一步实施。本节记录代码改动、build/deploy 流程、A/B 对照结果。

#### 真正根因（修正 §5.9.8 推断）

通过 jstack v2 + paimon-java 源码追踪定位：

```text
PaimonMetadata.getGlobalIndexes (StarRocks FE)
  → IndexFileHandler.scanEntries
  → SnapshotManager.latestSnapshot
  → SnapshotLoaderImpl.load()
       ┌── try (Catalog catalog = catalogLoader.load()) {       ← 每次 new
       │       └── RESTCatalogLoader.load()
       │             └── return new RESTCatalog(context, false)
       │                   └── this.api = new RESTApi(...)
       │                         └── createAuthProvider(...)
       │                               └── new DLFAuthProvider()  ← token=null
       │
       └── catalog.loadSnapshot(identifier)
             → RESTApi.loadSnapshot
             → HttpClient.getHeaders → RESTAuthFunction.apply
             → DLFAuthProvider.mergeAuthHeader
             → getFreshToken() {
                   shouldRefresh()=true   (token==null, cold start)
                   sync(this)             ← own monitor, 0 contention
                   refreshToken()
                   → DLFECSTokenLoader.loadToken
                   → SimpleHttpClient.get(ECS_metadata_url)  ← HTTP RTT 是大头
                 }
```

关键源码（paimon `tag/SnapshotLoaderImpl.java:44-52`，未做改动）：

```java
public Optional<Snapshot> load() throws IOException {
    try (Catalog catalog = catalogLoader.load()) {   // ← 每次 new RESTCatalog
        return catalog.loadSnapshot(identifier).map(TableSnapshot::snapshot);
    }
}
```

`RESTCatalogLoader.load()` 直接 `return new RESTCatalog(context, false)`，不复用 instance。

每 query c=20 期间 19 个线程**各自**走完整 cold-start auth 链 → 19 个 ECS metadata HTTP 请求并发打 metadata service。这才是 QPS plateau 6.5 的源头，不是 lock。

#### 实施：StarRocks FE 内存级 query-scope cache

`PaimonMetadata` 实例由 `MetadataMgr.metadataCacheByQueryId` 按 queryId 缓存，**同一个 query 内多次 `getOptionalMetadata()` 返回同一实例**。在该实例上加 ConcurrentHashMap 缓存：

```java
// fe/fe-core/src/main/java/com/starrocks/connector/paimon/PaimonMetadata.java
private final Map<Identifier, Optional<List<Range>>> indexShardListCache = new ConcurrentHashMap<>();
private final Map<Identifier, Map<String, Set<String>>> globalIndexesCache = new ConcurrentHashMap<>();

private List<Range> getIndexShardList(Table table) {
    if (!Config.enable_paimon_global_index_metadata_query_cache) {
        return computeIndexShardList(table);
    }
    Identifier id = Identifier.create(table.getCatalogDBName(), table.getCatalogTableName());
    return indexShardListCache
            .computeIfAbsent(id, k -> Optional.ofNullable(computeIndexShardList(table)))
            .orElse(null);
}
// 同模式包装 getGlobalIndexes(...)
```

Cache 生命周期 = PaimonMetadata 实例生命周期 = 单 query。**跨 query 不复用 → 0 staleness 风险**。

`ApplyTopNIndexRule.check` + `.transform` 两阶段（详见 §5.9.8 重复表）合计 4 次 `latestSnapshot()` REST roundtrip → **1 次**。

Gated by mutable FE config `enable_paimon_global_index_metadata_query_cache`（默认 `true`），支持 `ADMIN SET FRONTEND CONFIG` 热改回 false 做对照 / 紧急回滚。

#### 落地工程

| 项目 | 值 |
|---|---|
| Commit | `4af108e10b58e86f57fe1501fe7f6d4a3025017a` |
| Branch | `xiaolong/bugfix-paimon-global-index-topn-null-deref` |
| Jenkins build | `emr-olap-starrocks-35-develop #765`（FE-only，~6.7 min） |
| OSS tarball | `oss://olap-beijing/starrocks-develop/xiaolong/starrocks-3.5.16-1.1.1-paimon-gi-meta-cache-202606091711.tar.gz` |
| 部署方式 | FE-only：替换 `/opt/apps/STARROCKS3/starrocks-current/fe/lib`，备份在 `fe/lib.bak.20260609172718`；BE 不动 |
| 回滚 | `mv lib lib.broken; mv lib.bak.20260609172718 lib && restart_fe`（一行命令）|

#### A/B 实测：`cohere_1m_cosine_seq6` c=20（60s bench，duration 后取稳态）

| Metric | cache_off（baseline）| cache_on（P0-2）| Δ |
|---|---|---|---|
| **QPS** | **6.54** | **12.80** | **+96 %（×1.96）** |
| p50 latency | 2992 ms | 1503 ms | −50 % |
| p95 | 3706 ms | 2004 ms | −46 % |
| p99 | 3883 ms | 2202 ms | −43 % |
| c=1 warmup 均值 | 1083 ms/q | 784 ms/q | −28 % |
| 60s 总 query 数 | 405 | 780 | +93 % |

跟 §5.9.8 推演完全吻合：4 → 1 REST roundtrip / query，单 query 节省约 300ms，c=20 并发下 ECS metadata service 不再被打满，QPS 接近翻倍。p99 同步压缩 −43%，长尾收敛。

#### A/B 实测：`cohere_1m_cosine_seq6` c=50

| Metric | cache_off | cache_on | Δ |
|---|---|---|---|
| **QPS** | **6.71** | **14.36** | **+114 %（×2.14）** |
| p50 | 7377 ms | 3399 ms | −54 % |
| p95 | 8491 ms | 4395 ms | −48 % |
| p99 | 8812 ms | 4841 ms | −45 % |
| count / 60s | 430 | 893 | +108 % |

cache_off 在 c=50 时仍卡在 ~6.7 QPS（与 c=20 的 6.54 几乎一致）—— 实测确认 §5.9.8 推断的"DLF REST + per-query new auth chain throughput ceiling ~6.5 QPS"是结构性瓶颈，与并发度无关。cache_on 把这个 ceiling 解除后，c=50 提升到 14.36 QPS，p99 同步压缩接近一半。

#### A/B 实测：`cohere_1m_cosine`（single-shard）c=20 / c=50

| conc | Metric | cache_off | cache_on | Δ |
|---|---|---|---|---|
| 20 | QPS | 6.19 | **13.49** | **+118 %（×2.18）** |
| 20 | p50 | 3098 ms | 1430 ms | −54 % |
| 20 | p95 | 4186 ms | 1890 ms | −55 % |
| 20 | p99 | 4601 ms | 2198 ms | −52 % |
| 20 | count / 60s | 383 | 824 | +115 % |
| 50 | QPS | 6.65 | **13.51** | **+103 %（×2.03）** |
| 50 | p50 | 7306 ms | 3505 ms | −52 % |
| 50 | p95 | 8226 ms | 4700 ms | −43 % |
| 50 | p99 | 8442 ms | 5019 ms | −41 % |
| 50 | count / 60s | 447 | 844 | +89 % |

single-shard 与 seq6（6-shard）行为一致：
- baseline 6.2-6.7 QPS plateau 横跨 c=20 / c=50 / 单/六 分片配置 → 强证据"DLF auth chain throughput ceiling ~6.5 QPS"是结构性瓶颈，与分片数 + 并发度都无关
- cache_on 后 single-shard 也几乎翻倍（×2.0-2.2），跟 seq6 收益级别完全一致
- single-shard c=50 vs c=20 QPS 差异极小（13.51 vs 13.49）→ 1M data 单分片 BE 容量上限触顶，瓶颈下移到 BE-side ANN compute

#### P0-B 收益汇总（4 表 × 2 并发）

| 数据集 | conc | baseline QPS | P0-B QPS | Δ |
|---|---|---|---|---|
| seq6 (6-shard) | 20 | 6.54 | **12.80** | **+96 %** |
| seq6 (6-shard) | 50 | 6.71 | **14.36** | **+114 %** |
| single-shard | 20 | 6.19 | **13.49** | **+118 %** |
| single-shard | 50 | 6.65 | **13.51** | **+103 %** |

所有 4 组对照中 baseline 都卡在 6.2-6.7 QPS（DLF auth chain ceiling），P0-B 全部突破到 12.8-14.4，**+96-118 %**。p50/p95/p99 同步压缩 41-55 %。

#### 完整证据档案（§5.9.9）

- 源代码改动：`fe/fe-core/src/main/java/com/starrocks/connector/paimon/PaimonMetadata.java`, `Config.java`, `docs/{en,zh}/administration/management/FE_parameters/shared_lake_other.md`
- A/B 结果：master `/tmp/benchmark/results/p0_2_ab_173012/{cache_on,cache_off}.json`
- 综合验证：master `/tmp/benchmark/results/p0_2_full_173957/`

### 5.9.10 P0-C 落地：paimon-java DLFECSTokenLoader 进程级 token cache（**累计 ×4 QPS**）

> 在 P0-B 已落地的基础上，验证"真正瓶颈推到 loadSnapshot REST 本身 + 每次 fresh DLFAuthProvider 触发的 ECS metadata token HTTP fetch"假设。

#### 真正剩余瓶颈（P0-B 之后的链路）

P0-B 之后，每 query 在 FE Planner 内 `latestSnapshot()` 从 4 次降到 1 次。这 1 次依然包含：
1. `RESTCatalogLoader.load()` → `new RESTCatalog(context)` → `new RESTApi(...)` → `new DLFAuthProvider(...)` —— 全新 instance，token=null
2. `mergeAuthHeader` → `getFreshToken()` → `shouldRefresh()=true` → `synchronized(this)` → `refreshToken()` → `DLFECSTokenLoader.loadToken()` → `SimpleHttpClient.get(ECS_metadata_url)` —— ECS metadata HTTP fetch
3. `loadSnapshot` 本身的 REST HTTP

c=50 时 19+ 并发线程都各自走完整链 → 同时 fetch token from ECS metadata service → 形成第二个 throughput ceiling。

#### 实施：DLFECSTokenLoader 进程级静态 token cache

paimon-java fork `1-ali-26.1-lake-optimizer` 上的改动，在 `DLFECSTokenLoader` 加进程级静态 `ConcurrentMap<String, TokenCacheEntry>`，key 是 `ecsMetadataURL + "::" + roleName`，single-flight 刷新（每 key 一个 `synchronized(entry.lock)` 块），过期阈值与 `DLFAuthProvider.shouldRefresh()` 一致（`TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 1 hour`）。

```java
// paimon-api/.../DLFECSTokenLoader.java
private static final ConcurrentMap<String, TokenCacheEntry> SHARED_TOKEN_CACHE = new ConcurrentHashMap<>();

private static final class TokenCacheEntry {
    volatile DLFToken token;
    final Object lock = new Object();
}

@Override
public DLFToken loadToken() {
    if (roleName == null) {
        roleName = getRole(ecsMetadataURL);
    }
    String cacheKey = ecsMetadataURL + "::" + roleName;
    TokenCacheEntry entry = SHARED_TOKEN_CACHE.computeIfAbsent(cacheKey, k -> new TokenCacheEntry());

    DLFToken cur = entry.token;
    if (cur != null && !needsRefresh(cur)) return cur;
    synchronized (entry.lock) {
        cur = entry.token;
        if (cur != null && !needsRefresh(cur)) return cur;
        DLFToken fresh = getToken(ecsMetadataURL + roleName);
        entry.token = fresh;
        return fresh;
    }
}
```

不动 `DLFAuthProvider`（其 instance-level token + synchronized 不变；它内部 `tokenLoader.loadToken()` 这一跳现在拿全局 cache 命中结果）。不动 `DLFTokenLoader` 接口，不动 `DLFLocalFileTokenLoader`。

#### 部署工程

| 项目 | 值 |
|---|---|
| Paimon 仓库 | `/Users/drake_wang/workspace/alibaba/paimon`，version `1-ali-26.1-lake-optimizer` |
| 改动文件 | `paimon-api/src/main/java/org/apache/paimon/rest/auth/DLFECSTokenLoader.java` |
| Build 命令 | `mvn clean package -DskipTests -pl paimon-bundle -am`（~56s） |
| 产物 jar | `paimon-bundle/target/paimon-bundle-1-ali-26.1-lake-optimizer.jar`（46 MB，含 paimon-api 在内的 shaded uber jar） |
| 部署方式 | scp 到 master，备份原 jar 到 `paimon-bundle-*.jar.bak.20260609180632`，替换 `/opt/apps/STARROCKS3/starrocks-current/fe/lib/paimon-bundle-1-ali-26.1-lake-optimizer.jar`，重启 FE |
| 启动 | `JAVA_HOME=/usr/lib/jvm/java-17 start_fe.sh --daemon` |
| 回滚 | `mv $JAR $JAR.broken; mv $JAR.bak.20260609180632 $JAR; restart` |
| 验证 | FE alive 5s，sanity query OK |

#### A/B 实测：4 组合（P0-B × P0-C）

| 数据集 | conc | 历史 baseline (B off + C off) | P0-B only (B on + C off) | **P0-C only (B off + C on)** | **P0-B+C (B on + C on)** | P0-C only 单独 Δ vs baseline | 累计 Δ vs baseline |
|---|---|---|---|---|---|---|---|
| seq6 (6-shard) | 50 | 6.71 | 14.36 | **12.997** | **26.97** | **+94 %** | **×4.02** |
| single-shard | 20 | 6.19 | 13.49 | **12.21** | **23.52** | **+97 %** | **×3.80** |
| single-shard | 50 | 6.65 | 13.51 | **13.90** | **28.80** | **+109 %** | **×4.33** |

延迟（seq6 c=50）：

| Metric | baseline | P0-B only | P0-C only | P0-B+C | P0-B+C vs baseline |
|---|---|---|---|---|---|
| QPS | 6.71 | 14.36 | 12.997 | **26.97** | **+302 %** |
| p50 | 7377 ms | 3399 ms | 3682 ms | **1716 ms** | **−77 %** |
| p95 | 8491 ms | 4395 ms | 5185 ms | **2716 ms** | **−68 %** |
| p99 | 8812 ms | 4841 ms | 5645 ms | **3262 ms** | **−63 %** |

延迟（single-shard，最显著）：

| conc | Metric | baseline | P0-B only | P0-C only | P0-B+C | P0-B+C vs baseline |
|---|---|---|---|---|---|---|
| 20 | QPS | 6.19 | 13.49 | 12.21 | **23.52** | **+280 %** |
| 20 | p50 | 3098 ms | 1430 ms | 1587 ms | **809 ms** | **−74 %** |
| 20 | p99 | 4601 ms | 2198 ms | 2556 ms | **1396 ms** | **−70 %** |
| 50 | QPS | 6.65 | 13.51 | 13.90 | **28.80** | **+333 %** |
| 50 | p50 | 7306 ms | 3505 ms | 3484 ms | **1699 ms** | **−77 %** |
| 50 | p99 | 8442 ms | 5019 ms | 4869 ms | **2441 ms** | **−71 %** |

#### 关键观察

1. **P0-B 和 P0-C 几乎独立可乘**：单独 P0-B ×2.14，单独 P0-C ×1.94，组合 ×4.02 ≈ ×2.14 × ×1.94 = ×4.15。说明二者攻击的是 query 路径上**不同的 latency 源头**：
   - P0-B：减少 query 内 `latestSnapshot()` 调用次数（4 → 1，约 4×）
   - P0-C：减少每次调用 cold-start 一次 ECS token HTTP 的开销（每次 call 内部缩短 ~一半）
2. **token cache 收益独立显现**：即使 P0-B disabled（B off + C on），单独 P0-C 已经把 baseline 6.71 → 12.997，几乎与单独 P0-B 收益齐平。
3. **后续瓶颈下移到 FE Optimizer CPU 与 `loadSnapshot` REST**：c=50 时 p50=1716ms，约 1.7s 之中 BE 仅 ~22ms（profile 旧值），剩 ~1.68s 全在 FE。token 已 cache，剩余 HTTP 仅 1 次 `loadSnapshot`，其余应是 Optimizer / ExecPlanBuild CPU。需重 profile 验证。
4. **P0-D（跨 query snapshot LRU cache）收益预期已缩水**：之前 plan 估 ×5+，按当前 1 次 REST + cpu-bound 比例推算实际收益 ×1.2-1.5（QPS 27 → 32-40）。staleness 风险维持，opt-in 价值在剩余 HTTP 依赖消除。

#### 完整证据档案（§5.9.10）

- 源代码改动：`paimon-api/src/main/java/org/apache/paimon/rest/auth/DLFECSTokenLoader.java`（fork `1-ali-26.1-lake-optimizer`）
- A/B 结果：master `/tmp/benchmark/results/p0_c_full_180741/{seq6,ss}_*.json`
- Jar 备份（回滚）：master `/opt/apps/STARROCKS3/starrocks-current/fe/lib/paimon-bundle-1-ali-26.1-lake-optimizer.jar.bak.20260609180632`
- Post P0-B+C profiles（5 个独立 query）：`docs/profiles_p0bc/profile_p0bc_*.txt`

### 5.9.11 BE perf record post P0-B+C — 真正 BE hot path 终于可见（**lumina ANN 32-37%，kernel sched 21-23%**）

P0-B+C 后 FE 不再 bottleneck，BE 第一次进入真正繁忙状态。c=50 sustained bench（seq6, max_qps=27.94），3 worker `starrocks_be` 并行 `perf record -F 99 -g` 60 s。

#### Top symbols 汇总（3 BE 一致）

| Category | BE-220 | BE-221 | BE-222 | 性质 |
|---|---|---|---|---|
| **lumina ANN compute** | | | | |
| ↳ `DiskANNSearcherBackend::ExpandNodes`（graph walk）| 18.6 % | 14.0 % | 14.0 % | C++ user-space |
| ↳ `EvalIPAvx512`（SIMD inner-product 距离）| 10.5 % | 12.3 % | 11.9 % | AVX-512 vectorized |
| ↳ `SelectExpandNodes`（top-K beam）| 7.8 % | 6.1 % | 5.9 % | C++ user-space |
| **小计 ANN compute** | **37 %** | **32 %** | **32 %** | **fundamental work**|
| **kernel scheduling + lock** | | | | |
| ↳ `__sched_yield`（coroutine yield）| 8.4 % | 8.6 % | 8.8 % | userspace → kernel |
| ↳ `syscall_enter_from_user_mode` | 7.5 % | 8.7 % | 9.1 % | kernel entry |
| ↳ `__schedule` + `finish_task_switch` | 3.4 % | 3.6 % | 3.4 % | kernel sched |
| ↳ `__lock_text_start` | 1.7 % | 2.3 % | 2.0 % | kernel spinlock |
| **小计 scheduling/lock** | **21 %** | **23 %** | **23 %** | **接近 ANN compute 的 2/3** |
| **memory + userspace lock** | | | | |
| ↳ `my_malloc` + `my_free`（jemalloc）| 1.0 % | 4.2 % | 4.1 % | |
| ↳ `__pthread_mutex_unlock` + `__lll_lock_wait` | 2.1 % | 2.7 % | 2.9 % | |
| **小计** | **3 %** | **7 %** | **7 %** | BE-221/222 比 BE-220 显著高 |
| ZSTD decompress | 1.3 % | 1.4 % | 1.6 % | 已不显著（§5.8 ZSTD 假设解除）|

#### 与 Round 8（c=20 FE-bound 时）对比

| 指标 | Round 8 (c=20, FE bottleneck, BE idle) | Round P0BC (c=50, FE 不 bottleneck, BE 真忙)| 变化 |
|---|---|---|---|
| `__sched_yield` | **24 %** | **8.4-8.8 %** | **−65 %** —— 比例下降但绝对仍 top 5 |
| `ExpandNodes` | 较低（BE 多在等）| 14-19 % | 上升，真正 ANN 工作显形 |
| `EvalIPAvx512` | 较低 | 11-12 % | 上升 |
| `__lock_text_start` | 较高 | 1.7-2.3 % | 下降（FE 不再触发 wait）|
| ZSTD | 1-2 % | 1.3-1.6 % | 稳定 |

**关键判断**：之前 Round 8 / §5.8 关于 lumina sched_yield 是 binding constraint 的结论，是在 BE idle 状态下的 perf 假象。c=50 + FE 解除 bottleneck 后，sched_yield 相对占比降到 8 %，**真正的 BE 工作（ExpandNodes + EvalIPAvx512 + SelectExpandNodes）成为 32-37 % 的大头**。这是健康的 ANN workload 状态。

#### 修正后的下一步（按 ROI 排序）

| 优先级 | 改动 | 预期收益 | 团队 | 备注 |
|---|---|---|---|---|
| **P1-A** | lumina coroutine 调度优化（减少 sched_yield + syscall_enter） | +5-10 % QPS | paimon-cpp / lumina | 之前 SearcherPool 实验失败，需重新设计 batching 或 yield-less 路径 |
| **P1-B** | DiskANN `beam_width` / `list_size` sweep（task #93）| +5-15 % QPS（recall trade-off） | 配置层 | 通过 `ann_params` JSON + `top_index_local_rows` 调节，无需重 build |
| **P1-C** | BE scan path my_malloc/my_free 4-7 %（BE-221/222 显著高于 BE-220）| +2-5 % | stella BE | 复用 chunk buffer / column pool |
| **P2** | `__lock_text_start` 2-3 % kernel lock 来源（perf annotate 看具体 syscall） | +1-3 % | 调研 |
| **P2** | ZSTD page cache（1.3-1.6 %）| <2 % | 已基本无空间 |

#### 完整证据档案（§5.9.11）

- 本地：`docs/perf_p0bc/perf_{symbol,callgraph}_10.105.221.{220,221,222}.txt`（6 个文件，已发给 user）
- 本地：`docs/perf_p0bc/bench.json`（max_qps=27.94 @ c=50 seq6）
- 本地：`docs/perf_p0bc/run.log`
- master：`/tmp/benchmark/results/perf_p0bc_195132/`

### 5.9.12 三层 P0 优化汇总（**total ×4 QPS, FE 5-7% wall, BE 真正成 dominator**）

至此连续三层优化全部落地 + 实测验证：

| 优先级 | 改动位置 | 改动 | 累积 QPS @ c=50 seq6 | 累积 p99 |
|---|---|---|---|---|
| **baseline** | — | — | 6.71 | 8812 ms |
| **P0-B** | StarRocks FE `PaimonMetadata` | query-scope 缓存 `getIndexShardList` / `getGlobalIndexes`（4 次 REST → 1 次） | 14.36 (×2.14) | 4841 ms (−45 %) |
| **P0-C** | paimon-java `DLFECSTokenLoader` | 进程级静态 token cache（消除 fresh AuthProvider 的 ECS HTTP fetch） | **26.97 (×4.02)** | **3262 ms (−63 %)** |

延迟视角（seq6 c=50）：
- p50：7377 ms → 3399 ms → **1716 ms**（−77 %）
- p99：8812 ms → 4841 ms → **3262 ms**（−63 %）

性能视角 across 4 表 × 并发：

| 数据集 | conc | baseline | P0-B+C | 累积 Δ |
|---|---|---|---|---|
| seq6 (6-shard) | 20 | 6.54 | 12.80*¹ | +96 % |
| seq6 (6-shard) | 50 | 6.71 | **26.97** | **+302 %** |
| single-shard | 20 | 6.19 | **23.52** | **+280 %** |
| single-shard | 50 | 6.65 | **28.80** | **+333 %** |

*¹ 只测了 P0-B only 数据（13.49→对应 c=20 P0-B+C 应 ≥ 21+），未单独跑 P0-B+C c=20 seq6

剩余优化空间约 +5-30 %（受 lumina coroutine 调度 + DiskANN 参数调优 + BE scan path 内存复用 3 项叠加上限约束）。FE / auth chain 已无 actionable 空间。

#### Post-P0-B+C profile：dominator 转移到 BE，FE Planner ×30 reduction

部署完成后，连续跑 5 个独立 ANN query（cohere_1m_cosine_seq6），拉 profile 锁定下一个 dominating 项：

| Profile | Total | **FE Planner** | `ApplyTopNIndexRule::check` (1st) | `getPaimonRemoteFileInfos` | **BE QueryCumulativeScanTime** | BE % wall |
|---|---|---|---|---|---|---|
| #1（cold）| 3.06 s | 825 ms | 471 ms（含 token cold fetch + first REST）| 349 ms | 1.86 s | 72 % |
| #2 | 2.12 s | **109 ms** | 28 ms | 77 ms | 1.42 s | **94 %** |
| #3 | 2.03 s | 97 ms | 17 ms | 76 ms | 1.56 s | 95 % |
| #4 | 2.86 s | 97 ms | 17 ms | 76 ms | 1.39 s | 96 % |
| #5 | 2.19 s | 86 ms | 22 ms | 59 ms | 1.50 s | 95 % |

**关键观察**：

1. **FE Planner 从 3165 ms → 86-109 ms（×30 reduction）**。`ApplyTopNIndexRule::check` 第 2 次（在 Optimizer 内）和 `ScanIndexFileEntries` 其他 4 处全部 0 ms —— P0-B query-scope cache 命中。
2. **Profile #1 (cold) Planner 825 ms 是一次性现象**：包含进程级 token cold fetch（首次 ECS metadata HTTP）+ 首次 loadSnapshot REST。之后所有 query 都是 warm 状态。
3. **`getPaimonRemoteFileInfos` 缩到 59-77 ms**：这是 FE 计算 scan range 的内部步骤，不是网络 RTT。CPU bound，已经不显著。
4. **新 dominator 是 BE**：`QueryCumulativeScanTime` 1.39-1.56 s，DriverTotalTime 1.92-2.74 s，**BE 占 wall 94-96 %**。

#### P0-D 重新评估 → 放弃

跨 query LRU cache snapshot 的目标是省 `ApplyTopNIndexRule::check` 的 17-28 ms（warm 状态）/ 471 ms（cold）。
- Warm 场景：17-28 ms / 2000-2900 ms wall ≈ **0.6-1.4 %** 单 query 提升
- Cold 场景：占比大但只一次性，不在 sustained QPS bench 中显现
- Cache TTL staleness 风险维持
- **结论：P0-D 完全不值得做**。

#### 下一步建议（已不在 FE/Planner 范围）

真正剩余瓶颈在 BE ANN 计算（94-96 % wall）。需要：

| 优先级 | 行动 | 位置 | 预期收益 |
|---|---|---|---|
| **P1** | 重 perf record on BE during c=20/c=50 bench（**FE 不再是 bottleneck，BE 才是真正 hot path**） | master + 3 worker `starrocks_be` perf | 信息收益 |
| **P1.X** | 复审 lumina sched_yield 在 c=50 真负载下的占比（之前 c=20 perf 数据 BE 是 idle，结论不可信） | paimon-cpp perf 数据 | 信息 |
| **P2** | DiskANN beam_width × list_size sweep（task #93）| BE config | +10-30 % |
| **P2** | per-shard topN 减小（`top_index_local_rows`）| session var | +5-15 % |

记号：之前 §5.9 / §5.8 关于 BE-side lumina sched_yield、SearcherPool、coroutine 等结论都基于 c=20 期间 BE idle 的 perf 数据，**全部失效**。FE 不再是 bottleneck 后，BE 的真实 hot path 还未被采集。

### 5.9.13 方法学修正：30 ad-hoc profile 实为 brute-force，FE Planner 才是 ANN 真 dominator

> 验证过程中发现的关键校正。Bench 数据有效，但 §5.9.11 内"FE Planner 5-7% / BE 94-96% wall"是基于错误 SQL pattern 测出的 brute-force profile，应**仅适用于无 ANN 下推的 brute-force ORDER BY 工作流**，不适用于 ANN。

#### 触发：再跑一次 single-query profile 时发现 SQL pattern 错误

测 `cohere_1m_cosine` 用 `SELECT id FROM t ORDER BY cosine_similarity(vector, array_repeat(CAST(0.01 AS FLOAT), 768)) DESC LIMIT 10` —— 想要的 warm latency 是 2.3s。Profile 显示：

- ApplyTopNIndexRule::check 19 ms，**`transform` 未运行**
- CONNECTOR_SCAN: `RawRowsRead: 1,000,000`，`DataCacheReadBytes: 2.659 GB`
- Plan: `PaimonScanNode → Project(cosine_similarity) → TopN`，**无任何 PaimonGlobalIndex 节点**

→ 这是 brute-force 全表 ORDER BY，**没有 ANN 索引下推**。

#### 根因：`IndexAnalyzer` 触发条件 vs 测试 SQL 不匹配

读 `fe/fe-core/src/main/java/com/starrocks/connector/index/IndexAnalyzer.java:56-60` + `:141-169`：

```java
private static final Set<String> APPROX_VECTOR_DISTANCE_FUNCTIONS = Set.of(
    FunctionSet.APPROX_COSINE_SIMILARITY,    // approx_cosine_similarity
    FunctionSet.APPROX_INNER_PRODUCT,         // approx_inner_product
    FunctionSet.APPROX_L2_DISTANCE            // approx_l2_distance
);
// ...
if (APPROX_VECTOR_DISTANCE_FUNCTIONS.contains(call.getFnName())) {
    boolean isLhsColumn = call.getChild(0) instanceof ColumnRefOperator;
    ScalarOperator column = call.getChild(isLhsColumn ? 0 : 1);
    ScalarOperator array  = call.getChild(isLhsColumn ? 1 : 0);
    if (column instanceof ColumnRefOperator &&
            ScalarOperatorUtil.isFloatArray(array) &&
            ScalarOperatorUtil.isLiteral(array)) {
        // → 触发 lumina ANN 下推
    }
}
```

两个必要条件：
1. 函数名是 `approx_cosine_similarity` / `approx_inner_product` / `approx_l2_distance`
2. 向量参数是**字面量** float array（`isLiteral`）

我手写的 SQL 用 `cosine_similarity`（精确版） + `array_repeat(...)`（函数调用，非字面量）—— 两个条件都不满足 → 优化器 fallback 到全表扫描。

#### 验证：换成 `approx_cosine_similarity` + 真字面量 → ANN 下推生效

用 768-dim 真字面量 float array：

```sql
SELECT id FROM cohere_1m_cosine_seq6
ORDER BY approx_cosine_similarity([0.222, 0.267, 0.341, ..., 0.035], vector)
LIMIT 10
```

Profile（warm, c=1, query id `fce8f4b5-6407-11f1-8835-00163e496943`）：

| Metric | brute-force（错 SQL）| **ANN（对 SQL）** | Δ |
|---|---|---|---|
| Total wall | 2341 ms | **599 ms** | −74 % |
| ApplyTopNIndexRule::transform | 不运行 | **运行** ✅ | — |
| RawRowsRead | 1,000,000 | **60** | −99.994 % |
| QueryCumulativeScanTime | 1518 ms | **7.5 ms** | −99.5 % |
| FE Planner Total | 318 ms (14 %) | **557 ms (93 %)** | — |
| BE 占 wall | 65 % | **1 %** | — |

→ **ANN BE 工作仅 7.5 ms（图遍历命中 60 个候选），wall 几乎 100% 在 FE Planner**。

#### Bench 数据 vs ad-hoc profile —— 哪些有效哪些无效

| 数据来源 | SQL pattern | workload | 是否反映 ANN |
|---|---|---|---|
| `driver_seq6.py` / `driver.py` 内 `build_sql()` | `approx_cosine_similarity(vector, {literal_768_floats})` | **ANN** | ✅ 有效 |
| §5.9.10 全部 P0-B / P0-C bench（4 表 × 2 conc）| 用 driver | **ANN** | ✅ 有效 |
| §5.9.11 c=50 sustained BE perf record（驱动 driver_seq6.py）| 用 driver | **ANN** | ✅ 有效（lumina 32-37% 是真 ANN compute）|
| §5.9.11 30 个 ad-hoc query profile（我手写）| `cosine_similarity` + `array_repeat` | **brute-force** | ❌ 不反映 ANN |
| `docs/profiles_p0bc/profile_p0bc_*.txt` 5 个（我手写）| 同上 | **brute-force** | ❌ 不反映 ANN |

→ **journal § 5.9.9 / 5.9.10 / 5.9.12 的 QPS 提升数字全部有效**（6.71 → 26.97 / 28.80）；**§5.9.11 关于"FE Planner 5-7% / BE 94-96% wall"的归因仅适用于 brute-force**，不适用于 ANN。

#### 独立验证：ANN c=50 sustained ceiling

2026-06-09 21:42-21:45 跑独立 120s c=50 sustained bench（driver_seq6.py，确认 SQL 是 ANN）：

| Metric | 数值 |
|---|---|
| **QPS** | **29.06** |
| p50 / p95 / p99 | 1694 / 2201 / 2507 ms |
| count | 3,539 / 121.77 s |

跟 §5.9.10 的 26.97 QPS 一致（小幅波动 ±10 %）。**29 QPS 是当前 3-BE × P0-B+C 的 ANN ceiling**。

#### 修正后的真正剩余瓶颈

ANN warm 单 query c=1 = 600 ms，c=50 sustained p50 = 1694 ms（×2.8 串行化）。两个数字都说 **FE Planner 是真 dominator**，不是 BE。具体 FE 暗物质：

| 阶段 | 单 query warm | 占比 |
|---|---|---|
| ExecPlanBuild | 456 ms | 76 % |
| ↳ getPaimonRemoteFileInfos | 97 ms | 16 % |
| ↳ **未细分 ~330 ms** | — | **55 % ← 真正暗物质** |
| ApplyTopNIndexRule::check (1st cold) | 90 ms | 15 % |
| Deploy | 13 ms | 2 % |
| BE | 7.5 ms | 1 % |

#### 修正后的下一步（替换之前 §5.9.11 P1-A/B/C）

| 优先级 | 改动 | 预期 |
|---|---|---|
| **P1-NEW** | 给 `ExecPlanBuild` 加 trace timer 拆解 330 ms 暗物质（backend selector / scan range planning / paimon index metadata loading 等）| 信息收益 |
| **P1-NEW** | 缓存 `getPaimonRemoteFileInfos`（97 ms / query）跨 query 复用 paimon manifest | −10-15 % wall |
| **P1-NEW** | FE mysql-nio-pool / planner 调度调研：为什么 c=50 时 per-query 600 ms → 1700 ms（×3 串行化）| 待 trace |
| **P0-D 重新评估** | 跨 query snapshot LRU cache（TTL ~1 min）。在 ANN 路径下，能省 ScanIndexFileEntries 26 ms + 部分 ApplyTopNIndexRule::check cold start 开销 | 5-10 % wall（之前 v0.9 估只省 17-28 ms 是基于 brute-force）|
| ~~BE-侧调优~~ | num_nodes_to_cache / sector_aligned_read / lumina coroutine | **基本无 ROI**（ANN BE 已 7.5 ms）|

#### 完整证据档案（§5.9.13）

- 错误 SQL ad-hoc profile：`docs/profile_3rd_82815001-6406-11f1-8835-00163e496943.txt`（brute-force, total 2341 ms）
- 正确 ANN profile：`docs/profile_ann_fce8f4b5-6407-11f1-8835-00163e496943.txt`（ANN, total 599 ms）
- 独立 c=50 sustained 验证：master `/tmp/benchmark/results/ann_verify_214209/bench.json`（QPS 29.06）

### 5.9.14 FE timer trace 落地 + 真 dominator 拆解（**internalSqlExec + getPaimonRemoteFileInfos**）

> 在 §5.9.13 修正后补 FE Planner timer trace 把 ExecPlanBuild 内"暗物质"曝光。Commit `eb22c9f22c9`，Jenkins build #778 FE-only，已部署到 master `120.26.175.189`。

#### 改动

仅 trace，不改语义：

- `fe/fe-core/src/main/java/com/starrocks/connector/paimon/PaimonGlobalIndexService.java`：把 `evaluate()` 拆成 `evaluate.buildSql` + `evaluate.internalSqlExec` + `evaluate.emptyResult` 三段 `Tracers.watchScope`
- `fe/fe-core/src/main/java/com/starrocks/planner/PaimonScanNode.java`：在 `setupScanRangeLocations` 内调用 `evaluate()` 的位置包一层 `Tracers.watchScope("table.PaimonGlobalIndexService.evaluate")`

#### 单 query warm c=1 拆解（commit eb22c9f22c9 部署后）

```
Total: 1204ms (warm c=1, FE just restarted so still slightly cold)
└─ Planner Total: 1149ms (95% wall)
   ├─ ApplyTopNIndexRule::check (1st cold): 340ms
   │  └─ ScanIndexFileEntries: 29ms
   ├─ Optimizer: 9ms (rule cache 命中)
   └─ ExecPlanBuild: 778ms
      ├─ PaimonGlobalIndexService.evaluate: 466ms ← exposed dark matter
      │  ├─ evaluate.buildSql: 3ms
      │  └─ evaluate.internalSqlExec: 461ms
      │     ├─ inner Analyzer: 1ms
      │     ├─ inner Optimizer: 2ms
      │     ├─ inner ExecPlanBuild: 221ms ← 嵌套 planner (`$global_index` 表 scan range)
      │     ├─ inner Deploy: 10ms
      │     └─ 未细分 ~225ms ← inner BE wait + collect = 真 ANN 工作
      └─ getPaimonRemoteFileInfos: 307ms ← cold (warm 通常 97ms)
└─ BE QueryCumulativeScanTime: 7.5ms (lumina 几乎 instant)
```

#### c=20 sustained 期间 in-flight sample profile（contention 状态）

c=20 bench QPS = **13.91**（trace overhead ≈ 0，无 regression），p50=1401ms，p99=2014ms。从独立 connection 抓 5 个 sample profile（2 个成功，3 个 404 因 `IsProfileAsync=true` 异步写入有 race）。其中一个 wall=1515ms 拆解：

```
Total: 1515ms
└─ Planner Total: 1472ms (97% wall！)
   ├─ ApplyTopNIndexRule::check (1st): 451ms      ← contention inflate ×5 (warm 90ms)
   │  └─ ScanIndexFileEntries: 147ms
   ├─ ExecPlanBuild: 1011ms
   │  ├─ PaimonGlobalIndexService.evaluate: 635ms
   │  │  ├─ evaluate.buildSql: 2ms
   │  │  └─ evaluate.internalSqlExec: 632ms
   │  │     ├─ inner ExecPlanBuild: 375ms
   │  │     ├─ inner Deploy: 5ms
   │  │     └─ 未细分 ~250ms
   │  └─ getPaimonRemoteFileInfos: 375ms       ← contention inflate ×4 (warm 97ms)
   └─ Deploy: 10ms
└─ BE QueryCumulativeScanTime: 8.848ms        ← BE 完全 idle
```

#### 关键观察

1. **c=20 高并发下所有 FE Planner 项都 ×4-5 inflate**：单 query 1.5s, 中 1.47s 在 FE Planner，BE 仅 9ms
2. **BE 完全空闲**（8.8ms × 20 conc / 3 BE = 0.06 core）—— 进一步证实 FE 是 throughput ceiling
3. **`evaluate.internalSqlExec` 461-632ms** 是个嵌套 SQL execution，其中 inner ExecPlanBuild 221-375ms 是 FE 自己的工作，剩余 225-252ms 是真 BE ANN dispatch 等待
4. **`getPaimonRemoteFileInfos` 307-375ms** 是另一大块 —— 内部走 paimon `scan.plan()` → `snapshotManager.latestSnapshot()` → 新 RESTCatalog → REST 拉 snapshot pointer

#### Paimon manifest cache 现状（用户 follow-up 问题）

| 缓存层 | 状态 | 证据 |
|---|---|---|
| paimon-java 内部 **manifest cache**（`catalogCache`）| ✅ 已生效 | profile 显示 `cachedManifestNumInCatalog: 6`，`objectsCache numReadFromCache: 3 / numReadFromRemote: 0`，paimon `planTime: 2ms`（manifest 文件本身 100% 命中内部 LRU）|
| paimon-java 内部 **snapshot cache**（`SnapshotLoaderImpl.load()`）| ❌ 每次 fresh RESTCatalog | §5.9.8 已验证。`AbstractDataTableScan.java:456 snapshotManager.latestSnapshot()` 触发同一冷启动路径 |
| stella `PaimonMetadata.getRemoteFiles` 内部 `paimonSplits` map | ⚠️ query-scope，但 key 包含 predicate（每 query vector 不同 → cache miss） | `PaimonMetadata.java:226` `paimonSplits.containsKey(filter)` 检查；filter 是 `PredicateSearchKey(db, table, snapshotId, predicate)` |
| stella **跨 query** RemoteFiles 缓存 | ❌ 不存在 | grep 确认无相关代码 |

→ **manifest 本身已 cache，但 snapshot pointer 没**。每个 query 走 `scan.plan()` 时调用 `latestSnapshot()` 是 cold start 触发新的 RESTCatalog + REST roundtrip。300+ ms 大部分是这条网络往返 + 处理（paimon `planTime` 仅 2ms，所以其余 300ms 几乎完全在 REST 链）。

#### 修正后的优化候选（按 ROI）

| 优先级 | 改动 | 节省（c=20 sustained）| 节省（c=1 warm）| 改动复杂度 |
|---|---|---|---|---|
| **P0** | **跨 query snapshot 缓存**（or 跨 query `getPaimonRemoteFileInfos` 整体缓存，TTL ~30s-1min，opt-in）| **280ms / query**（375→97）| 200ms（307→97）| 小，stella FE 内 |
| **P1** | 并行 `evaluate()` + `getPaimonRemoteFileInfos` 用 `CompletableFuture` | **375ms**（取 max 而非 sum）| 同 | 中，PaimonScanNode 改异步 |
| **P1** | `evaluate.internalSqlExec` 直接 BE RPC 绕过 inner SQL planner | inner ExecPlanBuild **375ms** | 221ms | 大（需新 BE RPC primitive，跨团队）|
| **P2** | `ApplyTopNIndexRule::check` 跨 query 缓存 cold start 部分（snapshot 缓存生效后自然缩） | tail effect | 250-300ms 一次性 | 小（依赖 P0）|

**理论 c=20 wall** 上限：1515 - 280 - 375 = ~860ms → QPS 23-25（vs 现 13.91）。
**c=50 sustained 理论 QPS**：50 / 0.9 = ~55 QPS。

#### 完整证据档案（§5.9.14）

- 单 query trace profile：`docs/profile_trace_06a9f40d-6418-11f1-80a1-00163e496943.txt`
- c=20 sustained profile：`/tmp/benchmark/results/c20_trace_233526/profile_172581a6-6419-11f1-80a1-00163e496943.txt`（master）
- Trace commit：`eb22c9f22c9` on branch `xiaolong/bugfix-paimon-global-index-topn-null-deref`，Jenkins build #778

### 5.9.15 R1: 自主优化 5-6h 推 QPS ≥80（autonomous，2026-06-10 凌晨）

> 用户授权 5-6h 自主优化。目标 c=50 sustained QPS 从 ~29 推到 ≥80（×2.8）。每步操作 / 决策 / 验证结果实时记录。

#### R1.P0 准备：新分支 + 默认值确认

- 分支：`xiaolong/perf-r1-fe-planner-opt` from `xiaolong/bugfix-paimon-global-index-topn-null-deref` (commit `eb22c9f22c9`，含 P0-B+P0-C+timer trace)
- Cache staleness 决策：cross-query metadata cache **默认 OFF**，session var `enable_paimon_metadata_cross_query_cache` 显式打开；TTL 5-10s 可配
- Push 策略：每个改动 commit + push 到 ali-origin → Jenkins FE-only build → 部署
- Regression handling：QPS 倒退立即 revert commit，记 journal，继续下一个
- 每轮验证：profile 重定位 bottleneck（避免上轮一样的盲推），同时关注 BE CPU

#### R1 优化候选清单（按落地顺序）

| 阶段 | 改动 | 预期 |
|---|---|---|
| R1.P1 | 加细 timer 在 `PaimonMetadata.getRemoteFiles` 内（convertPredicates / newScan / `paimonCatalog.getSplits` 等），定位 305ms 暗物质实际位置 | 信息 |
| R1.P2 | 跨 query snapshot/RemoteFiles cache（session var opt-in，5-10s TTL）| −280ms / query |
| R1.P3 | `ApplyTopNIndexRule::check` 跨 query cold-start cache | −90-400ms |
| R1.P4 | 并行 `evaluate()` + `getPaimonRemoteFileInfos`（CompletableFuture）| −200-300ms |
| R1.P5 | 全量 c=50 bench + 收尾 journal | 最终 QPS 数字 |

#### 实时记录

##### R1.P1 — getRemoteFiles 内 4 个 timer（baseline 信息收集）

| 项 | 状态 |
|---|---|
| 分支 | `xiaolong/perf-r1-fe-planner-opt` |
| Commit | `806ec7add2c` "deep FE timers in PaimonMetadata.getRemoteFiles" |
| Jenkins | build #780 SUCCESS |

**部署后惊人发现 1**：当前 fe/lib 里的 `paimon-bundle` 已经是**原版**（无 P0-C SHARED_TOKEN_CACHE 符号），md5 5d1a5ff084... — 上一次 trace deploy (#778) 时被 fe/lib swap **整体替换掉了我们手动 overlay 的 P0-C 版**！这意味着过去几小时所有 c=50 = 29 QPS 都是**没有 P0-C** 的状态。

**Immediate fix**：从 local jar 重 overlay P0-C 版本的 paimon-bundle 到 fe/lib，再 deploy R1.P1 build #780（含 trace），并在最后 overlay P0-C jar。

**部署后 baseline 测试（c=20 + c=50 with P0-C restored + R1.P1 trace）**：

| conc | QPS | p50 | p95 | p99 | Δ vs P0-C undone state |
|---|---|---|---|---|---|
| 20 | **25.44** | 775 ms | 1006 ms | 1208 ms | +83 % (×1.83) |
| 50 | **29.92** | 1618 ms | 2101 ms | 2409 ms | +0 % (×1.0) |

奇怪的现象：**c=20 大涨 +83 %，c=50 几乎不变**。说明 c=50 contention 下另一个 bottleneck 主导。

**c=50 jstack（6 次 × 50 conc = 300 mysql-nio-pool 线程）：**

| Top frame | 出现 |
|---|---|
| `sun.nio.ch.Net.poll` | 245 / 300 |
| Stack pattern: `evaluate→internalSql` | 103 |
| Stack pattern: `PaimonMetadata.indexShardList` | 97 |
| Stack pattern: `scan.setup→getRemoteFiles` | 88 |

**Net.poll 245 中 221 (90 %) 仍在 `DLFECSTokenLoader`**！但其中 stack frame 是 `DLFECSTokenLoader.getRole(line 110)` **不是** `getToken`。

##### R1.P2 ⚡ — 真正的 bug：`getRole()` 不在 P0-C cache 里

之前 P0-C 只 cached **token**，但**没 cache role name**：

```java
@Override
public DLFToken loadToken() {
    if (roleName == null) {
        roleName = getRole(ecsMetadataURL);   // ← HTTP call! Per-instance!
    }
    String cacheKey = ecsMetadataURL + "::" + roleName;
    // ... then SHARED_TOKEN_CACHE lookup hit
}
```

每个 query 创建的新 DLFECSTokenLoader instance 都 `roleName = null` → 调 `getRole(url)` → HTTP fetch ECS metadata 的 role 字符串 → 再去查 token cache（命中）。**整个 P0-C 的 token cache hit 路径都被这条前置 getRole call 卡住。**

**Fix**：加 `SHARED_ROLE_NAME_CACHE: ConcurrentMap<String, String>` 按 `ecsMetadataURL` 缓存 role-name 全局（role 是 ECS 实例级别属性，几乎不变）。

```java
private static final ConcurrentMap<String, String> SHARED_ROLE_NAME_CACHE =
        new ConcurrentHashMap<>();

@Override
public DLFToken loadToken() {
    if (roleName == null) {
        roleName =
                SHARED_ROLE_NAME_CACHE.computeIfAbsent(
                        ecsMetadataURL, DLFECSTokenLoader::getRole);
    }
    // ... rest unchanged
}
```

文件：`paimon-api/src/main/java/org/apache/paimon/rest/auth/DLFECSTokenLoader.java`（paimon 工作区 local edits，未 commit）。

**Build**：`mvn clean package -DskipTests -pl paimon-bundle -am` (本地 ~56s)。
**Deploy**：scp paimon-bundle jar 到 master，停 FE → 替换 fe/lib/paimon-bundle-*.jar → 启 FE。

**🎯 R1.P2 c=50 sustained bench 结果（120s, 2026-06-10 02:55-02:58）**：

| Metric | R1.P1 (no role cache) | **R1.P2 (with role cache)** | Δ |
|---|---|---|---|
| **QPS** | 29.92 | **87.28** | **×2.92** |
| p50 | 1618 ms | **554 ms** | −66 % |
| p95 | 2101 ms | **719 ms** | −66 % |
| p99 | 2409 ms | **872 ms** | −64 % |
| count / 120s | 3623 | **10,519** | +190 % |

**✅ QPS 87.28 ≥ 80 目标达成！** 单 role-cache fix 就直接跨过目标线。下一步：c=75/c=100 看 scaling + 找剩余空间。

##### R1.P2 — 后续 scaling + 稳定性验证

**c=50/75/100/150 sweep（60-180s sustained）**：

| conc | QPS | wall ≈ conc/QPS | 备注 |
|---|---|---|---|
| 50 | **87-92**（3 run 均值 91.06）| 0.55 s | 稳定 |
| 75 | 90.4 | 0.83 s | |
| **100** | **99-100**（3 run 均值 99.69，single max 100.02）| 1.01 s | **peak** |
| 150 | 95.8 | 1.57 s | 队列堆积，p50 涨到 1.5 s |

c=100 三次独立运行均 ≥ 99 QPS。c=150 QPS 反而略降，FE 100 conc 时已经把 BE inner SQL 通道打满。

**P0-B 仍有效（toggle 测试）**：

| State | QPS @ c=50 | p99 | Δ |
|---|---|---|---|
| P0-B OFF (R1.P2 only) | 72.46 | 908 ms | baseline |
| **P0-B ON + R1.P2** | **90.81** | 819 ms | **+25 %** |

→ P0-B query-scope metadata cache 和 R1.P2 role-name cache **正交独立可叠加**。

##### R1.P2 — c=150 新瓶颈定位（jstack + BE perf + BE CPU）

c=150 FE jstack 6 次 × 150 conc = 900 个 mysql-nio-pool 线程 slot：

| Top frame | 数量 | 占比 |
|---|---|---|
| `java.lang.Object.wait` | 752 | **83.6 %** |
| `sun.nio.ch.Net.poll` | 68 | 7.6 % |
| 其他 | <2 % each | 噪声 |

Stack pattern：
- **727 / 900 = 80.8 %** 线程在 `evaluate→InternalSqlExecutor.execute→DefaultCoordinator.getNext→ResultReceiver.getNext→ProtobufRpcProxy.doWaitCallback→BlockingRpcCallback.wait`
- 即等 inner SQL 的 BE RPC 响应

剩余 Net.poll 仅 70 处全是 `DLF loadSnapshot`，**ECS metadata token 痕迹完全消失** ✅（R1.P2 role+token cache 都生效）。

**BE 侧 CPU & perf** during c=100 sustained：

| BE | %CPU | %CPU / 96 cores | 估计 cores busy |
|---|---|---|---|
| 10.105.221.220 | 2220 | 23 % | 22.2 |
| 10.105.221.221 | 2360 | 24 % | 23.6 |
| 10.105.221.222 | 2100 | 22 % | 21.0 |

→ **BE 不是 CPU saturated**（96 cores 仅用 22-24%）。

BE perf record 60s top symbols (avg across 3 BE)：
- `lumina::EvalIPAvx512` **13-15 %**（SIMD 向量距离计算）
- `lumina::DiskANNSearcherBackend::ExpandNodes` 4.5-6 %
- `syscall_enter_from_user_mode` 3 %
- `__lock_text_start` (kernel spin) 2 %
- `my_malloc` / `my_free` 3.5-4 %
- `ZSTD_decompress` 2.3 % (one BE only)
- `__pthread_mutex_unlock_usercnt` 2.4 %

→ BE 端 lumina 实际 compute 占 BE CPU 的 ~20 %（即 BE 总 CPU 的 ~5 %）。其余 BE 时间在 fragment 内 IO/sched/locks。

**c=100 BE 理论上限分析**：
- BE 23 cores × 3 BE = 69 cores busy total
- 99 QPS 100 conc → 99 inner SQL fragments/s 触发 ~297 BE fragments/s（3-way dispatch）
- 每 fragment ≈ 0.23 s BE wall（含 ANN compute + 内部 IO + RPC return）
- 这是 paimon 内 lumina dispatch 的物理上限

#### R1 收尾结论

**目标达成**：QPS 87.3 ≥ 80 在 c=50；c=100 peak 99 QPS（×33 vs 原始 baseline 3 QPS, ×3.3 vs v1.1 的 28.8 QPS）。

| 优化 | 文件 | 部署形式 | 增量 |
|---|---|---|---|
| **P0-B** （v1.1 保留）| stella FE `PaimonMetadata.java` | FE config 默认 ON | +25 % @ c=50（toggle 验证）|
| **R1.P2 (P0-C-bis)** （本轮关键）| paimon-java `DLFECSTokenLoader.java` | paimon-bundle jar overlay | ×2.92 QPS @ c=50（vs 仅 token cache 状态）|
| **R1.P1**（trace）| stella FE `PaimonGlobalIndexService.java` + `PaimonScanNode.java` + `PaimonMetadata.java` | FE 内 timer trace | 0（观测改动）|

**下一步剩余瓶颈**：
- **80 % FE 线程时间等 inner SQL BE RPC**，BE 处理 fragment 是物理上限
- Inner SQL pipeline 对 ANN query 是高 overhead：每 query 走完整 Parser/Analyzer/Optimizer/ExecPlanBuild → 3-way BE dispatch
- 真正突破 100 QPS 需要：
  - **(高 ROI)** 在 PaimonGlobalIndexService.evaluate 处绕过 inner SQL，用直接 BE Thrift/Brpc primitive（设计 1 个 `paimon_global_index_evaluate` BE RPC，FE 直接调）—— 估计 +50-100 % QPS
  - **(中 ROI)** Inner SQL plan cache（同 shape 缓存 plan，只换 args）—— 估计 +20-30 % QPS
  - **(infra)** 横向扩容 BE 数量 → linear scale

#### R1 关键 commit & deploy 状态

| Item | 状态 |
|---|---|
| Stella FE branch | `xiaolong/perf-r1-fe-planner-opt`，commit `806ec7add2c`（trace）pushed |
| Stella FE Jenkins | build #780 SUCCESS（含 P0-B + trace） |
| Stella FE deployed jar md5 | `0310517c1f...` |
| Paimon-java branch（**本地仅，未 push**）| `xiaolong/perf-r1-token-role-cache`，commit `5d73e164b` |
| Paimon-java jar 本地路径 | `/Users/drake_wang/workspace/alibaba/paimon/paimon-bundle/target/paimon-bundle-1-ali-26.1-lake-optimizer.jar` |
| Paimon-java deployed jar md5 | `400c2c6e14...`（master `/opt/apps/STARROCKS3/.../fe/lib/paimon-bundle-*.jar`） |
| 回滚 | master 上 `paimon-bundle-*.jar.bak.r1p1_20260610023931`（含旧 P0-C-v1 + 备份的 fe/lib swap）|

#### 完整证据档案（R1）

- bench json：`docs/r1p2_evidence/c50_run{1,2,3}.json` + `c100_run{1,2,3}.json`
- BE perf：`docs/r1p2_evidence/perf_10.105.221.{220,221,222}.txt`
- FE jstack 后期：`docs/r1p2_evidence/fe_jstack_c150_post_role_cache_jstack_1.txt`
- bench scripts on master：`/tmp/bench_and_profile.sh`, `/tmp/r1p2_scale.sh`, `/tmp/r1p2_verify.sh`
- 全部 results dir：master `/tmp/benchmark/results/r1p2_*`

#### R1 final 收尾运行（end-of-autonomous, 2026-06-10 03:47-03:49）

| conc | wall (s) | count | **QPS** | p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| 50 | 60.46 | 5524 | **91.37** | 534 | 633 | 877 |
| **100** | 60.77 | 6081 | **100.06** ⭐ | 987 | 1135 | 1319 |

**R1 autonomous run 收官状态**：QPS 100 在 c=100 sustained 整数突破。






| 版本       | 日期                   | 变更摘要                                                                                                                                                                                             |
| -------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **v1.3** | **2026-06-10 03:45** | **§5.9.15 R1 autonomous 5h run，QPS 87→99 突破**。新分支 `xiaolong/perf-r1-fe-planner-opt`（commit `806ec7add2c` trace + Jenkins #780），paimon-java 本地分支 `xiaolong/perf-r1-token-role-cache`（commit `5d73e164b` 未 push）。**关键发现**：v1.2 后 fe/lib swap 把 P0-C jar 覆盖回原版 → c=50 = 29 QPS 状态一直没意识到。手动恢复 P0-C 后 c=20 +83% 但 c=50 仍 30 QPS。jstack 揭示真 bug：原 P0-C 只缓存 token，**没缓存 role-name**（每个新 DLFECSTokenLoader 都先 HTTP fetch role，221/245 Net.poll 仍在 `getRole`）。**Fix R1.P2**：加 `SHARED_ROLE_NAME_CACHE`。**结果**：c=50 = **87.3 QPS**（×2.92 vs 29.92），c=100 peak **99-100 QPS**，p99 2409→872ms (−64%)。三次独立运行稳定。P0-B toggle: OFF=72.5 / ON=90.8 QPS（P0-B 仍 +25%）。c=150 saturate at 95.8，80% mysql-nio-pool 等 BE inner SQL RPC（BE CPU 22-24% 未饱和）。剩余空间需绕开 inner SQL pipeline（直接 BE RPC primitive）或横向扩容。 |
| **v1.2** | **2026-06-09 23:50** | **§5.9.14：FE timer trace 落地 + 暗物质曝光**。Commit `eb22c9f22c9`，Jenkins #778 FE-only build + 部署。trace 把 `ExecPlanBuild` 内"330ms 暗物质"拆开：`PaimonGlobalIndexService.evaluate.internalSqlExec = 461-632ms`（嵌套 SQL，inner ExecPlanBuild 221-375ms + inner BE wait 225-252ms）+ `getPaimonRemoteFileInfos = 307-375ms`。c=20 sustained QPS 13.91（trace overhead ≈ 0），sample profile 显示 FE Planner = 1472ms / 1515ms wall = **97 %**，BE 8.8ms = 0.6 %。paimon-java 内部 manifest cache 已 100% 命中（`objectsCache numReadFromCache: 3 / numReadFromRemote: 0`，`planTime: 2ms`），但 snapshot pointer 每次 fresh REST（`AbstractDataTableScan.java:456 snapshotManager.latestSnapshot()` 触发 §5.9.8 同条 cold-start 路径）—— 这是 `getPaimonRemoteFileInfos` 300+ms 的根因。下一步优化方向：跨 query snapshot cache（−280ms / query）+ 并行 evaluate+getRemoteFiles（−375ms）+ 直接 BE RPC 绕过 inner SQL planner（−375ms）；c=20 wall 上限 ~860ms → QPS 23-25。 |
| **v1.1** | **2026-06-09 21:50** | **§5.9.13：方法学修正**。验证发现我手写的 30 + 5 个 ad-hoc query profile 用了错的 SQL pattern（`cosine_similarity` 而非 `approx_*`，`array_repeat(...)` 而非字面量），lumina ANN 下推未触发，**实际是 brute-force 全表扫描**。读 `IndexAnalyzer.java:56-60 + :141-169` 确认两个必要条件：`approx_*` 函数 + 字面量 float array。换 SQL 后 ANN 生效：warm 单 query 2341ms → **599ms（−74%）**，`RawRowsRead 1M → 60（−99.994%）`，`QueryCumulativeScanTime 1518ms → 7.5ms（−99.5%）`。**bench 数据全部有效**（driver_*.py 一直用正确 SQL）；§5.9.11 关于"BE 94-96% wall"仅适用 brute-force，不适用 ANN。独立验证：c=50 sustained = **29.06 QPS**（匹配 §5.9.10 的 26.97）。**修正 dominator 归因：ANN 工作流下 FE Planner 是真 dominator**，BE 仅 7.5ms / query。剩余优化空间从 BE-侧 转移到 FE Planner（ExecPlanBuild 内 330ms 暗物质 + 跨 query metadata cache）。BE-侧 P2-A/P2-B 收益归零。 |
| **v1.0** | **2026-06-09 19:57** | **§5.9.11 + §5.9.12：三层 P0 优化全部落地 + BE perf 首次看到真正 ANN hot path**。Bench 实测三段累积：baseline (6.71 QPS) → P0-B (14.36) → P0-B+C (**26.97 QPS @ c=50 seq6, ×4.02**)；single-shard c=50 6.65 → **28.80 QPS（×4.33）**；p99 全部压缩 41-77 %。post-P0-B+C BE perf record on 3 worker × 60s：lumina ANN compute 32-37 %（ExpandNodes 14-19 % + EvalIPAvx512 10-12 % + SelectExpandNodes 6-8 %），kernel scheduling/lock 21-23 %（sched_yield 8.4-8.8 % — 比 Round 8 c=20 时的 24 % 下降 ~65 %，证伪"sched_yield 是 binding constraint"），malloc/lock 3-7 %，ZSTD 1.3-1.6 %（彻底不显著）。下一步可优化项：P1-A lumina 调度（5-10 %）、P1-B DiskANN beam_width/list_size sweep（5-15 %，task #93）、P1-C BE scan path 复用（2-5 %）。FE / auth chain 已无 actionable 空间。 |
| **v0.9** | **2026-06-09 18:20** | **§5.9.10：P0-C 落地 + A/B 验证 — paimon-java fork `1-ali-26.1-lake-optimizer` 上给 `DLFECSTokenLoader` 加进程级静态 token cache（key by `ecsMetadataURL::roleName`，single-flight 刷新，1h 过期阈值），本地 build paimon-bundle 替换 master `/opt/apps/STARROCKS3/.../fe/lib/paimon-bundle-*.jar` 并重启 FE。**P0-B 与 P0-C 几乎独立可乘**（单独 ×1.94 / ×2.14，组合 ×4.02-4.33）。`cohere_1m_cosine_seq6` c=50：6.71 → **26.97 QPS**，p99 8812ms → 3262ms。`cohere_1m_cosine` (single-shard) c=50：6.65 → **28.80 QPS**，p99 8442ms → 2441ms。Trade-off：token revoke 感知滞后 ~1h（生产 IAM 紧急场景需重启 FE 清 cache）；运维选 A 方案（保留设计 + 文档化）。后续 P0-D（跨 query snapshot cache）收益缩水到 ×1.2-1.5，需先 profile 验证下一个 dominating 项。 |
| **v0.8** | **2026-06-09 17:35** | **§5.9.9：P0-2 落地 + A/B 验证 — PaimonMetadata query-scope cache 实施完成（commit `4af108e10b5`，Jenkins build #765 FE-only，部署 120.26.175.189）。§5.9.8 关于 DLFAuthProvider lock contention 的推断被 c=20 稳态 jstack 修正（0 BLOCKED；19 个 distinct DLFAuthProvider instances；真正根因是 `SnapshotLoaderImpl.load()` 用 try-with-resources 每次 `new RESTCatalog`，触发 cold-start `DLFECSTokenLoader` HTTP fetch）。修复方向：把 4 次 / query 的 `latestSnapshot()` REST roundtrip 压缩成 1 次。**A/B 实测**（`cohere_1m_cosine_seq6` c=20，60s）：cache_off=6.54 QPS / p99=3883ms，cache_on=12.80 QPS / p99=2202ms → **+96% QPS，p99 −43%**。c=1 单 query 节省 28%。Gated by mutable FE config `enable_paimon_global_index_metadata_query_cache`。c=50 + single-shard 对照结果待补。 |
| **v0.7** | **2026-06-09 15:05** | **§5.9.8：颠覆性发现 — 真正瓶颈在 FE Planner DLF REST + DLFAuthProvider lock，不在 BE/lumina**。单 query profile 实测：Planner 3.165s = 98.8%，BE Execution 22.7ms = 0.7%。FE jstack 命中 ApplyTopNIndexRule → IndexFileHandler.scanEntries → SnapshotManager.latestSnapshot → RESTCatalog.loadSnapshot → DLFAuthProvider.refreshToken (synchronized) → CloseableHttpClient.execute。"ScanIndexFileEntries" 是 DLF REST HTTP 调用 + 同步 token refresh，不是 OSS。c=1 latency 0.94s，c=20 3.2s — 完全是 DLF 调用串行 + 锁争用造成。之前所有 BE-side 瓶颈结论全部推翻：thread 99.9% S 是 BE idle 等 FE 派发，不是 lumina spinning。移交对象改为 StarRocks FE 主线 + paimon-java，不是 paimon-cpp/lumina。 |
| **v0.6** | **2026-06-09 14:50** | **Round 9 (§5.9)：SearcherPool 实验负向（POOL=4 QPS 5.35 < POOL=1 5.97 < baseline 6.50，已回退）。Reader cache cap=0 vs cap=16 在 DataCache 已热状态下仅 +2.7%。Final 252 profile 聚合直接证据：DataCacheReadBytes sum 789.52MB，readBytes(storage)=0，网络 RX 1MB/s — 100% DataCache disk tier 命中，0 字节 OSS 直读。IO 完全 ruled out。同时记录关键工程坑：FE HttpPort=18030 非 8030、SSH "Bad fd" 真根因是 Claude shell zombie until-ssh、OSS 跨 region 必须 public endpoint。** |
| **v0.5** | **2026-06-07 17:25** | **Round 7 config sweep + Round 8 perf record on O8c：c=20 平台期 ~6.4 QPS 验证为结构性（§5.8）。ANN BE 是 binding，单 BE 单分片下 lumina coroutine sched_yield 24% 是最大可争空间（ZSTD page cache 在非 binding BE 上 ROI 低）**。 |
| v0.4     | 2026-06-07 16:30     | 收尾汇报。Fix A3 (`76aadcbf051`) + O8c (`4baa3ae690d`) 完整验证，累计 4-5x QPS + p99 -60-78%。§5 新增完整 final report：累计结果、改动 scope、调研路径回顾、没做的、生产推荐配置、移交事项。                                                      |
| v0.3     | 2026-06-07 14:42     | Fix A3 commits register 加 O8c 行 + 实测效果回填（§4.12.5）。Round 6 perf 验证 thread pool 路径完全消失（§4.13）。                                                                                                     |
| v0.2     | 2026-06-07 13:00     | Round 4 真实 perf 数据：`my_free` 27.42% 真凶。Code audit：ReadAsync 没有 cached path 分支。**Fix A3** commit `76aadcbf051` 落地。§4.6 ~ §4.9 新增。                                                                 |
| v0.1     | 2026-06-07 11:00     | 初版 skeleton。Phase 1 启动等 build #717。Audit prior doc CRC32 假设，发现源码已有 gate，与 prior 主张矛盾；待 release build flamegraph 实测验证。                                                                            |
|          |                      |                                                                                                                                                                                                  |
|          |                      |                                                                                                                                                                                                  |



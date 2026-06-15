# Paimon ANN QPS 调优完整记录 (cohere_1m_cosine, c=100)

最后更新：2026-06-15

> 本文档是 Paimon 全局索引 (DiskANN/lumina) ANN 查询在 POC 集群 (120.26.175.189, 3FE+3BE 共享存储) 上
> c=100 并发 QPS 调优的**完整沉淀**：每一项优化、对应的 QPS 增幅、生效机制、FE/BE 参数配方、代码变更清单
> (仓库/分支/commit + 哪些有效哪些可 drop)、持久化状态、以及下一个瓶颈的 roadmap。
> 读者：接手本 perf 工作流的工程师，以及需要复现/上线该优化的同事。
>
> 基准数据集：`cohere_1m_cosine`（100 万行，768 维，cosine，**1 shard**），查询 `BYPASS=true`（FE→BE thrift 旁路）。
> 硬约束：并发 c ≤ 100。目标：QPS ≥ 200。**已达成并锁定：稳定 ~213 QPS。**

---

## 1. 结论：QPS 16 → 213 的完整路径

| 阶段 | 优化项 | QPS | 增量 | 类型 |
|---|---|---|---|---|
| 起点（FE 重启后未配置） | — | **16** | — | 退化态 |
| A | FE gimetacache 两开关 (`enable_paimon_snapshot_id_cache` + `enable_paimon_global_index_shard_cache`) | **108** | +575% | FE session/config |
| B | BE paimon 元数据缓存 TTL (`paimon_manifest_scan_cache_ttl_ms` + `paimon_scan_metadata_cache_ttl_ms` = 600000) | **165** | +53% | BE config (runtime) |
| C | FE evaluate RPC 三路轮询 (`BYPASS_BE_ROTATION`) | **183** | +11% | FE 代码 |
| D | `pipeline_dop = 1` | **195** | +7% | FE session (SET GLOBAL) |
| E | `enable_profile = false` | **213** | +9% | FE session (SET GLOBAL) |

**最终稳定性验证**（阶段 E 全配置，6×60s c=100 连续）：
`214.4 / 210.6 / 211.6 / 214.4 / 215.3 / 215.4 QPS`，均值 ~213.6，σ<2，p50 ~510ms，p99 721–757ms，
每轮 n≈12.8–13.0k；3 BE 全程 `http=200`，**零崩溃零重启**。

> 注：曾验证过一项 **FE paimon token cache**（paimon-java `SHARED_TOKEN_CACHE`，108→139），但它**未进入当前部署的
> bundle**（部署的是 stock paimon-bundle）。阶段 A 的 gimetacache 已把 per-query `loadSnapshot` HTTP 去掉，
> 把 ceiling 直接抬到 108→165，token cache 的边际收益被后续优化覆盖，故当前线上路径**不依赖** token cache。
> 见 §5 "可 drop / 待定" 与 §6。

---

## 2. 每个优化项详解

### A. FE gimetacache 两开关 — 16 → 108 (+575%)

**机制**：FE 规划阶段每个查询都会 `scan.plan()` 重新解析 paimon snapshot id 与 global-index shard 布局，
触发 per-query DLF REST HTTP。两个开关让 FE 进程级缓存 snapshot-id 和 GI shard 信息，消除每查询的规划期 HTTP。

**⚠️ 关键坑**：这两个是 `ADMIN SET FRONTEND CONFIG`（运行时 config，非 session var），**FE 重启后重置为默认值**。
不重新 apply → QPS 跌回 ~16。每次 FE 重启后必须立即重设（见 §4 复现配方）。

```sql
ADMIN SET FRONTEND CONFIG ('enable_paimon_snapshot_id_cache' = 'true');
ADMIN SET FRONTEND CONFIG ('enable_paimon_global_index_shard_cache' = 'true');
```

### B. BE paimon 元数据缓存 TTL — 108 → 165 (+53%)

**机制**：`BYPASS=true` 时 BE 自己解析 paimon snapshot/manifest。两个 TTL 默认 `0`（禁用）→ 每查询都重新
从 OSS/DLF 扫描元数据（约占每查询 ~200ms）。设为 600000ms (10min) 后：c=1 单查询 floor **278ms → 75ms**。

```bash
# 对 3 个 BE 全部执行（:18040 是 BE 的 http/brpc 管理端口）
for be in 10.105.221.220 10.105.221.221 10.105.221.222; do
  curl -s -X POST "http://$be:18040/api/update_config?paimon_manifest_scan_cache_ttl_ms=600000"
  curl -s -X POST "http://$be:18040/api/update_config?paimon_scan_metadata_cache_ttl_ms=600000"
done
```

**⚠️ TTL 必须够长**：设 60000ms 时一次跑崩到 27 QPS — 缓存刷新**没有 single-flight**，TTL 边界 ~100 并发查询
同时穿透重新扫描（比无缓存更糟）。cohere_1m 是**静态表**（无写入），600000ms 安全。
**写多的表**需权衡新鲜度 vs 穿透，或在 paimon-cpp 里给元数据缓存刷新加 single-flight（正解，之后短 TTL 才安全）。

**⚠️ 持久化**：`update_config` 是运行时设置，**BE 重启丢失**。要持久化需写入 `be.conf`。

### C. FE evaluate RPC 三路轮询 — 165 → 183 (+11%)

**机制**：旧代码 `PaimonGlobalIndexService.evaluateViaBypass()` 把每个 evaluate RPC 都打到 `beIds.get(0)`
（钉死第一个 BE）。cohere_1m 是 1-shard → 每查询 1 个 evaluate RPC → 全部落到 BE 221。
metric `paimon_global_index_evaluate_count`：BE220=0 / **BE221=306125** / BE222=0。
轮询后完全均衡：Δ220=11068 / Δ221=11067 / Δ222=11068。

**⚠️ 关键认知**：+11% 远小于 "三路分摊 CPU" 理论预测的 ~3×。说明**单 BE 的 evaluate CPU 不是硬 ceiling**，
真正的瓶颈在下游（Phase-2 主查询 brpc）。但此项仍是上 200 的必要条件（不做则钉死 ~165）。

**代码**（已实现，本次提交持久化）：`PaimonGlobalIndexService.java` 加 `static AtomicInteger BYPASS_BE_ROTATION`，
`int base = floorMod(getAndIncrement(), beIds.size())`，`getBackend(beIds.get((base+i)%beIds.size()))`。

> 历史 blocker 已解除：早期分发会崩冷 BE（paimon-cpp `GlobalIndexScanImpl::Create` 里 `paimon::Bytes` double-free）。
> paimon-cpp 的 `DeepCopyEntries` + `Re-home cached manifest Bytes` 两个 BugFix commit（见 §5）已修复，
> 本次轮询全程零崩溃验证通过。

### D. pipeline_dop = 1 — 183 → 195 (+7%)

**机制**：c=100 时默认 auto-DOP (`dop=0`) **过度并行**：100 个并发查询各自再 fan-out 成多个 pipeline driver，
争抢 BE scan/exec 线程池。c=100 本身已有 100 路查询级并行，查询内并行只增加竞争。
dop sweep {0,1,2,4}：0→185 / **1→196** / 2→187 / 4→190，dop=1 明显最优。

```sql
SET GLOBAL pipeline_dop = 1;
```
**持久化**：SET GLOBAL 写入 FE 元数据，**FE 重启保留**。

### E. enable_profile = false — 195 → 213 (+9%)

**机制**：每查询的 runtime profile 构建 + 上报约占 ~9% 开销。A/B 决定性：
profON `194.9/194.6/197.1` vs profOFF `215.8/212.9/210.3`（同会话背靠背）。对召回/正确性零影响（profile 是纯观测）。

```sql
SET GLOBAL enable_profile = false;
```
**持久化**：SET GLOBAL，FE 重启保留。
**⚠️ Tradeoff**：关掉后无法通过 profile dump 调试慢查询。压测/demo 集群可关；生产建议保留（付 ~9% QPS）
或仅在排查时临时开。

---

## 3. 无效 / 已排除的尝试（避免后人重走）

| 尝试 | 结果 | 原因 |
|---|---|---|
| BE RAM-tier datacache (`datacache_mem_size` 0→12G) | 无效 | BE 在 c=100 下 ~50% idle，非 IO/CPU bound |
| SearcherPool size > 1 (`PAIMON_LUMINA_SEARCHER_POOL_SIZE`) | 回退 | N× index read-stream 抖动，反而降 QPS |
| `lumina_search_parallel_number` 5–24 sweep | no-op | 278ms floor 不是查询内搜索线程数限制，是元数据重扫 |
| `list_size` / `beam_width` sweep | 边际/无 | 同上，瓶颈不在 lumina 内部 |
| `top_index_local_rows` sweep | 边际 +14% @c=10 | 低并发有效，c=100 被上游 ceiling 覆盖 |

**核心教训**：c=100 下 3 个 BE 全程 ~50% idle，**所有 BE-side compute 杠杆都是 no-op**。
瓶颈历史上在 FE 规划期 HTTP，现在在 Phase-2 主查询 brpc 等待。**先 jstack FE / 看 BE idle%，再动杠杆。**

---

## 4. 完整复现配方（冷集群 → 213 QPS）

> 前提：FE 已部署带 `BYPASS_BE_ROTATION` 轮询的 jar（§5）；driver 用 `BYPASS=true`。

```bash
# 1. BE 元数据缓存 TTL（3 个 BE 全做）
for be in 10.105.221.220 10.105.221.221 10.105.221.222; do
  curl -s -X POST "http://$be:18040/api/update_config?paimon_manifest_scan_cache_ttl_ms=600000"
  curl -s -X POST "http://$be:18040/api/update_config?paimon_scan_metadata_cache_ttl_ms=600000"
done

# 2. FE 配置 + session（mysql -h127.0.0.1 -P9030 -uroot）
ADMIN SET FRONTEND CONFIG ('enable_paimon_snapshot_id_cache' = 'true');
ADMIN SET FRONTEND CONFIG ('enable_paimon_global_index_shard_cache' = 'true');
SET GLOBAL pipeline_dop = 1;
SET GLOBAL enable_profile = false;
```

3. warm 30s c=100，再正式跑。
4. **验证**：evaluate_count 三 BE 均衡（`/metrics`）、3 BE `http=200`、QPS ≥ 200。

**FE 重启后必做**：重新 apply 第 2 步的两个 `ADMIN SET FRONTEND CONFIG`（否则 QPS→16）；
两个 `SET GLOBAL` 会自动保留。**BE 重启后必做**：重新 `update_config` 两个 TTL（或已写入 be.conf）。

---

## 5. 代码变更清单（仓库 / 分支 / commit）

涉及 3 个仓库。本地路径见 memory `reference_paimon_source_paths`。

### 仓库 1：stella-3.5-global-index (FE, Java)
- **分支**：`xiaolong/perf-r2-snapshot-cache`
- **已部署 FE 版本**：`starrocks-3.5.16-1.1.3-gimetacache-202606141959`（commit `f8eecbbcd10`，含 gimetacache），
  当前线上 jar 已**热替换**为本地构建的 +轮询 版本。

| commit | 内容 | 是否有效 / 处置 |
|---|---|---|
| `f8eecbbcd10` | gimetacache：snapshot-id + GI shard 进程级缓存 | ✅ 有效（阶段 A），保留 |
| `a8f70b9a2e2` | R2.D opt-in 跨查询 snapshot-id 缓存 | ✅ 有效，保留 |
| `06c32ab66d9` `3a7682420aa` | R5/R4：FE 侧 wire BE 元数据缓存 TTL config | ✅ 有效（阶段 B 的 FE 配套），保留 |
| `e2d6d96af7d` `d4e5a209741` `5d91dc7fe3f` `e074e993e3a` | R6 FE→BE thrift 旁路（PoC→真实 evaluator→去 attachmentHandler） | ✅ 有效（BYPASS 路径基础），保留 |
| `8e24fbbc868` | 降 FE 日志量防 /mnt/disk1 写满 | ✅ 有效（压测必需），保留 |
| `eb22c9f22c9` `806ec7add2c` `8289f9a51f7` `9319e5fd2c0` | 各类 timer/trace（evaluate / getRemoteFiles / inner-SQL / ScanCreate） | ⚠️ 诊断用，**可 rebase 掉**（上线前精简） |
| **本次新提交** | `PaimonGlobalIndexService.java` evaluate RPC 三路轮询 (`BYPASS_BE_ROTATION`) | ✅✅ 有效（阶段 C），**核心** |

**未提交、属于其它工作流的改动**（不进本次 QPS commit，避免混入）：
- `ApplyTopNIndexRule.java`：similarity-vs-distance 排序方向修正 — 属**召回修复**（见 plan `synchronous-tickling-sedgewick`），独立提交。
- `InternalSqlExecutor.java` / `StmtExecutor.java`：timer 拆分 + SQL WARN 截断 — 诊断/日志，独立处理。

### 仓库 2：paimon-cpp (BE, C++)
- **分支**：`xiaolong/perf-r5-manifest-cache`
- 构建链：`emr-olap-paimon-cpp` Jenkins（agent 必须 `slave4-emr-olap-starrocks-35-develop`，
  参数 `PAIMON_CPP_BRANCH` + `INSTALL_STARROCKS_THIRDPARTY=True`）→ 触发 stella build。见 memory `reference_paimon_cpp_build`。

| commit | 内容 | 是否有效 / 处置 |
|---|---|---|
| `07e7508e` | R4：TableSchema + latest Snapshot 进程级缓存 | ✅ 有效（阶段 B BE 侧），保留 |
| `ef2819ca` | R5：manifest scan 输出进程级缓存 | ✅ 有效（阶段 B BE 侧），保留 |
| `5a585738` | BugFix：deep-copy 缓存元数据，止 shared_ptr 引用计数竞争 | ✅✅ 关键（解除轮询崩溃 blocker），保留 |
| `38194a5a` | BugFix：缓存 manifest Bytes 改用进程生命周期 pool | ✅✅ 关键（同上），保留 |
| `a6b00b62` `eb4ffca1` | BTreeIndexMeta 编码 / 非对称 fallback 修复 | ✅ 召回正确性，保留 |
| `6325b574` | R3.1：ScanCreate 分段计时 out-param | ⚠️ 诊断用，**可 rebase 掉** |
| `69a4f50a` | SearcherPool（N searcher/index） | ❌ **无效，建议 drop**（§3，反降 QPS） |
| `af942241` | GlobalIndexReaderCache single-flight | ✅ 防 thundering herd，保留 |
| `92fc462b` `fada5ee1` `5fabb264` | LuminaFileReader read-ahead + reader cache | ⚠️ 收益边际，cap=0 可禁；保留但非必需 |

### 仓库 3：paimon (Java, paimon-1-ali-fork)
- **分支**：`xiaolong/perf-r1-token-role-cache`

| commit | 内容 | 是否有效 / 处置 |
|---|---|---|
| `5d73e164b` | DLF ECS token + role-name 进程级缓存 (`SHARED_TOKEN_CACHE`) | ⚠️ **验证有效 (108→139) 但未进当前部署 bundle**；当前线上路径不依赖（gimetacache 已覆盖该 HTTP）。**待定**：若后续主查询瓶颈解除、token 锁重新浮现，再 drop-in（见 §6） |
| `9e5d9dcac` `78e8cd37b` | BTreeIndexMeta null/empty 区分 + selector NPE 修复 | ✅ 召回正确性，保留 |
| (working tree 未提交) `DLFAuthProvider.java` / `LoggingInterceptor.java` / `AbstractFileStoreScan.java` | 调试/日志改动 | ⚠️ 未提交，诊断用 |

---

## 6. 持久化状态总览（哪些会因重启丢失）

| 项 | 载体 | 重启后 | 持久化做法 |
|---|---|---|---|
| gimetacache 两开关 | `ADMIN SET FRONTEND CONFIG` | ❌ FE 重启丢 | 写 `fe.conf` 或重启后脚本重设 |
| `pipeline_dop=1` / `enable_profile=false` | `SET GLOBAL` | ✅ 保留 | 已持久（FE 元数据） |
| BE 两个 TTL=600000 | `update_config` | ❌ BE 重启丢 | 写 `be.conf` |
| FE 轮询代码 | 本地构建热替换 jar | ⚠️ taihao 重新部署 FE 会回退 | **本次 commit + Jenkins 出包**才永久 |
| paimon-cpp 缓存/修复 | 已构建进 BE binary | ✅ | 已在部署 binary 内 |

---

## 7. 下一个瓶颈与 roadmap

**当前态**：~213 QPS，仍 latency-bound，**BE CPU ~50% idle**（idle 32–76%）。

**瓶颈定位**（FE jstack under c=100）：~85–100/100 查询线程 `TIMED_WAITING` 在
`ProtobufRpcProxy.doWaitCallback` ← `ResultReceiver.getNext` ← `DefaultCoordinator.getNext` ←
`StmtExecutor.handleQueryStmt` = **等 BE 主查询 (Phase-2) 结果**（取行 + cosine 重排 + topN），
**不是** DLF token、**不是** GI evaluate。

**下一步思路（按预期收益/工程量）**：

1. **Phase-2 主查询 brpc / BE exec 路径**（最高优先）：主查询 fragment 在 BE 上的 row-fetch + cosine 重排
   是否串行/线程池受限？profile 主查询 fragment（注意需临时开 enable_profile），看 BE 端 exec 是否有锁/队列。
2. **driver 侧**：单 Python driver 进程在 master 上，~213 QPS 时可能成为 co-limiter。查 driver CPU；
   必要时多进程 driver 验证天花板是否在客户端。
3. **token cache drop-in**（条件触发）：若解除 Phase-2 瓶颈后 jstack 重新出现 `getFreshToken`/`DLFAuthProvider`
   锁帧，则把 paimon-java `5d73e164b` 的 `SHARED_TOKEN_CACHE` 构建进 bundle 热替换（步骤见 memory
   `qps-ceiling-is-fe-dlf-rest` FIRST breakthrough）。
4. **代码上线收尾**：轮询 commit 进 Jenkins 出包；rebase 掉 timer/SearcherPool 等无效/诊断 commit；
   gimetacache + BE TTL 写入 conf 文件求持久。

---

## Changelog

| 版本 | 日期 | 变更摘要 |
|---|---|---|
| v1.0 | 2026-06-15 | 初版：QPS 16→213 五阶段完整记录、复现配方、3 仓库 commit 清单、持久化状态、下一瓶颈 roadmap |

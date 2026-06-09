# Paimon Global Index ANN 性能优化 — Final Report

## 🌅 早上 handoff（R1 autonomous run，2026-06-10 02:08-03:45）

| 指标 | baseline | 现状 | 目标 |
|---|---|---|---|
| c=50 sustained QPS | 6.7 | **91** ★ (3-run avg) | ≥ 80 ✅ |
| c=100 peak QPS | — | **99.7** (max 100.02) | — |
| p99 @ c=50 | 8812 ms | **819 ms** | — |

**关键修复**：发现原 P0-C 只 cache 了 token 但**没 cache role-name**，每个 fresh DLFECSTokenLoader 实例都先 HTTP fetch role 才查 token cache（jstack: 90% mysql-nio-pool 等 `getRole`）。新增 `SHARED_ROLE_NAME_CACHE`，c=50 QPS 30 → 87 → 91 (×3 vs prior R0)。

**事故记录**：v1.2 trace deploy (#778) 用 fe/lib 整体 swap 把 P0-C jar 覆盖回原版，被沉默回归了 2h+。已恢复 + 修正。**未来 FE 重 deploy 必须验证 paimon-bundle md5！**

**代码改动状态**：
- **stella FE**：`xiaolong/perf-r1-fe-planner-opt` (commit `806ec7add2c` trace timer) **已 push**，Jenkins #780 tarball 在 OSS
- **paimon-java**：`xiaolong/perf-r1-token-role-cache` (commit `5d73e164b`) **本地 only，未 push** —— 等你决策是否 push / 发 MR

**生产部署状态**（120.26.175.189）：
- FE 正常运行，alive=true
- `starrocks-fe.jar` md5 `0310517c1f...`（含 trace）
- `paimon-bundle-*.jar` md5 `400c2c6e14...`（含 R1.P2 role cache）
- 回滚 backup：`paimon-bundle-*.jar.bak.r1p1_20260610025504`（前一版 = 仅 token cache）

**下一步建议（按 ROI）**：
1. **(短期)** 把 `xiaolong/perf-r1-token-role-cache` push 到 paimon 仓库 + 走正式 MR/cherry-pick 流程（这是 ×3 QPS 的核心修复）
2. **(中期)** R1.P3 — Inner SQL plan cache or 绕开 inner SQL pipeline（直接 BE RPC primitive）：预期 +50% QPS 到 150 QPS。设计 1-2 天，code 3-5 天
3. **(长期)** 横向扩容 BE 数量 → linear scale

剩下完整调研过程见下文 + journal §5.9.15。

---

> 最后更新：2026-06-10 03:45（v1.2，R1 autonomous run 突破 QPS 87-100）

> Stella 3.5.16 / Paimon Global Index DiskANN 在 EMR DLF REST catalog 场景下，把 c=50 sustained QPS 从原始 **6.7 推到 91**（3-run 均值，max 92），c=100 peak **99-100 QPS**（3-run 均值 99.7，max 100.02），p99 从 8.8 s 压到 **0.82 s**。本报告总结落地改动、可复现路径、剩余瓶颈与下一步方向。

## ⚡ v1.2 R1 autonomous run 突破（2026-06-10 02:08-03:15）

| Metric | v1.1 baseline | **v1.2 (R1.P2 deployed)** | 累计 Δ vs original baseline (6.71) |
|---|---|---|---|
| **c=50 QPS** | 29.92 | **87.3** | **×13.0** |
| c=100 QPS | — | **99.3** ★ peak | ×14.8 |
| c=50 p50 | 1618 ms | **554 ms** | −92.5 % vs original 7377 ms |
| c=50 p99 | 2409 ms | **872 ms** | −90.1 % vs original 8812 ms |

### v1.2 关键修复（基于 v1.1 残留瓶颈调研）

**关键发现 1（事故）**：v1.1 期间的 trace deploy (#778) 用 fe/lib 整体 swap 把 P0-C 的 paimon-bundle jar 覆盖回了原版 —— P0-C 失效但未被注意到。所有 v1.1 之后的 c=50 = 29 QPS 都是**没有 P0-C** 的状态。从 master `fe/lib` 取符号确认：原 P0-C 的 `SHARED_TOKEN_CACHE` 已不存在。

**关键发现 2（真 bug）**：手动 overlay P0-C jar 后 c=20 +83 % 但 c=50 持平 29 QPS。c=50 jstack 显示 245 Net.poll 中 **221 (90 %) 仍在 `DLFECSTokenLoader.getRole`**（line 110），不是 `getToken`。原 P0-C 只 cache 了 token，**没 cache role-name**：

```java
@Override
public DLFToken loadToken() {
    if (roleName == null) {
        roleName = getRole(ecsMetadataURL);  // ← HTTP call per fresh instance!
    }
    String cacheKey = ecsMetadataURL + "::" + roleName;
    // ... SHARED_TOKEN_CACHE hits but never reached because getRole blocks here
}
```

每个 per-query DLFECSTokenLoader 实例都先 HTTP fetch role-name，才查 token cache。

**Fix（R1.P2）**：加 `SHARED_ROLE_NAME_CACHE: ConcurrentMap<String, String>` 按 `ecsMetadataURL` 全局缓存 role-name。`paimon-1-ali-26.1-lake-optimizer` 本地 commit `5d73e164b` on branch `xiaolong/perf-r1-token-role-cache`（**未 push**）。本地 mvn build paimon-bundle，scp + 替换 master 上的 jar，重启 FE。

**实测**：c=50 sustained 三次独立运行 = 87-88 QPS，三次结果一致。c=75 = 90.4 QPS，c=100 = 99.3 QPS（**peak**），c=150 = 95.8 QPS（saturate，p50 inflate 到 1.5 s）。

### v1.2 剩余瓶颈（c=150 jstack 实测）

调查 c=150 jstack：900 个 mysql-nio-pool thread slot 中：

| Top frame | 数量 | 占比 |
|---|---|---|
| `java.lang.Object.wait` | 752 | 83.6 % |
| `sun.nio.ch.Net.poll` | 68 | 7.6 % |

具体到 stack pattern：
- **727/900 = 80.8 % 在 `evaluate→InternalSqlExecutor.execute` 路径等 inner SQL BE RPC 响应**（`ProtobufRpcProxy.doWaitCallback → BlockingRpcCallback`）
- 剩余 Net.poll 仅 70 处（全是 DLF loadSnapshot，无 ECS metadata 痕迹 ✅）

→ **新瓶颈是 BE 内层 SQL 执行吞吐上限**（~100 QPS on this 3-BE cluster），不再是 FE auth chain。

### v1.2 部署状态

| Item | Path / Identifier |
|---|---|
| FE branch | `xiaolong/perf-r1-fe-planner-opt` (commit `806ec7add2c` deep timers, Jenkins #780) |
| FE deployed jar | `/opt/apps/STARROCKS3/starrocks-current/fe/lib/starrocks-fe.jar` md5 `0310517c1f...` |
| Paimon branch | `xiaolong/perf-r1-token-role-cache` (commit `5d73e164b`, **local only**, not pushed) |
| Paimon deployed jar | `/opt/apps/STARROCKS3/starrocks-current/fe/lib/paimon-bundle-1-ali-26.1-lake-optimizer.jar` md5 `400c2c6e14...` |
| Paimon jar 备份 | `paimon-bundle-*.jar.bak.r1p1_20260610023931`（前一版本 P0-C-v1 with 只 token cache，可回滚）|

---



## 📌 v1.1 重要修正（2026-06-09 21:50）

之前 v1.0 在 §3.4 "Post-P0-B+C profile" 引用的 30 个 ad-hoc query profile + 5 个保存到 `docs/profiles_p0bc/` 的 profile，**实际是用错的 SQL pattern 测出来的 brute-force 全表扫描数据**，**不是 ANN 工作流**：

- 我手写的脚本用了 `cosine_similarity(vector, array_repeat(...))`（精确版 + 函数调用参数）
- 但 lumina ANN 下推（`ApplyTopNIndexRule.transform`）**仅在以下条件全部满足时触发**（见 `IndexAnalyzer.java:56-60` + `:141-169`）：
  1. 函数是 `approx_cosine_similarity` / `approx_inner_product` / `approx_l2_distance`
  2. 参数中向量列是 `ColumnRefOperator`，另一参数是**字面量** float array
- 我的 ad-hoc query 两条都不满足 → 优化器走全表扫描 → BE 读 1M 行 + 计算 1M 次距离

**但** —— driver_seq6.py / driver.py（实际 bench 脚本）**一直**用 `approx_cosine_similarity` + 真 literal float array（从 vec_file 读 768-dim 数组直接拼接）。所以：

| 数据来源 | workload | 是否有效 |
|---|---|---|
| **Bench QPS / latency 数据**（journal §5.9.9-§5.9.12 + 本报告 §2）| **ANN** | ✅ 有效 |
| BE perf record（c=50 sustained driver bench 期间采的，§3.4 / §4）| **ANN** | ✅ 有效 |
| 我手写的 30 + 5 ad-hoc profile（§3.4 旧版表格）| **brute-force** | ❌ 已修正 |

**核心数据不变**：×4 QPS 提升 / p99 −63 % 都是 ANN 工作流上的真实收益。修正影响仅在"BE vs FE dominator"的归因 —— ANN 工作流下 **FE Planner 是 dominator**，不是 BE。

### ANN 真实 latency 分解（warm 单 query c=1，用正确 SQL）

| 阶段 | 耗时 | 占比 |
|---|---|---|
| Parser | 2 ms | 0.3 % |
| ApplyTopNIndexRule::check (1st) | 90 ms | 15 % |
| ApplyTopNIndexRule::transform | ✅ 运行 | — |
| **ExecPlanBuild** | **456 ms** | **76 %** ← 真 dominator |
| ↳ getPaimonRemoteFileInfos | 97 ms | 16 % |
| ↳ 未细分 ~330 ms | — | 55 % ← 暗物质 |
| Deploy | 13 ms | 2 % |
| **BE QueryCumulativeScanTime** | **7.5 ms** | **1.3 %** ← lumina 几乎 instant |
| **Total wall** | **599 ms** | — |

→ 60 行 RawRowsRead（vs brute-force 1,000,000 行）。lumina ANN 图遍历命中候选极小。**ANN 整个 wall 几乎 100% 在 FE Planner**。

### ANN c=50 sustained 验证（独立 120s bench, 2026-06-09 21:42-21:45）

| Metric | 数值 |
|---|---|
| **QPS** | **29.06** |
| p50 / p95 / p99 | 1694 / 2201 / 2507 ms |
| count | 3,539 / 121.77 s |

跟 journal §5.9.10 的 26.97 QPS 完全吻合。**29 QPS 是当前 3-BE 集群 + P0-B+C 的真实 ANN ceiling**。

### 修正后的剩余优化方向（替换 §6 中的 P2-A 优先级）

由于 ANN 的 BE 工作仅 7.5 ms，BE-侧调优（`num_nodes_to_cache` / `sector_aligned_read`）的 ROI 较 v1.0 估计**显著降低**。真正剩余空间几乎全部在 FE Planner：

| 优先级 | 改动 | 预期 |
|---|---|---|
| **P1-NEW** | 给 `ExecPlanBuild` 加 trace timer，拆解 330 ms "暗物质"（含 backend selector、scan range planning、paimon index metadata 等）| 信息收益 → 锁定下一个 actionable |
| **P1-NEW** | 缓存 `getPaimonRemoteFileInfos`（97 ms / query）跨 query 复用 paimon manifest | −10-15 % wall |
| **P1-NEW** | FE mysql-nio-pool / planner 调度调研：c=50 时为何 per-query 从 600ms 涨到 1700ms（×3 串行化）| 待 trace |
| **P0-D 重新评估**（v1.0 已放弃）| 跨 query snapshot LRU cache（TTL ~1 min）现在能省 ScanIndexFileEntries 26 ms + check 90 ms 冷启动 | 5-10 % wall |
| ~~P2-A num_nodes_to_cache~~ | ~~BE-侧 cache~~ | 几乎无收益（BE 已 7.5 ms） |
| ~~P2-B sector_aligned_read~~ | ~~IO 对齐~~ | 几乎无收益 |
| **P3-A** DiskANN build 重建（ef_construction / neighbor_count）| 同 recall 下 latency ↓ | 仍有效，但需重 build |
| **P3-B** 横向扩容（3 BE → 6 BE）| linear | **仍是最稳的 ×2 路径** |

---

## 目录

- [1. TL;DR](#1-tldr)
- [2. 累积收益](#2-累积收益)
- [3. 已落地的优化](#3-已落地的优化)
- [4. 为什么不再继续在配置层调](#4-为什么不再继续在配置层调)
- [5. 推翻的旧结论](#5-推翻的旧结论)
- [6. 长期方向：build-layer + 扩容](#6-长期方向build-layer--扩容)
- [7. 推荐配置 & 回滚步骤](#7-推荐配置--回滚步骤)
- [8. 附录：完整数据档案](#8-附录完整数据档案)
- [9. Changelog](#9-changelog)

## 1. TL;DR

| 维度 | baseline | 现状 (P0-B + P0-C) | Δ |
|---|---|---|---|
| **QPS @ c=50 (seq6, 1M)** | 6.71 | **26.97** | **×4.02** |
| **QPS @ c=50 (single-shard, 1M)** | 6.65 | **28.80** | **×4.33** |
| **p50** | 7377 ms | **1716 ms** | **−77 %** |
| **p99** | 8812 ms | **3262 ms** | **−63 %** |
| **单 query c=1 wall** | 1083 ms | 784 ms | −28 % |

两层改动累计达成上述提升，**总改动 4 个文件 + 1 个 paimon-java jar 替换 + 1 次 FE 重启**：
- **P0-B** StarRocks FE：`PaimonMetadata` query-scope metadata cache（4 张 DLF REST 调用 → 1）
- **P0-C** paimon-java fork：`DLFECSTokenLoader` 进程级 token cache（消除 fresh AuthProvider 的 ECS HTTP fetch）

P0-D（跨 query snapshot LRU cache）评估后**放弃** — Post P0-B+C 该路径仅占 wall <1 %。

配置层 sweep（list_size / beam_width / parallel_number）实测**全部 ±5 % 噪声范围内**，**无 actionable knob**。剩余优化空间已转移到 build-layer（DiskANN 图重建参数）与横向扩容，超出"短期调优"范围。

## 2. 累积收益

### 2.1 跨表 × 跨并发对照（c=20 / c=50；4 表 × 2 并发）

| 数据集 | conc | baseline | P0-B only | P0-C only | **P0-B+C** | 累计 Δ vs baseline |
|---|---|---|---|---|---|---|
| seq6 (6-shard, 1M) | 20 | 6.54 | 12.80 | — | 12.80*¹ | +96 % |
| seq6 (6-shard, 1M) | 50 | 6.71 | 14.36 | 12.997 | **26.97** | **+302 %** |
| single-shard (1M) | 20 | 6.19 | 13.49 | 12.21 | **23.52** | **+280 %** |
| single-shard (1M) | 50 | 6.65 | 13.51 | 13.90 | **28.80** | **+333 %** |

*¹ c=20 seq6 仅测了 P0-B only，未单独跑 P0-B+C；实测推断 ≥21+。

### 2.2 P0-B vs P0-C：几乎独立可乘

| 单独 | × | 组合 | 实际 ÷ 理论 |
|---|---|---|---|
| P0-B ×2.14 | × | P0-C ×1.94 | **×4.15 理论** vs **×4.02 实测** |

→ 二者攻击 query 路径上**不同的 latency 源**，互不重叠：
- **P0-B**：减少 query 内 `latestSnapshot()` 调用次数（4 → 1）
- **P0-C**：减少每次 `latestSnapshot()` 内 cold-start fresh AuthProvider 触发的 ECS metadata HTTP fetch

### 2.3 延迟视角（seq6 c=50）

| Metric | baseline | P0-B only | P0-C only | **P0-B+C** |
|---|---|---|---|---|
| QPS | 6.71 | 14.36 | 12.997 | **26.97** |
| p50 | 7377 ms | 3399 ms | 3682 ms | **1716 ms** |
| p95 | 8491 ms | 4395 ms | 5185 ms | **2716 ms** |
| p99 | 8812 ms | 4841 ms | 5645 ms | **3262 ms** |

p99 同步压缩 63 %，长尾收敛 — 不只是均值上升。

## 3. 已落地的优化

### 3.1 真正瓶颈定位（关键证据链）

1. **完整 query profile**（QueryId `858032d8`，Total 3.205s）显示 BE Execution 仅 22.7ms = **0.7 %**，FE Planner 占 **98.8 %**。
2. **FE jstack v2**（c=20 稳态，15 次采样）：
   - 19 个 distinct `DLFAuthProvider` instance — 每个 mysql worker 线程都 hold 自己的 monitor（**0 BLOCKED**，无锁竞争）
   - 19 个 thread 全部停在 `sun.nio.ch.Net.poll` 等 ECS metadata HTTP response
3. **paimon-java 源码追踪**：`SnapshotLoaderImpl.load()` 用 `try (Catalog catalog = catalogLoader.load())` → `new RESTCatalog(context)` → `new RESTApi(...)` → `createAuthProvider(...)` → **每次新建 DLFAuthProvider** → token=null → cold-start `DLFECSTokenLoader.loadToken()` HTTP

→ 每 query 在 FE Planner 内被 `ApplyTopNIndexRule.check` + `transform` 两阶段调用共 **4 次 `latestSnapshot()`**，每次都 cold-start auth 链。c=50 并发下 19+ 线程同时打 ECS metadata 形成 throughput ceiling ~6.5 QPS。

### 3.2 P0-B：StarRocks FE query-scope metadata cache

**位置**：`fe/fe-core/src/main/java/com/starrocks/connector/paimon/PaimonMetadata.java`

`PaimonMetadata` 实例由 `MetadataMgr.metadataCacheByQueryId` 按 queryId 缓存 —— 同一 query 内多次 `getOptionalMetadata()` 返回同一实例。利用此性质加 ConcurrentHashMap：

```java
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

**Gating**：mutable FE config `enable_paimon_global_index_metadata_query_cache`（默认 `true`），支持 `ADMIN SET FRONTEND CONFIG` 热改回滚。

**工程**：commit `4af108e10b5`，Jenkins build #765（FE-only，~6.7 min），部署只换 `fe/lib` 不动 BE。

### 3.3 P0-C：paimon-java DLFECSTokenLoader 进程级 token cache

**位置**：`paimon-api/src/main/java/org/apache/paimon/rest/auth/DLFECSTokenLoader.java`（paimon fork `1-ali-26.1-lake-optimizer`）

```java
private static final ConcurrentMap<String, TokenCacheEntry> SHARED_TOKEN_CACHE =
        new ConcurrentHashMap<>();

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
    // Single-flight refresh per cache key
    synchronized (entry.lock) {
        cur = entry.token;
        if (cur != null && !needsRefresh(cur)) return cur;
        DLFToken fresh = getToken(ecsMetadataURL + roleName);
        entry.token = fresh;
        return fresh;
    }
}
```

进程级静态 cache，key 是 `(ecsMetadataURL, roleName)` —— ECS metadata token scope 一致。过期阈值与 `DLFAuthProvider.shouldRefresh()` 一致（`TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 1 hour`）。

**Trade-off**：token revoke 感知滞后从 query 级（秒级）变为 ~1h 级。**生产 ECS IAM revoke 是低频紧急事件**，运维可重启 FE 立即清 cache。已被业务接受。

**工程**：本地 `mvn clean package -DskipTests -pl paimon-bundle -am`（~56s），scp 替换 master 上 `paimon-bundle-*.jar`，重启 FE。无 Jenkins job 触发。

### 3.4 验证：Post-P0-B+C profile + BE perf

**30 个独立 ANN query profile（warm 状态）**：

| Metric | min | p50 | p95 | max |
|---|---|---|---|---|
| FE Planner | 78 ms | 113 ms | 637 ms | 776 ms |
| BE QueryCumulativeScanTime | 1.31 s | 1.42 s | 1.54 s | 1.57 s |
| BE % of wall | — | — | — | 94-96 % |

→ FE Planner 从 3165 ms（v0.7 测）→ **113 ms（×28 reduction）**，BE 成为绝对 dominator。

**3 BE × 60s perf record（c=50 sustained）**：

| Category | 占比 | 性质 |
|---|---|---|
| lumina ANN compute（ExpandNodes + EvalIPAvx512 + SelectExpandNodes）| 32-37 % | C++ user-space，fundamental work |
| kernel scheduling/lock（sched_yield + syscall_enter + __schedule + __lock_text）| 21-23 % | lumina coroutine + IO pipelining 设计开销 |
| jemalloc + userspace lock | 3-7 % | malloc heavy hitter on BE-221/222 |
| ZSTD decompress | 1.3-1.6 % | 不显著 |

## 4. 为什么不再继续在配置层调

### 4.1 三轮 sweep 全部 ±5 % 噪声

| sweep | 范围 | 最佳 | vs baseline | 结论 |
|---|---|---|---|---|
| **list_size × beam_width**（7 组）| LS ∈ {512, 1024, 2048}, BW ∈ {2, 4, 8} | LS512_BW4 = 29.34 | **+4 % QPS, −24 % p99** | 边际 |
| **parallel_number**（7 组）| pn ∈ {1, 2, 4, 5, 8, 16, 32} | pn8 = 29.51 | **+0.7 % QPS** | **完全 flat** |

### 4.2 根本原因（验证证据来自 lumina_release docs）

读 `lumina_release/docs/reference/DiskANNParameters.md` + `OptionsReference.md` 确认：

1. **`diskann.search.beam_width` 已 deprecated**：
   > Kept in schema for compatibility, but the current DiskANN backend does not read it.
   - OptionsReference 表格 `Deprecated: true`
   - **我们的 BW sweep 是无效操作**，−8 % 是纯测量噪声

2. **真正的并行度 knob 是 `search.parallel_number`**（默认 5，文档推荐 2-4），但在 c=50 inter-query 高并发下已无收益空间：
   - 50 个 query × 3 BE × ~16 vCPU = BE 整体 CPU 已饱和
   - lumina 内部已用 **coroutine + IO/compute pipelining**，单 query 内 IO 已异步化，外加 thread 并不 multiply throughput
   - pn=1（串行）与 pn=8 几乎持平 → 证实 intra-query 并行在此 workload 下冗余

3. **lumina Performance Report 直接说明**：
   > Lumina redesigned DiskANN's `beam search` by introducing coroutine-based concurrency for graph traversal. This not only eliminates synchronization barriers between successive `beam` iterations but also enables pipelined execution of computation and IO.

   → BE perf 看到的 `sched_yield 8.4-8.8 %` + `syscall_enter 7.5-9 %` 是 lumina **设计预期的固有开销**，不是 bug。

## 5. 推翻的旧结论

| 旧结论（§5.8/§5.9 v0.5-v0.7）| 真相 |
|---|---|
| "lumina sched_yield 24% 是 binding constraint" | sched_yield 24 % 是 BE idle 等 FE 派发时的 perf 假象。c=50 + FE 不 bottleneck 后降到 8.4-8.8 % |
| "thread 99.9% S → lumina coroutine spinning" | BE worker S 状态是闲着等 RPC，不是 lumina 自旋 |
| "SearcherPool 实验应有正向" | 已实测负向（POOL=4 QPS 5.35 < POOL=1 5.97）；lumina 内部已 pipelined |
| "loadavg 5% 是 lumina 让出 CPU" | BE 工作本来就少（22ms × 6.4 QPS = 0.14 core）|
| "DataCache 命中是关键" | 100% 命中后与瓶颈无关 |
| 6.5 plateau 由 BE 限制 | 由 FE Planner DLF REST + per-query new AuthProvider 限制 |
| §5.9.8 推断"DLFAuthProvider lock contention" | jstack v2 实测 0 BLOCKED；19 个 distinct instances，是 per-query new 而非锁竞争 |
| BW=2 vs BW=4 慢 8 % → coroutine 并行度依赖 BW | beam_width 被 lumina 忽略，差异是噪声 |

## 6. 长期方向：build-layer + 扩容

配置层 sweep 已经穷尽，剩余优化空间需更重的改动。按 ROI × 改动量排序：

| 优先级 | 改动 | 预期收益 | 改动量 | 谁来做 |
|---|---|---|---|---|
| **P2-A** | `diskann.search.num_nodes_to_cache` 启用（默认 0=禁；上限 10 % 节点数）| 重复 query 5-15 % | searcher level config，可能需 stella BE / paimon-cpp 加 option 传参 | stella + paimon-cpp |
| **P2-B** | `diskann.search.sector_aligned_read = true` + `diskann.build.reorder_layout = true` 重 build | IO bound 时 5-10 % | **重 build 索引**（业务可接受性需评估）| 数据团队 + paimon-cpp |
| **P3-A** | DiskANN build 参数调优：`ef_construction` ↑、`neighbor_count` ↑、`disk_encoding.type` 选 | 同 recall 下 latency ↓ 或同 latency 下 recall ↑ | **重 build + 算法验证**，需 ground-truth recall 对比 | 算法团队 |
| **P3-B** | 横向扩容：BE worker 数 3 → 6 | ≈ linear（×1.8-2.0 QPS）| 集群扩容 | 运维 |
| **P3-C** | 纵向扩容：BE 单机 vCPU/内存 ↑ | sub-linear | 集群升级 | 运维 |

**短期内推荐**：P2-A（`num_nodes_to_cache`）—— 不需重 build，但需 stella BE / paimon-cpp 暴露这个 searcher option。预期对**热 query 集中型**业务有 5-15 % 收益。

**中期推荐**：组合 P3-B（横向扩容到 6 BE）+ P2-B（重 build 含 sector_aligned_read），达到 c=50 QPS 60+。

## 7. 推荐配置 & 回滚步骤

### 7.1 当前生产配置（FE）

```sql
ADMIN SET FRONTEND CONFIG ("enable_paimon_global_index_metadata_query_cache" = "true");
-- 默认即开
```

### 7.2 当前 BE config（lumina）

```
lumina_diskann_search_list_size = 1024  -- 默认
lumina_diskann_search_beam_width = 4    -- 默认（lumina 忽略，保留兼容性）
lumina_search_parallel_number = 5       -- 默认
```

无需改动 — sweep 已确认默认值在 sweet spot。

### 7.3 回滚步骤

**P0-B 回滚**（仅 FE config）：
```sql
ADMIN SET FRONTEND CONFIG ("enable_paimon_global_index_metadata_query_cache" = "false");
-- 立即生效，无需重启
```

**P0-C 回滚**（jar swap，需重启 FE）：
```bash
INSTALL=/opt/apps/STARROCKS3/starrocks-current
JAR=paimon-bundle-1-ali-26.1-lake-optimizer.jar
# 备份在 fe/lib/$JAR.bak.20260609180632
$INSTALL/fe/bin/stop_fe.sh
mv $INSTALL/fe/lib/$JAR $INSTALL/fe/lib/$JAR.broken
mv $INSTALL/fe/lib/$JAR.bak.20260609180632 $INSTALL/fe/lib/$JAR
chown starrocks:starrocks $INSTALL/fe/lib/$JAR
export JAVA_HOME=/usr/lib/jvm/java-17
PATH=$JAVA_HOME/bin:$PATH $INSTALL/fe/bin/start_fe.sh --daemon
```

## 8. 附录：完整数据档案

### 8.1 本地

- `docs/paimon-ann-perf-optimization-journal.md`（v1.0，含 §5.9.1-§5.9.13 完整调研日志）
- `docs/profiles_p0bc/profile_p0bc_*.txt`（5 个 P0-B+C 后 query profile）
- `docs/perf_p0bc/perf_{symbol,callgraph}_10.105.221.{220,221,222}.txt`（3 BE × 60s perf record）
- `docs/perf_p0bc/bench.json`（max_qps=27.94 @ c=50 seq6）

### 8.2 Master（120.26.175.189）

- A/B 实测：`/tmp/benchmark/results/p0_2_ab_173012/`（P0-B c=20 单独）
- 综合验证：`/tmp/benchmark/results/p0_2_full_173957/`（P0-B 全 4 表）
- P0-C 实测：`/tmp/benchmark/results/p0_c_full_180741/`（4 case A/B）
- BE perf：`/tmp/benchmark/results/perf_p0bc_195132/`
- 配置层 sweep：`/tmp/benchmark/results/sweep_p93_200801/`（list_size × beam_width）
- 配置层 sweep：`/tmp/benchmark/results/sweep_pn_210123/`（parallel_number）

### 8.3 关键代码改动

- StarRocks Stella：commit `4af108e10b5` on branch `xiaolong/bugfix-paimon-global-index-topn-null-deref`
- paimon-java fork：本地改动未 commit，jar 备份在 master `fe/lib/*.bak.20260609180632`

### 8.4 关键 git 仓库

- Stella：`/Users/drake_wang/workspace/alibaba/stella-3.5-global-index`
- paimon-java：`/Users/drake_wang/workspace/alibaba/paimon`
- paimon-cpp：`/Users/drake_wang/workspace/alibaba/paimon-cpp`
- lumina_release（仅 artifacts + docs）：`/Users/drake_wang/workspace/alibaba/lumina_release`

## 9. Changelog

| 版本 | 日期 | 变更摘要 |
|---|---|---|
| **v1.2** | **2026-06-10 03:15** | **R1 autonomous run 突破 QPS 87+**。v1.1 之后发现 fe/lib swap 把 P0-C jar 覆盖回原版（accidental regression），手动 overlay 恢复后 c=20 +83% 但 c=50 仍 ~30 QPS。c=50 jstack 找到真 bug：原 P0-C 只 cache token 不 cache role-name，每个 fresh `DLFECSTokenLoader` 实例都 HTTP fetch role 后才查 token cache。**Fix**：加 `SHARED_ROLE_NAME_CACHE`（paimon local commit `5d73e164b`，未 push）。**结果**：c=50 sustained = **87.3 QPS**（×2.9 vs 29.92），c=100 peak **99 QPS**，p99 2409→872ms (−64%)。c=150 saturate at 95.8 QPS — 新瓶颈在 BE 内层 SQL 执行吞吐（80% mysql-nio-pool 线程等 `ProtobufRpcProxy.doWaitCallback`）。剩余优化空间需绕开 inner SQL pipeline（直接 BE RPC）或横向扩容。 |
| **v1.1** | **2026-06-09 21:50** | **方法学修正**。在文首加 "v1.1 重要修正" 节：之前 v1.0 §3.4 引用的 30 + 5 个 ad-hoc query profile 实际是 brute-force 工作流（手写 SQL 用了 `cosine_similarity` 而非 `approx_*` + `array_repeat` 而非 literal float array，未触发 lumina ANN 下推）。已说明 lumina ANN 触发的两个必要条件（`IndexAnalyzer.java:56-60` + `:141-169`）。**核心 bench 数据全部有效**（driver_seq6.py / driver.py 一直用正确 SQL，所以 6.71 → 26.97 / 28.80 QPS 是真实 ANN 工作流 throughput）。修正影响在 dominator 归因：ANN 工作流下 **FE Planner 是 dominator（96 %）**，BE 仅 7.5 ms / query；剩余优化空间从 v1.0 的 BE-侧 P2-A/P2-B 转移到 FE Planner（`ExecPlanBuild` 内 330 ms 暗物质 + 跨 query metadata cache）。独立验证 c=50 sustained = 29.06 QPS。 |
| v1.0 | 2026-06-09 | 初版 final report。基于 journal v1.0 + 全部 sweep 数据撰写。结论：×4 QPS 是配置层 actionable 上限；剩余空间需 build-layer 改动或集群扩容。|

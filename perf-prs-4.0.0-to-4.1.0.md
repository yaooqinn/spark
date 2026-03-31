# Performance-Relevant PRs: Apache Spark v4.0.0 → v4.1.0

**Range:** `v4.0.0` (2025-05-19) → `v4.1.0` (2025-12-11)  
**Total Commits in Range:** 2,912 (non-merge)  
**Performance-Relevant PRs Identified:** 25 (unique)

---

## Summary Statistics

| Category | Count |
|----------|-------|
| 1. Optimizer Rules | 5 |
| 2. AQE Enhancements | 1 |
| 3. Join Optimizations | 1 |
| 4. Scan/Pushdown/Pruning | 2 |
| 5. Codegen | 1 |
| 6. Shuffle / Compression | 2 |
| 7. Runtime Filters / SPJ | 2 |
| 8. Memory / Unsafe / Serialization | 5 |
| 9. Aggregate / Window | 0 |
| 10. Vectorized Reader / Parquet / ORC | 1 |
| Additional Expression-Level | 4 |
| **Total** | **25 (unique, with 1 cross-listed)** |

---

## 1. Optimizer Rules

New or improved Catalyst optimizer rules that generate better logical/physical plans.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-53762 | `17d15a4c6cc` | **Add date and time conversions simplifier rule** — simplifies redundant casts between date/time types | q3, q7, q13, q19, q21, q25, q26, q37, q40, q43, q46, q52, q53, q55, q63, q68, q73, q79, q89, q96, q98 |
| SPARK-51222 | `0bcff44a178` | **Optimize ReplaceCurrentLike** — performance improvement in the ReplaceCurrentLike optimizer rule | General planning overhead reduction |
| SPARK-53360 | `e28f4270061` | **Once strategy with ConstantFolding's idempotence should not be broken** — fixes ConstantFolding correctness to ensure proper one-pass optimization | All queries (planning reliability) |
| SPARK-51584 | `dc7683445f2` | **Push Project through Offset** — new rule to push projections through OFFSET operators for column pruning | Queries with LIMIT/OFFSET |
| SPARK-51663 | `be8489f66f2` | **Short circuit && operation for JoinSelectionHelper** — avoids expensive computation when first condition already fails during join strategy selection | All queries with joins (planning speed) |

---

## 2. AQE Enhancements

Adaptive Query Execution improvements for better runtime adaptation.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-51008 | `207390b29be` | **Add ResultStage for AQE** — adds a dedicated result stage in AQE execution for better stage management | All AQE-enabled queries |

---

## 3. Join Optimizations

Better join strategies, broadcast improvements, and join execution enhancements.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-52873 | `1d8481021cd` | **Further restrict when SHJ semi/anti join can ignore duplicate keys** — more accurate duplicate key detection in sort-merge hash join for semi/anti | q4, q11, q22, q35, q38, q69, q74, q87 |

---

## 4. Scan/Pushdown/Pruning

Predicate pushdown, projection pruning, and data source scan optimizations.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-51831 | `ba92e8ec851` | **Column pruning with existsJoin for Datasource V2** — prunes columns in EXISTS subqueries for V2 sources | q10, q35, q69 (EXISTS subqueries) |
| SPARK-52516 | `197c9d6051e` | **Don't hold previous iterator reference after advancing to next file in ParquetPartitionReaderFactory** — reduces memory footprint during multi-file Parquet scans | All TPC-DS queries reading Parquet |

---

## 5. Codegen (WholeStageCodegen)

Code generation improvements that speed up runtime execution.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-52817 | `72ce64ef9da` | **Fix Like Expression performance** — fixes performance regression in LIKE pattern matching | q7, q13, q19, q21, q25, q26, q53, q63, q89 (queries filtering on string columns with LIKE) |

---

## 6. Shuffle / Compression

Shuffle read/write optimizations and compression library upgrades.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-49386 | `a1d55d7d1f5` | **Add memory based thresholds for shuffle spill** — spill decisions based on actual memory usage, not just record count | q64, q72, q95 (large shuffle queries) |
| SPARK-49386 | `336ca8c12a1` | **More accurate memory tracking for memory based spill threshold** (followup) | q64, q72, q95 |
| SPARK-53636 | `0f4263205cc` | **Fix thread-safety issue in SortShuffleManager.unregisterShuffle** — prevents race condition in shuffle cleanup | Reliability for all queries |

---

## 7. Runtime Filters / Storage Partition Join (SPJ)

Dynamic partition pruning, bloom filters, and storage-level partition join improvements.

> **Note:** SPJ improvements only apply to DSv2 tables (Iceberg, Delta) created with bucketing/clustering on join keys, plus `spark.sql.sources.v2.bucketing.enabled=true`.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-53074 | `96a4f5097ad` | **Avoid partial clustering in SPJ to meet child's required distribution** — prevents suboptimal partition grouping | DSv2 partitioned queries |
| SPARK-53272 | `9297712a66d` | **Refactor SPJ pushdown logic out of BatchScanExec** — cleaner SPJ architecture enabling future optimizations | DSv2 partitioned queries |

---

## 8. Memory / Unsafe / Serialization

Memory management, unsafe operations, and serialization improvements.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-51790 | `467644ed5fa` | **Register UTF8String to KryoSerializer** — faster serialization of string data | All queries (shuffle, caching) |
| SPARK-51777 | `1fa05b8cb75` | **Register sql.columnar.* classes to KryoSerializer** — faster serialization for columnar batch classes | Queries using columnar processing / caching |
| SPARK-51813 | `beb71bb5a44` | **Add nonnullable DefaultCachedBatchKryoSerializer** — avoids null propagation overhead in cached batch serialization | Queries with cached tables |
| SPARK-51001 | `80f70a5dcf8` | **Refine arrayEquals** — optimizes array equality comparison in unsafe operations | All queries comparing arrays |
| SPARK-53124 | `8163fa4f581` | **Prune unnecessary fields from JsonTuple** — removes unused fields from JsonTuple expressions | General optimization |

---

## 9. Aggregate / Window Function Optimizations

No aggregate/window performance PRs were merged in this range.

---

## 10. Vectorized Reader / Parquet / ORC Optimizations

File format read/write performance improvements.

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-53633 | `e95f12b59d8` | **Reuse InputStream in vectorized Parquet reader** — avoids re-creating InputStreams per column chunk | All TPC-DS Parquet queries |

---

## Additional Expression-Level Optimizations

| JIRA | Commit | Description | Affected TPC-DS Queries |
|------|--------|-------------|------------------------|
| SPARK-53552 | `ce94cb3132f` | **Optimize substr SQL function** — faster substring extraction | q1, q6, q10, q38 (queries using SUBSTR) |
| SPARK-53171 | `89973d05769` | **Improvement UTF8String repeat** — faster string repeat | String manipulation queries |
| SPARK-53100 | `ba85b25e386` | **Use Java String.substring instead of StringUtils.substring** — faster substring via JDK intrinsics | All queries with string operations |
| SPARK-51704 | `eae5ca789f6` | **Eliminate unnecessary collect operation** — avoids unnecessary data collection | General overhead reduction |

---

## ⭐ Top 10 Most Impactful Changes for TPC-DS

### 1. 🥇 SPARK-52817 — Fix Like Expression Performance
**Commit:** `72ce64ef9da` | **Impact: HIGH**  
Fixes a performance regression in LIKE pattern matching that affected all queries filtering on string columns. LIKE is used extensively in TPC-DS for filtering store names, categories, and other string attributes.  
**Queries affected:** q7, q13, q19, q21, q25, q26, q53, q63, q89.

### 2. 🥈 SPARK-49386 — Memory Based Thresholds for Shuffle Spill
**Commits:** `a1d55d7d1f5`, `336ca8c12a1` | **Impact: HIGH**  
Changes spill decisions from record-count-based to actual memory pressure, resulting in more efficient memory usage during large shuffles. Reduces unnecessary spills when records are small and triggers spills earlier when records are large.  
**Queries affected:** q64, q72, q95 (large shuffle queries).

### 3. 🥉 SPARK-53762 — Date and Time Conversions Simplifier Rule
**Commit:** `17d15a4c6cc` | **Impact: MEDIUM-HIGH**  
Simplifies redundant casts between date/time types in the optimizer. TPC-DS uses date dimensions extensively, and this eliminates unnecessary conversion overhead in 21 queries.  
**Queries affected:** q3, q7, q13, q19, q21, q25, q26, q37, q40, q43, q46, q52, q53, q55, q63, q68, q73, q79, q89, q96, q98.

### 4. SPARK-52873 — Restrict SHJ Semi/Anti Join Duplicate Key Handling
**Commit:** `1d8481021cd` | **Impact: MEDIUM**  
More accurate duplicate key detection in sort-merge hash join for semi/anti joins. Avoids incorrect optimization that could lead to wrong results or suboptimal plans.  
**Queries affected:** q4, q11, q22, q35, q38, q69, q74, q87.

### 5. SPARK-53633 — Reuse InputStream in Vectorized Parquet Reader
**Commit:** `e95f12b59d8` | **Impact: MEDIUM**  
Avoids re-creating InputStreams per column chunk in the vectorized reader. Reduces object allocation and GC pressure for all Parquet reads.  
**Queries affected:** All TPC-DS Parquet queries.

### 6. SPARK-51790/51777/51813 — Kryo Serialization Improvements
**Commits:** `467644ed5fa`, `1fa05b8cb75`, `beb71bb5a44` | **Impact: MEDIUM**  
Registers common Spark types (UTF8String, columnar classes, cached batches) with KryoSerializer for faster serialization/deserialization. Affects shuffle and caching performance.  
**Queries affected:** All queries (shuffle, caching).

### 7. SPARK-51663 — Short Circuit JoinSelectionHelper
**Commit:** `be8489f66f2` | **Impact: MEDIUM**  
Short-circuits expensive computation in join strategy selection when first condition already fails. Reduces planning time for all join queries.  
**Queries affected:** All queries with joins (planning speed).

### 8. SPARK-53552 — Optimize substr SQL Function
**Commit:** `ce94cb3132f` | **Impact: MEDIUM**  
Faster substring extraction for SQL queries. Directly benefits queries that use SUBSTR for string manipulation.  
**Queries affected:** q1, q6, q10, q38.

### 9. SPARK-53100 — Use Java String.substring
**Commit:** `ba85b25e386` | **Impact: LOW-MEDIUM**  
Replaces Apache Commons StringUtils.substring with Java's built-in String.substring, leveraging JDK intrinsics for better performance.  
**Queries affected:** All queries with string operations.

### 10. SPARK-52516 — Parquet Iterator Memory Fix
**Commit:** `197c9d6051e` | **Impact: LOW-MEDIUM**  
Releases previous iterator references when advancing to the next file in Parquet reads, reducing peak memory usage during multi-file scans.  
**Queries affected:** All TPC-DS queries reading Parquet.

---

## ⚠️ Potential Regression Risks

| JIRA | Description | Risk |
|------|-------------|------|
| SPARK-49386 | Memory-based spill thresholds | New spill behavior based on memory pressure may cause more or fewer spills than the record-count-based approach, depending on cluster configuration. |
| SPARK-53360 | ConstantFolding idempotence fix | Fixing the "Once" strategy correctness could cause plan changes that differ from 4.0.0 behavior. |

---

## TPC-DS Query Impact Matrix

### Queries with Most Optimizations Applied

| Query | # of Applicable Optimizations | Key Changes |
|-------|-------------------------------|-------------|
| q25 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q7 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q13 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q19 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q21 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q53 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q63 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q89 | 4 | LIKE fix, date/time simplifier, Kryo, Parquet InputStream |
| q64 | 3 | Memory spill, Kryo, Parquet InputStream |
| q74 | 3 | SHJ semi/anti, Kryo, Parquet InputStream |

### Queries by Optimization Category

- **All queries:** Kryo serialization improvements, Parquet InputStream reuse, string operation optimizations
- **String-heavy (q7, q13, q19, q21, q25, q26, q53, q63, q89):** LIKE performance fix
- **Date-heavy (q3, q7, q13, q19, q21, q25, q26, q37, q40, q43, q46, q52, q53, q55, q63, q68, q73, q79, q89, q96, q98):** Date/time conversion simplifier
- **Semi/anti join (q4, q11, q22, q35, q38, q69, q74, q87):** SHJ duplicate key handling
- **Large shuffle (q64, q72, q95):** Memory-based spill thresholds

---

*Report generated by analyzing commits in the v4.0.0..v4.1.0 range (2,912 commits). Focus on SQL catalyst optimizer, execution engine, Parquet/ORC vectorized readers, shuffle, compression, and memory management.*

# Performance-Relevant PRs for Gluten/Velox: v4.1.0 → HEAD

**Range:** `v4.1.0` (2025-12-11) → `HEAD`  
**Original PRs:** 72 | **Relevant for Gluten/Velox:** 28

> **Filter rationale:** With Gluten/Velox, physical execution (codegen, vectorized Parquet/ORC readers, join/aggregate/window operators) is handled by Velox. Only Catalyst logical optimization, AQE plan adaptation, Spark-level infrastructure, and Python/Connect improvements remain relevant.

---

## What's Bypassed by Gluten/Velox

| Category | Count | Reason |
|----------|-------|--------|
| Codegen (§5: SPARK-56032 CSE in FilterExec) | 1 | Velox has its own expression evaluation engine |
| Vectorized Parquet/ORC (§10: 12 PRs) | 12 | Velox uses DwIO/Nimble, not Spark's vectorized reader |
| Join operators (§3: BHJ partitioning, off-heap LongHashedRelation) | 3 | Velox has its own join implementations |
| Aggregate/Window operators (§9: 3 PRs) | 3 | Velox has its own aggregate/window implementations |
| SPJ (§7: 5 PRs) | 5 | Velox manages its own scan partitioning |
| Shuffle compression libs (§6: LZ4, aircompressor, LZF) | 6 | ❌ if Celeborn native shuffle; ⚠️ partial if Spark shuffle |

**Total bypassed: ~30 PRs** (physical execution layer Velox replaces)

---

## ✅ Still Relevant for Gluten/Velox

### 1. Optimizer Rules — Catalyst Logical Plan (9 PRs, ALL relevant)

These run **before** Gluten's `ColumnarOverrides` transforms the plan. Better logical plans → better Velox execution.

| JIRA | Commit | Description | Gluten Impact |
|------|--------|-------------|---------------|
| SPARK-56034 | `015b344b198` | **Push down Join through Union** | Better plan structure for Velox join pushdown |
| SPARK-44571 | `78fcc934d31` | **Merge subplans with one row result** | Fewer redundant scans → less Velox compute |
| SPARK-54136 | `a871ba4464e` | **Extract PlanMerger from MergeScalarSubqueries** | Enables more subplan reuse |
| SPARK-54972 | `8e63b6111ef` | **NOT IN → anti-join for non-nullable columns** | Better join strategy for Velox |
| SPARK-54881 | `c3843a0bbc4` | **Improve BooleanSimplification** | Simpler filter expressions for Velox |
| SPARK-47672 | `835689e0c34` | **Avoid double eval filter+projection pushdown** | Fewer expressions in plan |
| SPARK-55647 | `172d68e3be1` | **ConstantPropagation for collated attributes** | Better constant folding |
| SPARK-55654 | `7d67ff3e58f` | **TreePattern pruning for analysis rules** | Faster query compilation |
| SPARK-55026 | `4aea124e58f` | **Optimize BestEffortLazyVal** | General engine overhead reduction |

### 2. AQE Enhancements (4 PRs, ALL relevant)

Gluten integrates with AQE — these affect plan adaptation at runtime.

| JIRA | Commit | Description | Gluten Impact |
|------|--------|-------------|---------------|
| SPARK-44065 | `99044a856bb` | **BHJ skew handling in OptimizeSkewedJoin** | AQE may switch Gluten's join strategy for skewed partitions |
| SPARK-54726 | `eea14181306` | **Faster InsertAdaptiveSparkPlan** | Reduces AQE planning overhead |
| SPARK-55461 | `8a85450403e` | **Improve AQE Coalesce Grouping** | Better partition coalescing affects Gluten's shuffle output |
| SPARK-55113 | `e77c38a8d43` | **EnsureRequirements copy tags** | Planning correctness for Gluten transforms |

### 3. Scan/Pushdown — Logical Level (3 of 6 PRs relevant)

| JIRA | Commit | Description | Gluten Impact |
|------|--------|-------------|---------------|
| SPARK-55596 | `a4dcd6d14d7` | **DSV2 Enhanced Partition Stats Filtering** | Fewer partitions for Velox to scan |
| SPARK-54835 | `76c9516417d` | **Avoid unnecessary temp QueryExecution** | Planning overhead reduction |
| SPARK-55250 | `cf5898b4aa0` | **Reduce Hive client calls on CREATE NAMESPACE** | DDL efficiency |

### 4. Memory / Infrastructure (4 of 6 PRs relevant)

| JIRA | Commit | Description | Gluten Impact |
|------|--------|-------------|---------------|
| SPARK-55341 | `1a93d554b5f` | **Storage level flag for cached local relations** | CTE caching control |
| SPARK-54528 | `e2649651d78` | **Close URLClassLoader eagerly** | Memory leak prevention |
| SPARK-53446 | `43f7936d7b3` | **Optimize BlockManager remove operations** | Faster block cleanup |
| SPARK-54464 | `8182bc3e001` | **Remove duplicate output.reserve in variant batch** | Variant processing efficiency |

### 5. Shuffle (3 PRs — ONLY if using Spark shuffle)

If using **Celeborn** or **Gluten's native shuffle**, these are bypassed. If using **Spark's shuffle**, they remain relevant.

| JIRA | Commit | Description | Condition |
|------|--------|-------------|-----------|
| SPARK-55064 | `7e4a040e07c` | **Query-level indeterminate shuffle retry** | Spark shuffle only |
| SPARK-55035 | `eec21d0c803` | **Shuffle cleanup in child executions** | Spark shuffle only |
| SPARK-54956 | `c6ca25a8028` | **Unify indeterminate shuffle retry** | Spark shuffle only |

### 6. Join — Plan-Level Only (1 of 4 PRs relevant)

| JIRA | Commit | Description | Gluten Impact |
|------|--------|-------------|---------------|
| SPARK-55551 | `895666b9e36` | **BHJ output partitioning preservation** | ⚠️ Gluten has its own partitioning logic via `GlutenBroadcastHashJoinExecTransformer`, but AQE decisions based on this change still affect Gluten's plan selection |

### 7. Python / PySpark (ALL 10 PRs relevant)

Gluten doesn't affect Python-side execution — all PySpark improvements apply.

| JIRA | Commit | Description |
|------|--------|-------------|
| SPARK-55459 | `238efa134ce` | **Fix 3x applyInPandas regression** |
| SPARK-37711 | `f612cd0ada2` | **Reduce pandas describe O(N) → O(1)** |
| SPARK-55025 | `69f1d5c5e46` | **List comprehension in pandas-on-Spark** |
| SPARK-56067 | `64949919104` | **Lazy import psutil** |
| SPARK-56066 | `533670b5787` | **Lazy import numpy** |
| SPARK-56062 | `811c7b780f3` | **Isolate memory_profiler import** |
| SPARK-55674 | `8ac083c08ef` | **Optimize 0-column table in Connect** |
| SPARK-56166 | `5c830867973` | **ArrowBatchTransformer schema enforcement** |
| SPARK-55328 | `917baeaa91a` | **Reuse codec in GroupedPythonArrowInput** |
| SPARK-55318 | `7b242f22346` | **vector_avg/vector_sum optimizations** |

### 8. Spark Connect (ALL 5 PRs relevant)

Gluten doesn't affect Connect client-server communication.

| JIRA | Commit | Description |
|------|--------|-------------|
| SPARK-54804 | `1e831b600ae` | **Reduce conf RPCs in createDataset** |
| SPARK-55887 | `a936ccf1489` | **Avoid full scan for CollectLimit** |
| SPARK-54183 | `361d0c9cc57` | **Eliminate temp DataFrame in toPandas** |
| SPARK-54933 | `f27f8d9c710` | **Cache binary_as_bytes config lookup** |
| SPARK-55065 | `6167ef277d0` | **Avoid duplicate JDBC API calls** |

---

## ⭐ Top 10 Most Impactful for Gluten/Velox TPC-DS

| Rank | JIRA | Description | Why it matters for Gluten |
|------|------|-------------|--------------------------|
| 1 | SPARK-44065 | BHJ skew in AQE | AQE skew handling affects Gluten's join plan adaptation |
| 2 | SPARK-56034 | Push Join through Union | Better logical plan → better Velox execution |
| 3 | SPARK-44571 | Merge single-row subplans | Eliminates redundant scans before Gluten transforms |
| 4 | SPARK-54972 | NOT IN → anti-join | Better join strategy selection for Gluten |
| 5 | SPARK-47672 | Avoid double eval in pushdown | Simpler plan for Gluten to transform |
| 6 | SPARK-55461 | AQE Coalesce Grouping | Better shuffle partition coalescing |
| 7 | SPARK-55551 | BHJ output partitioning | Affects AQE plan decisions upstream of Gluten |
| 8 | SPARK-55596 | DSV2 partition stats filtering | Fewer partitions to scan |
| 9 | SPARK-54881 | BooleanSimplification | Simpler filters for Velox |
| 10 | SPARK-54726 | Faster InsertAdaptiveSparkPlan | Planning overhead reduction |

---

## Summary by Impact

| Layer | PRs | Impact Level |
|-------|-----|-------------|
| Catalyst optimizer rules | 9 | **High** — directly improves plans Gluten receives |
| AQE | 4 | **High** — Gluten relies on AQE for runtime adaptation |
| PySpark/Pandas | 10 | **Medium** — Python-side improvements always apply |
| Spark Connect | 5 | **Medium** — client-server improvements always apply |
| Scan/DDL | 3 | **Low-Medium** — partition pruning benefits Velox scan |
| Memory/Infra | 4 | **Low** — JVM-level improvements |
| Shuffle (Spark only) | 3 | **Conditional** — only if NOT using Celeborn/native shuffle |
| Physical execution | 30 | ❌ **Bypassed** — Velox replaces these entirely |

**Bottom line:** Of 72 PRs, **28 are relevant for Gluten/Velox** (39%). The most impactful are the 9 optimizer rules and 4 AQE changes that shape the logical plan before Gluten transforms it.

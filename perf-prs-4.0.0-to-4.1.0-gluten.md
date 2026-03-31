# Performance-Relevant PRs for Gluten/Velox: v4.0.0 → v4.1.0

**Range:** `v4.0.0` (2025-05-19) → `v4.1.0` (2025-12-11)  
**Original PRs:** 25 | **Relevant for Gluten/Velox:** 8

> **Filter rationale:** With Gluten/Velox, physical execution (codegen, vectorized Parquet/ORC readers, join/aggregate operators) is handled by Velox. Only Catalyst logical optimization, AQE plan selection, and Spark-level infrastructure changes remain relevant.

---

## What's Bypassed by Gluten/Velox

| Category | Status | Reason |
|----------|--------|--------|
| Codegen (SPARK-52817 LIKE fix) | ❌ Bypassed | Velox has its own expression evaluation |
| Vectorized Parquet/ORC (SPARK-53633) | ❌ Bypassed | Velox uses DwIO/Nimble readers |
| Join operators (SPARK-52873 SHJ) | ❌ Bypassed | Velox has its own HashJoin/MergeJoin |
| Expression-level (substr, string.repeat) | ❌ Bypassed | Velox has its own string functions |
| Parquet memory (SPARK-52516) | ❌ Bypassed | Velox manages its own scan memory |

---

## ✅ Still Relevant for Gluten/Velox

### 1. Optimizer Rules (Catalyst — runs BEFORE Gluten)

| JIRA | Commit | Description | Impact |
|------|--------|-------------|--------|
| SPARK-53762 | `17d15a4c6cc` | **Date/time conversions simplifier** — eliminates redundant date casts in logical plan | Fewer expressions for Velox to evaluate |
| SPARK-53360 | `e28f4270061` | **ConstantFolding idempotence fix** — ensures correct one-pass optimization | Plan correctness |
| SPARK-51584 | `dc7683445f2` | **Push Project through Offset** — column pruning through OFFSET | Fewer columns scanned by Velox |
| SPARK-51663 | `be8489f66f2` | **Short circuit JoinSelectionHelper** — faster planning | Faster query compilation |
| SPARK-51222 | `0bcff44a178` | **Optimize ReplaceCurrentLike** — planning overhead reduction | Faster query compilation |

### 2. AQE (Gluten integrates with AQE)

| JIRA | Commit | Description | Impact |
|------|--------|-------------|--------|
| SPARK-51008 | `207390b29be` | **Add ResultStage for AQE** — better stage management | Affects Gluten's AQE integration |

### 3. Shuffle (if using Spark shuffle, not Celeborn native)

| JIRA | Commit | Description | Impact |
|------|--------|-------------|--------|
| SPARK-49386 | `a1d55d7d1f5` | **Memory-based shuffle spill thresholds** | ⚠️ Only if using Spark shuffle |
| SPARK-53636 | `0f4263205cc` | **Fix SortShuffleManager thread-safety** | ⚠️ Only if using Spark shuffle |

---

## Summary

With Gluten/Velox in the 4.0.0→4.1.0 range, only **5 optimizer rules + 1 AQE change** are directly beneficial. The high-impact items (LIKE fix, Parquet reader, SHJ) are all in the physical layer that Velox replaces.

**Key takeaway:** This range has limited Gluten/Velox benefit — the foundation work (Kryo, string ops) is mostly physical-layer.

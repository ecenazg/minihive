# MiniHive – Relational Query Processing and Optimization Engine

## Overview

**MiniHive** is a lightweight relational query processing engine that translates SQL queries into relational algebra, applies logical optimizations, and executes the resulting plans using a MapReduce-style execution model implemented with **Luigi**.

This repository is written as **engineering documentation**, not as a homework submission. It is intended to explain the full design, optimization strategy, and execution pipeline of the system.

MiniHive implements a complete vertical slice of a database system:
- SQL parsing
- Relational algebra construction
- Rule-based logical optimization
- MapReduce-style execution
- Communication cost measurement

---

## High-Level Architecture

**SQL Query**  
↓  
**Relational Algebra (AST)**  
↓  
**Logical Optimization Rules**  
↓  
**MapReduce Execution Plan**  
↓  
**Intermediate Files (LOCAL / HDFS)**

Each stage is explicitly implemented and inspectable.

---

## Execution Environments

MiniHive supports two execution modes.

### LOCAL Mode
- Uses the local filesystem
- Suitable for debugging and cost analysis
- Intermediate files stored as `.tmp`

### HDFS Mode
- Uses Hadoop Distributed File System
- Same logical execution as LOCAL
- Designed for larger-scale testing

Execution is controlled via:

```bash
--env LOCAL
--env HDFS
```

## SQL to Relational Algebra (`sql2ra.py`)

This module parses SQL queries and translates them into **relational algebra (RA)** abstract syntax trees using the `radb` framework.

### Supported SQL Features
- `SELECT`, `FROM`, `WHERE`
- Table aliases
- `DISTINCT`
- Cartesian products
- Conjunctive predicates

At this stage:
- **No optimization is applied**
- The focus is on **semantic correctness**

---

## Logical Optimization (`raopt.py`)

Logical optimization is implemented as a **rule-based pipeline**.  
Each rule is conservative, semantics-preserving, and independently testable.

### Optimization Pipeline
1. Break up selections  
2. Push down selections  
3. Merge selections  
4. Introduce joins  
5. Projection pushdown (optimized mode only)  
6. Remove redundant projections  
7. Remove redundant `DISTINCT` operators  

---

## Optimization Rules

### 1. Break Up Selections

Splits conjunctive predicates into stacked selections:

```bash
σ(A ∧ B)(R) → σ(A)(σ(B)(R))
```


This enables finer-grained predicate pushdown.

---

### 2. Push Down Selections

Moves selection predicates as close as possible to base relations when they reference only one side of a product.

**Effect:**
- Reduces intermediate tuple count  
- Minimizes unnecessary data flow  

---

### 3. Merge Selections

Recombines stacked selections after pushdown:
```bash
σ(A)(σ(B)(R)) → σ(A ∧ B)(R)
```


This reduces operator overhead.

---

### 4. Introduce Joins

Detects join predicates applied to cartesian products and replaces them with explicit joins:
```bash
σ(R.a = S.b)(R × S) → R ⨝_{R.a = S.b} S
```

This transformation is essential for efficient execution.

---

### 5. Projection Pushdown (Optimized Mode)

Pushes projections down the query tree to reduce tuple width **before joins and crosses**.

**Key properties:**
- Never removes attributes required by predicates or joins  
- Alias-safe  
- Conservative with unqualified attributes  
- Avoids harmful projections with negligible width reduction  

This rule accounts for the **largest communication cost reductions**.

---

### 6. Remove Redundant Projections

Eliminates nested projections that project the same attribute set:
```bash
π(A)(π(A)(R)) → π(A)(R)
```

This reduces unnecessary MapReduce stages and intermediate files.

---

### 7. Remove Redundant `DISTINCT`

Eliminates `DISTINCT` operators when they are provably unnecessary, for example:
- Projection on a primary key  
- `DISTINCT` following joins that cannot introduce duplicates  

This avoids expensive duplicate-elimination MapReduce jobs.

---

## Execution Engine (`ra2mr.py`)

The relational algebra plan is translated into a **DAG of Luigi tasks**, each representing a MapReduce-style operation.

### Implemented Operators
- Selection  
- Projection  
- Join  
- Cross product  
- Aggregation  
- `DISTINCT`  

Each task:
- Reads JSON-encoded key–value pairs  
- Writes JSON-encoded intermediate files  
- Preserves the output format across optimizations  

---

## Cost Model (`costcounter.py`)

Communication cost is approximated by measuring:

> **Total number of characters in all intermediate MapReduce files**

This metric:
- Ignores input files (same for all runs)  
- Is deterministic and reproducible  
- Serves as a proxy for network and disk I/O  

This allows fair comparison between optimized and unoptimized plans.

---

## Optimization Guarantees

The implementation ensures:
- Optimized execution **never costs more** than unoptimized execution  
- At least half of the benchmark queries show improvement  
- Several queries achieve **more than 66% reduction** in communication cost  
- No changes to file formats or JSON encoding  
- Both optimized and unoptimized modes remain functional  

---

## Why This Project Matters

MiniHive demonstrates how classical database optimization techniques translate into **real execution cost savings** in distributed systems.

It connects:
- Theory (relational algebra)  
- Optimization (rule-based rewriting)  
- Systems (MapReduce-style execution)  
- Measurement (cost modeling)  

This repository serves as:
- A learning resource  
- A systems design reference  
- A foundation for further research or experimentation  

---

## Author

**Ecenaz Güngör**  
**Project:** MiniHive – Relational Query Optimization Engine


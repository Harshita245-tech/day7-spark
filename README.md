# Day 7 – Immutability, Lineage and Fault Tolerance

## 📌 Objective

The objective of Day 7 is to understand **RDD Immutability, RDD Lineage, and Fault Tolerance** using **Apache Spark and Scala**.

This exercise demonstrates:

* RDD immutability
* Multi-step RDD transformations
* RDD lineage
* Spark `toDebugString`
* Fault tolerance through lineage
* Executor loss scenario
* Partition recomputation

---

## 🛠️ Technologies Used

| Technology       | Version                             |
| ---------------- | ----------------------------------- |
| **Scala**        | 2.12.18                             |
| **Apache Spark** | 3.5.3                               |
| **Spark Core**   | 3.5.3                               |
| **SBT**          | Project Build Tool                  |
| **RDD**          | Spark Resilient Distributed Dataset |

---

# 📚 Topics Covered

1. **RDD Immutability**
2. **Multi-step RDD Transformation Chain**
3. **RDD Lineage**
4. **Spark `toDebugString`**
5. **Fault Tolerance**
6. **Executor Loss Scenario**
7. **Partition Recomputation**

---

# 1️⃣ RDD Immutability

RDDs are **immutable distributed collections**.

This means that once an RDD is created, it **cannot be modified**.

When a transformation is applied, Spark creates a **new RDD** instead of changing the original RDD.

### Example

```scala
val numbers = sc.parallelize(1 to 10)
val doubled = numbers.map(_ * 2)
val evenNumbers = doubled.filter(_ % 2 == 0)
```

Here:

* `numbers` → Original RDD
* `doubled` → New RDD created by `map`
* `evenNumbers` → New RDD created by `filter`

The original `numbers` RDD remains unchanged.

### Key Point

> **Transformations never modify an existing RDD. They create new RDDs.**

---

# 2️⃣ Multi-step RDD Transformation Chain

A multi-step transformation chain was implemented using the following operations:

```text
Source RDD
    |
    v
map(_ * 2)
    |
    v
filter(_ > 10)
    |
    v
map(_ + 100)
    |
    v
Final RDD
```

### Input

```text
1, 2, 3, 4, 5, 6, 7, 8, 9, 10
```

### Step 1 – `map(_ * 2)`

```text
2, 4, 6, 8, 10, 12, 14, 16, 18, 20
```

### Step 2 – `filter(_ > 10)`

```text
12, 14, 16, 18, 20
```

### Step 3 – `map(_ + 100)`

```text
112, 114, 116, 118, 120
```

---

# 3️⃣ RDD Lineage

**RDD Lineage** is the record of the sequence of transformations used to create an RDD.

Spark maintains this lineage so that it can **recompute lost partitions** when required.

### Lineage

```text
Source RDD
    |
    v
map(_ * 2)
    |
    v
filter(_ > 10)
    |
    v
map(_ + 100)
    |
    v
Final RDD
```

If a partition is lost, Spark uses this lineage to determine how that partition can be recreated.

### Key Point

> **Lineage tells Spark how an RDD was created and allows Spark to recompute lost data.**

---

# 4️⃣ Spark `toDebugString`

Spark provides the `toDebugString` method to inspect an RDD's dependency and lineage information.

### Example

```scala
println(step3.toDebugString)
```

This displays the RDD dependency chain maintained by Spark.

It is useful for understanding:

* RDD dependencies
* Parent and child RDDs
* Partition information
* Narrow and wide dependencies

---

# 5️⃣ Fault Tolerance

RDDs provide **fault tolerance through lineage**.

If a partition is lost, Spark does not need to recompute the entire dataset.

Instead, Spark uses the lineage information to identify the transformations required to recreate the **lost partition**.

### Example

```text
Source RDD
    |
    v
map(_ * 2)
    |
    v
filter(_ > 10)
    |
    v
map(_ + 100)
    |
    v
Final RDD
```

If a partition of the final RDD is lost, Spark follows the lineage and recomputes the required partition.

### Key Point

> **Spark can recover lost RDD partitions by recomputing them from their lineage.**

---

# 6️⃣ Executor Loss Scenario

An executor loss scenario was demonstrated **conceptually**.

Assume an executor containing one or more RDD partitions is lost.

Spark performs the following steps:

1. **Detects** that the partition is unavailable.
2. **Checks the RDD lineage.**
3. **Identifies** the required input partition.
4. **Re-executes** the required transformations.
5. **Recomputes** the lost partition.
6. **Continues processing** using the available partitions.

The entire dataset does **not** need to be recomputed.

---

# 7️⃣ Partition Recomputation

Consider the following RDD transformation chain:

```text
Source RDD
    |
    v
map(_ * 10)
    |
    v
filter(_ > 50)
    |
    v
Processed RDD
```

If one partition of the processed RDD is lost, Spark uses the lineage to recompute **only that partition**.

### Recomputation Flow

```text
Source Partition
       |
       v
map(_ * 10)
       |
       v
filter(_ > 50)
       |
       v
Recovered Partition
```

The other available partitions are reused.

---

# 🖥️ Sample Output

```text
===== DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE =====

===== 1. RDD IMMUTABILITY =====
Original RDD:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

After MAP:
2, 4, 6, 8, 10, 12, 14, 16, 18, 20

After FILTER:
2, 4, 6, 8, 10, 12, 14, 16, 18, 20

RDD Immutability Explanation:
RDDs are immutable, so transformations do not modify the original RDD.
Each transformation creates a new RDD.

===== 2. MULTI-STEP RDD TRANSFORMATION CHAIN =====
Source RDD:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

Step 1 - MAP (* 2):
2, 4, 6, 8, 10, 12, 14, 16, 18, 20

Step 2 - FILTER (> 10):
12, 14, 16, 18, 20

Step 3 - MAP (+ 100):
112, 114, 116, 118, 120

===== 3. RDD LINEAGE =====
RDD Lineage:
Source RDD
    |
    v
map(_ * 2)
    |
    v
filter(_ > 10)
    |
    v
map(_ + 100)
    |
    v
Final RDD

===== 4. SPARK TODEBUG LINEAGE =====

===== 5. FAULT TOLERANCE =====
RDDs provide fault tolerance through lineage.
If a partition is lost, Spark uses lineage to recompute it.

===== 6. SIMULATE EXECUTOR LOSS =====
Original RDD Partitions:
Number of Partitions: 4

Executor Loss Scenario:
Assume one executor containing a partition is lost.
Spark identifies the lost partition.
Spark follows the RDD lineage.
Spark recomputes only the lost partition.
The remaining partitions do not need to be recomputed.

===== 7. WHAT SPARK RECOMPUTES =====
Source RDD -> map(_ * 10) -> filter(_ > 50)

If a partition of the final RDD is lost:
1. Spark checks the lineage.
2. Spark identifies the input partition required.
3. Spark re-executes the required transformations.
4. Only the lost partition is recomputed.
5. Other available partitions continue to be used.

===== 8. FINAL RESULT =====
Final RDD Count: 15
Final RDD Partitions: 4

===== DAY 7 IMMUTABILITY, LINEAGE AND FAULT TOLERANCE COMPLETED =====
```

---

# ▶️ How to Run

Navigate to the Day 7 project directory:

```bash
cd ~/day7-spark
```

Compile the project:

```bash
sbt compile
```

Run the application:

```bash
sbt run
```

---

# 📁 Project Files

```text
day7-spark/
│
├── src/
│   └── main/
│       └── scala/
│           └── Day7LineageFaultTolerance.scala
│
├── project/
│   └── build.properties
│
├── build.sbt
├── .gitignore
└── README.md
```

### File Description

| File                              | Description                   |
| --------------------------------- | ----------------------------- |
| `Day7LineageFaultTolerance.scala` | Main Spark application        |
| `build.sbt`                       | SBT project configuration     |
| `project/build.properties`        | SBT version configuration     |
| `.gitignore`                      | Ignores generated/build files |
| `README.md`                       | Project documentation         |

---

# 🎯 Result

The Day 7 exercise successfully demonstrated:

* ✅ **RDD Immutability**
* ✅ **Multi-step RDD Transformations**
* ✅ **RDD Lineage**
* ✅ **Spark `toDebugString`**
* ✅ **Fault Tolerance**
* ✅ **Executor Loss Concept**
* ✅ **Partition Recomputation**

The exercise shows how Spark uses **RDD lineage to recover lost partitions without recomputing the entire dataset**.

---

# ✅ Day 7 Completed

**Topic:** Immutability, Lineage and Fault Tolerance
**Framework:** Apache Spark
**Language:** Scala
**Build Tool:** SBT

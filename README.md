````markdown
# Day 7 - Immutability, Lineage and Fault Tolerance

## Objective

To understand RDD immutability, lineage and fault tolerance using Apache Spark and Scala.

This assignment demonstrates a multi-step RDD transformation chain, RDD lineage, why RDDs are immutable, how Spark recovers lost partitions, and how Spark recomputes data when an executor is lost.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Core
- SBT
- RDD

## Topics Covered

1. RDD Immutability
2. Multi-step RDD Transformation Chain
3. RDD Lineage
4. Spark `toDebugString` Lineage
5. Fault Tolerance
6. Executor Loss Scenario
7. Partition Recomputation

## 1. RDD Immutability

RDDs are immutable distributed collections.

This means an existing RDD cannot be modified after it is created.

When a transformation is applied, Spark creates a new RDD instead of changing the original RDD.

Example:

```scala
val numbers = sc.parallelize(1 to 10)
val doubled = numbers.map(_ * 2)
val evenNumbers = doubled.filter(_ % 2 == 0)
````

Here, `numbers`, `doubled`, and `evenNumbers` are separate RDDs.

The original RDD remains unchanged.

## 2. Multi-step RDD Transformation Chain

A multi-step transformation chain was created:

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

For input:

```text
1, 2, 3, 4, 5, 6, 7, 8, 9, 10
```

After `map(_ * 2)`:

```text
2, 4, 6, 8, 10, 12, 14, 16, 18, 20
```

After `filter(_ > 10)`:

```text
12, 14, 16, 18, 20
```

After `map(_ + 100)`:

```text
112, 114, 116, 118, 120
```

## 3. RDD Lineage

RDD lineage is the record of the sequence of transformations used to create an RDD.

Spark maintains lineage so that it can recompute lost data when required.

Lineage for this application:

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

If a partition is lost, Spark uses this lineage to determine how the lost partition can be recreated.

## 4. Spark toDebugString Lineage

Spark provides `toDebugString` to display information about an RDD and its dependencies.

Example:

```scala
println(step3.toDebugString)
```

This helps inspect the RDD dependency chain created by Spark.

## 5. Fault Tolerance

RDDs provide fault tolerance through lineage.

If a partition is lost, Spark does not need to recompute the entire RDD.

Spark uses the lineage information to identify the transformations required to recreate the lost partition.

For example:

```text
Source RDD -> map(_ * 2) -> filter(_ > 10) -> map(_ + 100)
```

If a final partition is lost, Spark follows the lineage and recomputes the required partition.

## 6. Executor Loss Scenario

Executor loss was simulated conceptually.

Assume an executor containing one or more RDD partitions is lost.

Spark:

1. Detects that the partition is unavailable.
2. Checks the RDD lineage.
3. Identifies the input partition required.
4. Re-executes the required transformations.
5. Recomputes the lost partition.
6. Continues processing using the available partitions.

The entire dataset does not need to be recomputed.

## 7. Partition Recomputation

For the following RDD:

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

If one partition of the processed RDD is lost, Spark uses the lineage to recompute that partition.

It re-executes:

```text
Source partition
       |
       v
map(_ * 10)
       |
       v
filter(_ > 50)
       |
       v
Recovered partition
```

Other available partitions are reused.

## Sample Output

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

## How to Run

```bash
cd ~/day7-spark
sbt compile
sbt run
```

## Files

* `Day7LineageFaultTolerance.scala` - Main Spark application
* `build.sbt` - SBT project configuration
* `project/build.properties` - SBT version
* `.gitignore` - Ignores generated files

## Result

Successfully demonstrated RDD immutability, multi-step transformations, RDD lineage, fault tolerance, partition recovery, and conceptual executor loss using Apache Spark.

===== DAY 7 COMPLETED =====


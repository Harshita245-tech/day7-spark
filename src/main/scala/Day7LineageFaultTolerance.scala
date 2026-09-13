import org.apache.spark.{SparkConf, SparkContext}

object Day7LineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    println("\n===== DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE =====")

    val conf = new SparkConf()
      .setAppName("Day7LineageFaultTolerance")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // =========================================================
    // 1. RDD IMMUTABILITY
    // =========================================================
    println("\n===== 1. RDD IMMUTABILITY =====")

    val numbers = sc.parallelize(1 to 10)

    val doubled = numbers.map(_ * 2)

    val evenNumbers = doubled.filter(_ % 2 == 0)

    println("Original RDD:")
    println(numbers.collect().mkString(", "))

    println("After MAP:")
    println(doubled.collect().mkString(", "))

    println("After FILTER:")
    println(evenNumbers.collect().mkString(", "))

    println("\nRDD Immutability Explanation:")
    println("RDDs are immutable, so transformations do not modify the original RDD.")
    println("Each transformation creates a new RDD.")


    // =========================================================
    // 2. MULTI-STEP RDD TRANSFORMATION CHAIN
    // =========================================================
    println("\n===== 2. MULTI-STEP RDD TRANSFORMATION CHAIN =====")

    val sourceRDD = sc.parallelize(1 to 10)

    val step1 = sourceRDD.map(_ * 2)

    val step2 = step1.filter(_ > 10)

    val step3 = step2.map(_ + 100)

    println("Source RDD:")
    println(sourceRDD.collect().mkString(", "))

    println("Step 1 - MAP (* 2):")
    println(step1.collect().mkString(", "))

    println("Step 2 - FILTER (> 10):")
    println(step2.collect().mkString(", "))

    println("Step 3 - MAP (+ 100):")
    println(step3.collect().mkString(", "))


    // =========================================================
    // 3. RDD LINEAGE
    // =========================================================
    println("\n===== 3. RDD LINEAGE =====")

    println("RDD Lineage:")
    println("Source RDD")
    println("    |")
    println("    v")
    println("map(_ * 2)")
    println("    |")
    println("    v")
    println("filter(_ > 10)")
    println("    |")
    println("    v")
    println("map(_ + 100)")
    println("    |")
    println("    v")
    println("Final RDD")

    println("\nLineage Explanation:")
    println("Lineage records the sequence of transformations used to create an RDD.")
    println("Spark uses lineage to recompute lost partitions when required.")


    // =========================================================
    // 4. VIEW SPARK LINEAGE
    // =========================================================
    println("\n===== 4. SPARK TODEBUG LINEAGE =====")

    println(step3.toDebugString)


    // =========================================================
    // 5. FAULT TOLERANCE
    // =========================================================
    println("\n===== 5. FAULT TOLERANCE =====")

    println("RDDs provide fault tolerance through lineage.")
    println("If a partition is lost, Spark does not need to recompute the entire RDD.")
    println("Spark uses the lineage information to recompute the lost partition.")


    // =========================================================
    // 6. SIMULATE EXECUTOR LOSS CONCEPTUALLY
    // =========================================================
    println("\n===== 6. SIMULATE EXECUTOR LOSS =====")

    val largeRDD = sc.parallelize(1 to 20, 4)

    val processedRDD = largeRDD
      .map(_ * 10)
      .filter(_ > 50)

    println("Original RDD Partitions:")
    println(s"Number of Partitions: ${largeRDD.getNumPartitions}")

    println("Processed RDD:")
    println(processedRDD.collect().mkString(", "))

    println("\nExecutor Loss Scenario:")
    println("Assume one executor containing a partition is lost.")
    println("Spark identifies the lost partition.")
    println("Spark follows the RDD lineage.")
    println("Spark recomputes only the lost partition.")
    println("The remaining partitions do not need to be recomputed.")


    // =========================================================
    // 7. WHAT SPARK RECOMPUTES
    // =========================================================
    println("\n===== 7. WHAT SPARK RECOMPUTES =====")

    println("Source RDD -> map(_ * 10) -> filter(_ > 50)")

    println("\nIf a partition of the final RDD is lost:")
    println("1. Spark checks the lineage.")
    println("2. Spark identifies the input partition required.")
    println("3. Spark re-executes the required transformations.")
    println("4. Only the lost partition is recomputed.")
    println("5. Other available partitions continue to be used.")


    // =========================================================
    // 8. FINAL RESULT
    // =========================================================
    println("\n===== 8. FINAL RESULT =====")

    println(s"Final RDD Count: ${processedRDD.count()}")
    println(s"Final RDD Partitions: ${processedRDD.getNumPartitions}")

    println("\n===== DAY 7 IMMUTABILITY, LINEAGE AND FAULT TOLERANCE COMPLETED =====")

    sc.stop()
  }
}

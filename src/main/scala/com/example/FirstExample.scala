package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FirstExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String]

    val animals = List("dog", "cat").toDF("animal")

    val wordCounts = words
      .join(animals, col("animal") === col("value"), "inner")
      .groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
//      .trigger(ProcessingTimeTrigger.create(10, TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
  }
}

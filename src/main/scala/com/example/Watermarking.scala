package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

import java.sql.Timestamp
import java.time.{LocalDate, LocalTime, ZoneId}

object Watermarking {
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
      .map(it => it.splitAt(5))
      .map(it => Word(toTimestamp(it._1), it._2))

    val wordCounts = words
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes", "10 minutes"),
        $"word")
      .count()

    val query = wordCounts.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
//            .trigger(ProcessingTimeTrigger.create(5, TimeUnit.MINUTES))
      .start()

    query.awaitTermination()
  }

  private def toTimestamp(hour: String) = {
    Timestamp.from(LocalTime.parse(hour).atDate(LocalDate.now()).atZone(ZoneId.of("CET")).toInstant)
  }
}

case class Word(timestamp: Timestamp, word: String)

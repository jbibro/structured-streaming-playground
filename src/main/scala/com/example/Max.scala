package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

import java.util.concurrent.TimeUnit

object Max {
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
      .as[String]
      .map(it => Product(it.split(" ")(0), it.split(" ")(1).toInt))
      .groupByKey(it => it.name)
      .mapGroupsWithState[MaxPrice, MaxPrice](GroupStateTimeout.NoTimeout()) {
        case (productName: String, products: Iterator[Product], state: GroupState[MaxPrice]) => {
          if (state.hasTimedOut) {
            val result = state.get
            state.remove()
            result
          } else {
            val maxInThisGroup = products.map(_.price).max
            val updatedMax = if (state.exists) {
              MaxPrice(productName, math.max(state.get.price, maxInThisGroup))
            } else {
              MaxPrice(productName, maxInThisGroup)
            }
            state.update(updatedMax)
//            state.setTimeoutDuration("3 seconds")
            updatedMax
          }
        }
      }


    val query = lines
      .writeStream
      .outputMode("update")
      .trigger(ProcessingTimeTrigger.create(10, TimeUnit.SECONDS))
      .format("console")
      .start()

    query.awaitTermination()

  }
}

case class MaxPrice(productName: String, price: Int)
case class Product(name: String, price: Int)

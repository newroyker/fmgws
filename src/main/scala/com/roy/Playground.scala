package com.roy

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

import scala.concurrent.duration._

object Playground extends App {
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("PlaygroundApp")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.shuffle.partitions", "750")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  spark.conf.set("spark.sql.shuffle.partitions", "42")

  println(spark.conf.get("spark.sql.shuffle.partitions"))

  val stateFunc = (key: Long, values: Iterator[(Timestamp, Long)], _: GroupState[Long]) => {
    Iterator((key, values.size))
  }

  val rateGroups = spark
    .readStream
    .format("rate")
    .load
    .withWatermark(eventTime = "timestamp", delayThreshold = "10 seconds")
    .as[(Timestamp, Long)]
    .groupByKey {
      case (_, value) => value % 2
    }
    .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.EventTimeTimeout)(stateFunc)

  val sq = rateGroups
    .writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime(10.seconds))
    .outputMode(OutputMode.Update)
    .start

  sq.awaitTermination()

  spark.stop()
}

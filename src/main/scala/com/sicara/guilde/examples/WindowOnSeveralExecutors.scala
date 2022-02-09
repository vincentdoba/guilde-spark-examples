package com.sicara.guilde.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WindowOnSeveralExecutors {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Window on several executors")
    .config("spark.driver.host", "localhost")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val schema = StructType(Seq(StructField("string", StringType), StructField("group", IntegerType), StructField("value", IntegerType)))
    val input = spark.read.option("header", "true").schema(schema).csv("/home/vincent/Data/rand_small.csv")

    input
      .withColumn("rank", rank().over(Window.partitionBy("group").orderBy( "value")))
      .filter(col("rank") === 1)
      .withColumn("rank", rank().over(Window.orderBy( "group", "value")))
      .filter(col("rank") === 1)
      .drop("rank")
      .write.mode("overwrite").parquet("/tmp/dropped")

  }

}

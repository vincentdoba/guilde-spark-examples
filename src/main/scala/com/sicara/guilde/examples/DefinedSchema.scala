package com.sicara.guilde.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DefinedSchema {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Use Defined Schema")
    .config("spark.driver.host", "localhost")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.sql.autoBroadcastJoinThreshold", "0")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val schema = StructType(Seq(StructField("string", StringType), StructField("group", IntegerType), StructField("value", IntegerType)))
    val input = spark.read.option("header", "true").schema(schema).csv("/home/vincent/Data/rand.csv")

    input.show(false)

  }

}

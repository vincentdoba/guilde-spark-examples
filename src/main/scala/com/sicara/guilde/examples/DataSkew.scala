package com.sicara.guilde.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataSkew {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Data Skew")
    .config("spark.driver.host", "localhost")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.sql.autoBroadcastJoinThreshold", "0")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val schema = StructType(Seq(StructField("string", StringType), StructField("group", IntegerType), StructField("value", IntegerType)))
    val input = spark.read.option("header", "true").schema(schema).csv("/home/vincent/Data/rand.csv")

    input
      .withColumn("group", when(col("group") > 15, lit(15)).otherwise(col("group")))
      .repartition(16, col("group"))
      .groupBy("group")
      .agg(sum("value").alias("value"))
      .write.mode("overwrite").parquet("/tmp/dropped")

  }

}

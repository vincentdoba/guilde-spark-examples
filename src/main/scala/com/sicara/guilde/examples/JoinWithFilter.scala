package com.sicara.guilde.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object JoinWithFilter {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Join With Filter")
    .config("spark.driver.host", "localhost")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.sql.autoBroadcastJoinThreshold", "0")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val schema = StructType(Seq(StructField("string", StringType), StructField("group", IntegerType), StructField("value", IntegerType)))
    val input = spark.read.option("header", "true").schema(schema).csv("/home/vincent/Data/rand.csv")

    import spark.implicits._
    val joined_dataframe = Seq((1, "coucou")).toDF("group", "joined_value")

    input
      .filter(col("group") === 1)
      .join(joined_dataframe, Seq("group"))
      .write.mode("overwrite").parquet("/tmp/dropped")

  }

}

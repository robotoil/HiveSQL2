package com.datastax.robotoil

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object HiveExample {

  def main (args: Array[String]) {
    print("\nstarting\n")

    // Create Spark configuration.
    val conf = new SparkConf()
      .setAppName("HiveSQLTest")

    // Create Spark context
    val sc = new SparkContext(conf)

    // Create Hive context
    val sqlContext = new HiveContext(sc)

    // Map Cassandra tables(s) for Hive
    sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "demo", "table" -> "users"))
      .load()
      .registerTempTable("demo.users")

    // Query
    val df = sqlContext.sql("select * from demo.users")

    // Output to console the retrieved records.
    df.collect().foreach(println)

    print("\nstopping\n")
  }
}

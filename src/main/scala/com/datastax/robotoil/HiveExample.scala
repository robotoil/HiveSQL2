package com.datastax.robotoil

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object HiveExample {

  def main (args: Array[String]) {
    print("starting\n")

    // Create Spark configuration.
    val conf = new SparkConf()
      .setAppName("HiveSQLTest")

    // Create Spark context
    val sc = new SparkContext(conf)

    // Create Hive context
    val sqlContext = new HiveContext(sc)

    // Map Cassandra tables(s) for Hive

    // Tables for sources and roll-ups
    val df1 = sqlContext.read.format("org.apache.spark.sql.cassandra").
      options(Map("keyspace" -> "demo", "table" -> "users")).
      load().registerTempTable("demo.users")

    val rdd = sqlContext.sql("select * from demo.users")
    // Output to console the retrieved records.
    rdd.collect().foreach(println)

    print("stopping\n")
  }
}

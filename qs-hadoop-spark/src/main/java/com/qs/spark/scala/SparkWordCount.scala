package com.qs.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zun.wei on 2018/6/12 10:44.
  * Description:
  */
object SparkWordCount {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val threshold = args(1).toInt

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    println("-----------------------------")
    tokenized.foreach(e => println(e))
    println("-----------------------------")
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println(charCounts.collect().mkString(", "))
  }

  /*

  spark-submit --class com.qs.spark.scala.SparkWordCount --master local ./qs-hadoop-spark-1.0-SNAPSHOT.jar hdfs://hadoop00:8020/testFile.txt 2

  spark-submit --class com.qs.spark.scala.SparkWordCount --master spark://hadoop00:7077 ./qs-hadoop-spark-1.0-SNAPSHOT.jar hdfs://hadoop00:8020/testFile.txt 2

   */
}

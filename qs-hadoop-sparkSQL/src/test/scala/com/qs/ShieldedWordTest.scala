package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by zun.wei on 2018/6/19 10:13.
  * Description:屏蔽字
  */
object ShieldedWordTest {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("ShieldedWordTest")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    //var path : String = "file:///E:\\json\\ShieldedWord.docx"
    var path : String = "file:///E:\\json\\words.txt"
    if (args.length > 0) path = args(0)

    val wordTest = spark.read.textFile(path)

//    val content : String = "毛泽东"
//    var b : Boolean = false
    wordTest.map(line => {
      val ss = line.split("、")
     ss
    }).filter(words => words.length > 2)
      .show(100000,false)

    //wordTest.show(false)

  }


}

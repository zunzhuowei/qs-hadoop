package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 如何使用data set api
  */
object DataSetApi {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DataSetApi")
      .master("local[2]")
      .getOrCreate()

    var path : String = "file:///E:\\json\\salary.csv"
    if (args.length > 0) path = args(0)

    //隐式转换
    import spark.implicits._

    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show()

    //dataFrame ==> dataSet
    val ds = df.as[Salarys]

    ds.show()

    //data set 在编译的时候就可以发现错误，而dataframe 需要在执行的时候才知道
    ds.map(line => line.ap).show()

    ds.select(ds.col("ap").as("aa")).show()

  }

  case class Salarys(trans_id:Int,cid:Int,iid:Int,ap:Double)

/*

$ cat salary.csv
trans_id,cid,iid,ap
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
111,1,1,500.0
 */


}

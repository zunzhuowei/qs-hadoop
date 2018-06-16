package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
/**
  * 基于Data Frame RDD 的使用
  */
object DataFrameRDDApp {

  /*
  数据原型：
  infos.txt

    1,张三,20,5000
    2,李四,23,555
    3,王五,56,54654
    4,赵六,23,2245

   */

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder()
      .appName("DataFrameRDDApp")
      .master("local[2]")
      .getOrCreate()

    var path : String = "file:///E:\\json\\files.txt"
    if (args.length > 0) path = args(0)


    baseReflect(path) //基于反射实现
    baseProgram(path) //基于编程的方式


    sparkSession.stop()// 关闭
  }




  def baseProgram(path:String) : Unit = {
    print("baseProgram  --- start -----------")
    val sparkSession = SparkSession.builder()
      .appName("DataFrameRDDApp")
      .master("local[2]")
      .getOrCreate()

    //RDD ==> DataFrame
    val rdd = sparkSession.sparkContext.textFile(path)
    //拿到rdd ,
    val infoRdd = rdd.map(_.split(","))
      .map(line => Row(line(0).toInt, line(1), line(2).toInt, line(3).toDouble))

    //创建structType
    val structType : StructType = StructType(Array(
      StructField("id",IntegerType,nullable = true),
      StructField("name",StringType,nullable = true),
      StructField("age",IntegerType,nullable = true),
      StructField("salary",DoubleType,nullable = true)
    ))

    val peopleDF = sparkSession.createDataFrame(infoRdd,structType)
    peopleDF.createOrReplaceTempView("people")
    peopleDF.printSchema()
    peopleDF.show()


    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = sparkSession.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 23") //.limit(1)
    // The columns of a row in the result can be accessed by field index
    teenagersDF.show()

  }



  def baseReflect(path:String): Unit = {
    print("baseReflect  --- start -----------")

    val sparkSession = SparkSession.builder()
      .appName("DataFrameRDDApp")
      .master("local[2]")
      .getOrCreate()

    //RDD ==> DataFrame
    val rdd = sparkSession.sparkContext.textFile(path)
    import sparkSession.implicits._ //隐式转换 toDF()

    val peopleDF = rdd.map(_.split(","))
      .map(line => Info(line(0).toInt, line(1), line(2).toInt, line(3).toDouble)).toDF()

    peopleDF.printSchema()
    peopleDF.show()

    peopleDF.createOrReplaceTempView("people")
    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = sparkSession.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 23") //.limit(1)
    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(attributes => "Name: " + attributes(0)).show()
  }

  case class Info(id:Int,name:String,age:Int,salary:Double)


  //官方文档：http://spark.apache.org/docs/2.1.0/sql-programming-guide.html

  /*
      DataFrame和RDD互操作的两种方式：
    1）反射：case class   前提：事先需要知道你的字段、字段类型
    2）编程：Row          如果第一种情况不能满足你的要求（事先不知道列）
    3) 选型：优先考虑第一种
   */

}

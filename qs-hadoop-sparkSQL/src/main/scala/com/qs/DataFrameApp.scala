package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * DataFrame 的基本使用
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkSession = SparkSession.builder()
      .appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()


    var path : String = null
    if (args.length > 0) {
      path = args(0)
    }else{
      path = "E:\\json\\people.json"
    }
    val peopleDf = sparkSession.read.json(path)


    peopleDf.printSchema()

    //默认输出数据集的前20条数据 select * from table limit 20
    peopleDf.show()

    //查询某列的前100条数据集 select name from table limit 100
    peopleDf.select("name").show(100)

    //查询某几列的数据并进行计算：select name,(age + 10) as age2 from table limit 20
    peopleDf.select(peopleDf.col("name"),(peopleDf.col("age") + 10).as("age2"))
        .show()

    //添加查询条件查询 (1) select name from table where age > 20
    peopleDf.select(peopleDf.col("name")).filter("age > 20").show()

    //添加查询条件查询 (2) select name from table where age > 20
    peopleDf.select(peopleDf.col("name")).filter(peopleDf.col("age") > 20).show()

    //添加查询条件查询 (3) select name from table where age > 20 and name = 'Andy1111'
    peopleDf.select(peopleDf.col("name")).filter("age > 20 and name = 'Andy1111'").show()

    //查看sql的执行计划
    peopleDf.select(peopleDf.col("name")).filter("age > 20 and name = 'Andy1111'").explain()

    //根据某一列进行分组，然后进行聚合操作： select age ,count(1) from table group by age
    peopleDf.filter("age is not null").groupBy("age").count().show()

    println("-----------------------")
    // select * from table where age is not null group by age order by age desc
    peopleDf.select("*")
      .filter("age is not null")
      .groupBy("age")
      .count()
      .orderBy(peopleDf.col("age").desc)
      .show()

    //关闭
    sparkSession.stop()

  }

}

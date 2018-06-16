package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * DataFrameApi操作
  */
object DataFrameApiApp {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("DataFrameApiApp")
      .master("local[2]")
      .getOrCreate()

    var path : String = "file:///E:\\json\\student.data"
    if (args.length > 0) path = args(0)


    import spark.implicits._
    val studentDf = spark.sparkContext.textFile(path)
      .map(_.split("\\|"))
      .map(attr => Student(attr(0).toInt, attr(1), attr(2).toLong, attr(3)))
      .toDF()

    studentDf.printSchema()

    //显示100条，字段值长也不实用省略符号
    studentDf.show(numRows = 100,truncate = false)

    studentDf.head(2).foreach(println)

    println(studentDf.first())

    studentDf.filter("name != ''").show(100)

    //过滤 '' 和 NULL
    studentDf.filter("name != '' and name != 'NULL'").show(100)

    //显示前1000条函数名
    studentDf.sqlContext.sql("show functions").show(1000)

    //name以‘z’开头的人
    studentDf.filter("substr(name,0,1) = 'z'").show()

    //根据id升序排序
    studentDf.sort("id").show(100)

    //根据id降序排序
    studentDf.sort(studentDf("id").desc).show(100)

    //根据id,name升序排序
    studentDf.sort("id","name").show(100)

    //根据id降序，name升序排序
    studentDf.sort(studentDf("id").desc,studentDf("name").asc).show()

    //别名
    studentDf.select(studentDf("name").as("student_name")).show()


    //创建另一个连接表
    val studentDf2 = spark.sparkContext.textFile(path)
      .map(_.split("\\|"))
      .map(attr => Student(attr(0).toInt, attr(1), attr(2).toLong, attr(3))).toDF()


    //使用id作为inner join 的Key
    studentDf.join(studentDf2,"id").show()


    //使用id作为left join 的Key
    studentDf.join(studentDf2,
      studentDf.col("id") === studentDf2.col("id"),
      "left").show()


  }

  case class Student(id:Int,name:String,phone:Long,email:String)

  /*

  $ cat student.data
1|zhangsan|17812345634|34234@qq.com
12|zhangsanda|17812345634|34234@qq.com
14|zhangsanad|17812345634|34234@qq.com
11|zhangsdsafaan|17812345634|34234@qq.com
31|zhangsaddan|17812345634|34234@qq.com
41|zhangsadfan|17812345634|34234@qq.com
1|zhangsan|17812345634|34234@qq.com
61|zhanadfgsan|17812345634|34234@qq.com
178|zhaadfangsan|17812345634|34234@qq.com
91|zhangsan|17812345634|34234@qq.com
01|zhangsdfan|17812345634|34234@qq.com
71|zhangsan|17812345634|34234@qq.com
41|zhangsdfahbvan|17812345634|34234@qq.com
133|zhangsjgfjan|17812345634|34234@qq.com
16|zhangsagasdn|17812345634|34234@qq.com
122|zhangsan|17812345634|34234@qq.com
1562|zhangfdgsdsan|17812345634|34234@qq.com
4351|zhangssfgsan|17812345634|34234@qq.com
1745|zhangsafgn|17812345634|34234@qq.com
1223|zhangsfgsan|17812345634|34234@qq.com
14|zhangsbxnan|17812345634|34234@qq.com
1634|zhangsvbxan|17812345634|34234@qq.com
1745|zhanxnxgsan|17812345634|34234@qq.com
1865|zhangsan|17812345634|34234@qq.com
1234|zhandergsan|17812345634|34234@qq.com
1534||23423423|34234@qq.com
186|NULL|7524523|34234@qq.com
1534||7835672462|34234@qq.com
1|zhangsan|17812345634|34234@qq.com
12|zhangsanda|17812345634|34234@qq.com
14|zhangsanad|17812345634|34234@qq.com
11|zhangsdsafaan|17812345634|34234@qq.com
31|zhangsaddan|17812345634|34234@qq.com
41|zhangsadfan|17812345634|34234@qq.com
1|zhangsan|17812345634|34234@qq.com
61|zhanadfgsan|17812345634|34234@qq.com
178|zhaadfangsan|17812345634|34234@qq.com
91|zhangsan|17812345634|34234@qq.com
01|zhangsdfan|17812345634|34234@qq.com
71|zhangsan|17812345634|34234@qq.com
41|zhangsdfahbvan|17812345634|34234@qq.com
133|zhangsjgfjan|17812345634|34234@qq.com
16|zhangsagasdn|17812345634|34234@qq.com
122|zhangsan|17812345634|34234@qq.com
1562|zhangfdgsdsan|17812345634|34234@qq.com
4351|zhangssfgsan|17812345634|34234@qq.com
1745|zhangsafgn|17812345634|34234@qq.com
1223|zhangsfgsan|17812345634|34234@qq.com
14|zhangsbxnan|17812345634|34234@qq.com
1634|zhangsvbxan|17812345634|34234@qq.com
1745|zhanxnxgsan|17812345634|34234@qq.com
1865|zhangsan|17812345634|34234@qq.com
1234|zhandergsan|17812345634|34234@qq.com
1534||23423423|34234@qq.com
186|NULL|7524523|34234@qq.com
1534||7835672462|34234@qq.com

   */
}

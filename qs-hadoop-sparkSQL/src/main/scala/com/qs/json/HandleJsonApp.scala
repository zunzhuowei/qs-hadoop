package com.qs.json

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

/**
  * Created by zun.wei on 2018/6/21 13:40.
  * Description:处理复杂json 的基本用法
  */
object HandleJsonApp {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[2]")
      .appName("HandleJsonApp").getOrCreate()

    val jsonDf =
      spark.read.format("json")
        .load("file:///D:\\idea_poject\\qs-hadoop\\qs-hadoop-file\\files\\people1.json")


    //缓存
    jsonDf.cache()

    jsonDf.createTempView("people")

    jsonDf.sqlContext.sql("select * from people").show(false)
/*

+----+-------------------------+------------+------------------+-------+
|age |childs                   |friend      |hobby             |name   |
+----+-------------------------+------------+------------------+-------+
|null|null                     |null        |null              |Michael|
|30  |null                     |null        |null              |Andy   |
|19  |null                     |null        |null              |Justin |
|20  |null                     |null        |[篮球, 足球, 计算机]     |Jeff   |
|19  |null                     |null        |[看电视, 打游戏, 看书, 上网]|Mimi   |
|21  |null                     |null        |[看电视, 打游戏, 看书, 上网]|Lisa   |
|28  |null                     |[1,zhangsan]|null              |Linda  |
|28  |[[3,wangwu], [4,zhaoliu]]|null        |null              |Coco   |
+----+-------------------------+------------+------------------+-------+

 */
    jsonDf.sqlContext.sql("select name,hobby[1] from people").show(false)

    /*
+-------+--------+
|name   |hobby[1]|
+-------+--------+
|Michael|null    |
|Andy   |null    |
|Justin |null    |
|Jeff   |足球      |
|Mimi   |打游戏     |
|Lisa   |打游戏     |
|Linda  |null    |
|Coco   |null    |
+-------+--------+
     */

    import spark.implicits._

    jsonDf.select($"name", functions.explode($"hobby")).show(false)
    //<==>两种方法都一样
    jsonDf.sqlContext.sql("select name,explode(hobby) from people").show(false)
/*
+----+---+
|name|col|
+----+---+
|Jeff|篮球 |
|Jeff|足球 |
|Jeff|计算机|
|Mimi|看电视|
|Mimi|打游戏|
|Mimi|看书 |
|Mimi|上网 |
|Lisa|看电视|
|Lisa|打游戏|
|Lisa|看书 |
|Lisa|上网 |
+----+---+

 */

    jsonDf.sqlContext.sql("select name,friend.name,friend.age from people").show()

    /*
    +-------+--------+----+
|   name|    name| age|
+-------+--------+----+
|Michael|    null|null|
|   Andy|    null|null|
| Justin|    null|null|
|   Jeff|    null|null|
|   Mimi|    null|null|
|   Lisa|    null|null|
|  Linda|zhangsan|   1|
|   Coco|    null|null|
+-------+--------+----+
     */

    jsonDf.sqlContext.sql("select name,childs[0].name as cname,childs[0].age as cage from people").show()

/*
+-------+------+----+
|   name| cname|cage|
+-------+------+----+
|Michael|  null|null|
|   Andy|  null|null|
| Justin|  null|null|
|   Jeff|  null|null|
|   Mimi|  null|null|
|   Lisa|  null|null|
|  Linda|  null|null|
|   Coco|wangwu|   3|
+-------+------+----+
 */

    //清缓存
    jsonDf.unpersist(true)

    spark.stop()
  }


}

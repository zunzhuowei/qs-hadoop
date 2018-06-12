package com.qs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by zun.wei on 2018/6/12 10:14.
 * Description:
 */
public class JavaWordCount {

    /**
     * 版本问题未实现。
     * @param args
     */
    public static void main(String[] args) {
        // create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count java"));
        System.out.println("args = " + args[0]);
        JavaRDD<String> lines = sc.textFile(args[0]);
        // Map each line to multiple words
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String line) {
                        return Arrays.asList(line.split(" "));
                    }
                });

// Turn the words into (word, 1) pairs
        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String w) {
                        return new Tuple2<String, Integer>(w, 1);
                    }
                });

// Group up and add the pairs by key to produce counts
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        counts.saveAsTextFile("hdfs://hadoop00:8020/counts.txt");

    }

}

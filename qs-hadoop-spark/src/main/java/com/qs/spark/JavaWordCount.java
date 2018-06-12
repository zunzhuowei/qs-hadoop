package com.qs.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by zun.wei on 2018/6/12 10:14.
 * Description:
 */
public class JavaWordCount {

    public static void main(String[] args) {
/*
        // create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));

        // get threshold
        final int threshold = Integer.parseInt(args[1]);

        // read in text file and split each document into words
        JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
                new FlatMapFunction() {
                    public Iterable call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                new PairFunction() {
                    public Tuple2 call(String s) {
                        return new Tuple2(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        // filter out words with fewer than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(
                new Function, Boolean>() {
            public Boolean call(Tuple2 tup) {
                return tup._2 >= threshold;
            }
        }
    );

        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
                new FlatMapFunction<Tuple2<String, Integer>, Character>() {
                    @Override
                    public Iterable<Character> call(Tuple2<String, Integer> s) {
                        Collection<Character> chars = new ArrayList<Character>(s._1().length());
                        for (char c : s._1().toCharArray()) {
                            chars.add(c);
                        }
                        return chars;
                    }
                }
        ).mapToPair(
                new PairFunction<Character, Character, Integer>() {
                    @Override
                    public Tuple2<Character, Integer> call(Character c) {
                        return new Tuple2<Character, Integer>(c, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        System.out.println(charCounts.collect());*/
    }

}

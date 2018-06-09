package com.qs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对原来的MapReduce进行升级改造，
 * 对输出路径已经存在的目录，把它删除在执行。
 */
public class MapReduce2App {

    private static final long one = 1;

    /**
     * Mapper<LongWritable, Text, Text, LongWritable> 第一个参数表示
     * 扫描的行下标，第二个表示扫描到的值，第三个表示输出的键的类型，第四个表示输出的值的类型
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接受到的每一行数据
            String line = value.toString();
            //按照指定的分隔符进行分割
            String [] words = line.split(" ");
            for (String word : words) {
                //通过上下文把map的处理结果输出
                context.write(new Text(word), new LongWritable(one));
            }
        }

    }


    /**
     *  Reducer<Text, LongWritable, Text, LongWritable>
     *      第一个参数表示输入类型的key，第二个尝试表示输入类型的value对应着
     *      上面的map的接口的输出。
     *      第三个和第四个表示输出结果的Key value
     */
    public static class MyReduceer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * 处理Map 们传过来的值。
         * @param key 每一个词的名称也就是键
         * @param values 每个词出现的次数所以是一个集合
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                //统计每个单词出现的次数
                sum += value.get();
            }
            //输出最终统计结果
            context.write(new Text(key), new LongWritable(sum));
        }
    }


    /**
     * 定义Driver
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path(args[1]);
        boolean b = fileSystem.exists(outPutPath);

        //如果输出路径存在就把它删除
        if (b) {
            System.out.println("output path is exists ,but has deleted");
            fileSystem.delete(outPutPath, true);
        }

        Job job = Job.getInstance(configuration);

        //设置job的处理类
        job.setJarByClass(MapReduce2App.class);

        //设置处理类的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置Map的相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReduceer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业输出路径
        FileOutputFormat.setOutputPath(job, outPutPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

/*

hadoop jar /home/hadoop/qs-hadoop-mapreduce-1.0-SNAPSHOT.jar
 \com.qs.mapreduce.MapReduceApp \
hdfs://hadoop00:8020/hdfsapi/test02/dept.sql \
 hdfs://hadoop00:8020/hdfsapi/wc/

 */


}

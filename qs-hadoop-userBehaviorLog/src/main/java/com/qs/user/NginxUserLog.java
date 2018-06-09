package com.qs.user;

import com.qs.user.source.UserAgent;
import com.qs.user.source.UserAgentParser;
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
 * 统计nginx用户访问日志的os个数基于Mapreduce
 */
public class NginxUserLog {
    private static final long one = 1;

    /**
     * Mapper<LongWritable, Text, Text, LongWritable> 第一个参数表示
     * 扫描的行下标，第二个表示扫描到的值，第三个表示输出的键的类型，第四个表示输出的值的类型
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        UserAgentParser userAgentParser = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接受到的每一行数据
            String line = value.toString();
            //按照指定的分隔符进行分割
            String [] source = line.split("\"");
            if (source.length > 4) {
                UserAgent agent = userAgentParser.parse(source[5]);
                //通过上下文把map的处理结果输出
                context.write(new Text(agent.getOs()), new LongWritable(one));
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
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
        job.setJarByClass(NginxUserLog.class);
        job.setJobName("NginxUserLog");

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
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

/*
hadoop jar qs-hadoop-userBehaviorLog-1.0-SNAPSHOT.jar com.qs.user.NginxUserLog hdfs://hadoop00:8020/access.log hdfs://hadoop00:8020/nginxlogout
 */

    /*
   mapreduce运行结果：

Linux	7546
OS X 10.11	2
OS X 10.13	1
Unknown	95
Windows	5
Windows 7	6
Windows XP	14
iPad OS 10.2	9
iPad OS 11.2	5
iPad OS 11.3	4
iPhone OS 10.0	11
iPhone OS 10.2	60
iPhone OS 10.3	96
iPhone OS 11.0	16
iPhone OS 11.1	19
iPhone OS 11.2	145
iPhone OS 11.3	466
iPhone OS 11.4	63
iPhone OS 8.2	31
iPhone OS 8.3	1183
iPhone OS 8.4	41
iPhone OS 9.0	5
iPhone OS 9.2	2
iPhone OS 9.3	185

     */

    /*
    单机版运行结果：

    result = {
	"iPhone OS 9.3":185,
	"iPhone OS 8.4":41,
	"OS X 10.13":1,
	"iPhone OS 8.3":1183,
	"Windows":5,
	"iPhone OS 9.2":2,
	"OS X 10.11":2,
	"iPhone OS 8.2":31,
	"iPhone OS 9.0":5,
	"Linux":7546,
	"Unknown":95,
	"Windows XP":14,
	"iPhone OS 10.0":11,
	"iPad OS 10.2":9,
	"Windows 7":6,
	"iPhone OS 11.0":16,
	"iPad OS 11.3":4,
	"iPad OS 11.2":5,
	"iPhone OS 11.3":466,
	"iPhone OS 11.4":63,
	"iPhone OS 11.1":19,
	"iPhone OS 10.2":60,
	"iPhone OS 10.3":96,
	"iPhone OS 11.2":145
}
     */
}

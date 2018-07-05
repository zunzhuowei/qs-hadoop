package com.qs.game;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 使用Stomr实现：词频统计
 * 读取指定目录的数据，实现单词计数的功能
 */
public class LocalWordCountStormTopology02 {


    /**
     * Spout 需要继承 BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector spoutOutputCollector;

        /**
         * 初始方法只会被调用一次
         * @param map 配置参数
         * @param topologyContext 上下文
         * @param spoutOutputCollector 数据发射器
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;

        }


        /**
         * 业务：
         * 1)   读取指定目录的文件夹下面的数据：D:\idea_poject\qs-hadoop\qs-hadoop-file\files\storm_wc.txt
         * 2)   把每一行数据发射出去
         */
        @Override
        public void nextTuple() {
            String path = "D:\\idea_poject\\qs-hadoop\\qs-hadoop-file\\files\\storm_wc.txt";
            try {
                boolean b = Files.exists(Paths.get(path));
                if (b) {
                    List<String> lines = FileUtils.readLines(new File(path), "UTF-8");
                    for (String line : lines) {
                        this.spoutOutputCollector.emit(new Values(line));
                        Utils.sleep(1000);
                    }
                    FileUtils.moveFile(new File(path),new File(path + "11"));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 声明输出字段
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("wcSpout"));
        }
    }




    /**
     * 数据切分SplitBolt
     */
    public static class WordCountSplitBolt extends BaseRichBolt {

        private OutputCollector spoutOutputCollector;

        /**
         * 初始化方法，只会执行一次
         * @param stormConf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.spoutOutputCollector = collector;
        }

        /**
         * 是一个死循环，职责：获取Spout发送过来的数据
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("wcSpout");
            String[] words = line.split(" ");
            for (String word : words) {
                this.spoutOutputCollector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("wcSplitBolt"));
        }
    }

    /**
     * 词频统计 bolt
     */
    public static class WordCountBolt extends BaseRichBolt {

        Map<String, Integer> result = new HashMap<>();

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("wcSplitBolt");
            Integer w = result.get(word);
            if (Objects.isNull(w)) {
                result.put(word, 1);
            } else {
                result.put(word, w + 1);
            }
            System.out.println("result = " + result);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("WordCountBolt", new WordCountBolt()).shuffleGrouping("WordCountSplitBolt");


        StormTopology stormTopology = builder.createTopology();

        //创建一个本地的storm集群：本地模式，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology01", new Config(), stormTopology);

    }
}

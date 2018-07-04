package com.qs.game;

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

import java.util.Map;

/**
 * 使用Stomr实现：实时计算求和
 */
public class LocalSumStormTopology01 {


    /**
     * Spout 需要继承 BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector spoutOutputCollector;
        int number = 0;

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
         * 产生数据，在生产环境中肯定是从消息队列中获取数据
         * 这个方式是一个死循环，会一直不端的执行下去
         */
        @Override
        public void nextTuple() {
            this.spoutOutputCollector.emit(new Values(++number));
            System.out.println("number = " + number);

            //防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }


    /**
     * 数据累计求和Bolt:接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        int sum = 0;

        /**
         * 初始化方法，只会执行一次
         * @param stormConf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        /**
         * 是一个死循环，职责：获取Spout发送过来的数据
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            //Bolt中获取值可以根据index获取，也可以根据上一个环节中定义的Field名称获取，常用此方法。
            Integer value = input.getIntegerByField("num");
            sum += value;
            System.out.println("sum = [" + sum + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        BoltDeclarer boltDeclarer = builder.setBolt("SumBolt", new SumBolt());
        boltDeclarer.shuffleGrouping("DataSourceSpout");

        StormTopology stormTopology = builder.createTopology();

        //创建一个本地的storm集群：本地模式，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology01", new Config(), stormTopology);

    }
}

package com.qs.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * Created by zun.wei on 2018/6/11 17:01.
 * Description:
 */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // the port to connect to
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");



        // parse the data, group it, window it, and aggregate the counts
        /*DataStream<WordWithCount> windowCounts = text
                .flatMap((FlatMapFunction<String, WordWithCount>) (value, out) -> {
                    for (String word : value.split("\\s")) {
                        out.collect(new WordWithCount(word, 1L));
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce((ReduceFunction<WordWithCount>) (a, b) -> new WordWithCount(a.word, a.count + b.count));*/

        DataStream<WordWithCount> windowCounts = text
                .flatMap((FlatMapFunction<String, WordWithCount>)
                        (value, out) -> Arrays.stream(value.split("\\s"))
                                .forEach(e -> out.collect(new WordWithCount(e, 1L))))
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce((ReduceFunction<WordWithCount>)
                        (a, b) -> new WordWithCount(a.word, a.count + b.count));


        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        String word;
        long count;

        public WordWithCount() {
        }

        WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

}

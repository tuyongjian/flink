package com.tu.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author tuyongjian
 * @date 2023/4/19 9:56
 */

public class MyKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1 （1个线程执行，以便观察）
        env.setParallelism(1);
        //配置kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-server");
        properties.put("group.id", "flink_group_1");
        //从kafka获取数据
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));
        //wordcount计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //用空格分隔为单词
                String[] words = value.split(" ");
                //统计单词使用频次，放入收集器
                Arrays.stream(words)
                        //洗去前后空格
                        .map(String::trim)
                        //过滤掉空字符串
                        .filter(word -> !"".equals(word))
                        //加入收集器
                        .forEach(word -> out.collect(new Tuple2<>(word, 1)));
            }
        })
                //按照二元组第一个字段word分组，把第二个字段统计出来
                .keyBy(0).sum(1);
        operator.print();

        env.execute();
    }
}

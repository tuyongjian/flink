package com.tu.flink.redis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author tuyongjian
 * @date 2023/4/18 16:09
 */

public class MyKafkaRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1 （1个线程执行，以便观察）
        env.setParallelism(1);
        //配置kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "server");
        properties.put("group.id", "flink_group_1");
        //从kafka获取数据
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));
        System.out.println(streamSource.print());
        // 定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("locahost")
                .setPort(6379)
                .setDatabase(9)
                .build();
        // 将结果写入redis
        streamSource.addSink(new RedisSink<>(config, new MyKafkaRedisSink()));
        // 执行任务
        env.execute("redis sink");
    }
}

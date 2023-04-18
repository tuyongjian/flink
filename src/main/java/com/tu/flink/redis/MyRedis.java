package com.tu.flink.redis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tuyongjian
 * @date 2023/4/18 16:09
 */

public class MyRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1 （1个线程执行，以便观察）
        env.setParallelism(1);
        // 转换成Student类型
       List<Student> list = new ArrayList<>();
        Student student = new Student("tu","30");
        list.add(student);

        DataStreamSource<Student> stream = env.fromCollection(list);

        // 定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .setDatabase(9)
                .build();
        // 将结果写入redis
        stream.addSink(new RedisSink<>(config, new MyRedisSink()));
        // 执行任务
        env.execute("redis sink");
    }
}

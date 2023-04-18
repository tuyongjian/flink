package com.tu.flink.redis;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 *
 * @author tuyongjian
 * @date 2023/4/18 15:56
 */

public class MyRedisSink implements RedisMapper<Student> {

    /**
     * 这里定义使用redis的什么数据结构
     * RedisCommand.SET 代表是redis中的String
     * LPUSH(RedisDataType.LIST),
     * RPUSH(RedisDataType.LIST),
     * SADD(RedisDataType.SET),
     * SET(RedisDataType.STRING),
     * PFADD(RedisDataType.HYPER_LOG_LOG),
     * PUBLISH(RedisDataType.PUBSUB),
     * ZADD(RedisDataType.SORTED_SET),
     * HSET(RedisDataType.HASH);
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"tu_flink");
    }

    @Override
    public String getKeyFromData(Student student) {
        return student.getName();
    }

    @Override
    public String getValueFromData(Student student) {
        return JSON.toJSONString(student);
    }



}

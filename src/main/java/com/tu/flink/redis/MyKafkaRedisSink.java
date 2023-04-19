package com.tu.flink.redis;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 *
 * @author tuyongjian
 * @date 2023/4/18 15:56
 */

public class MyKafkaRedisSink implements RedisMapper<String> {

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
        return new RedisCommandDescription(RedisCommand.SET,"tu_flink");
    }

    @Override
    public String getKeyFromData(String s) {
        return "tu_flink";
    }

    @Override
    public String getValueFromData(String s) {
        return s;
    }


}

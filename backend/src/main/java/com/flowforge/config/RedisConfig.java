package com.flowforge.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    // Redis key helpers
    public static String executionStatusKey(String executionId) {
        return "pipeline:" + executionId + ":status";
    }

    public static String nodeStatusKey(String executionId, String nodeId) {
        return "pipeline:" + executionId + ":node:" + nodeId;
    }

    public static String throughputKey(String executionId, String nodeId) {
        return "pipeline:" + executionId + ":throughput:" + nodeId;
    }

    public static String aiCacheKey(String promptHash) {
        return "ai:cache:" + promptHash;
    }

    public static String pubSubChannel(String executionId) {
        return "pipeline.status." + executionId;
    }
}

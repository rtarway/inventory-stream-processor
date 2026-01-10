package com.inventory.stream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inventory.stream.model.TotalUpdate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Profile("sink")
public class RedisSinkService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisSinkService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "running-totals-updates", groupId = "redis-sink-group")
    public void consume(String message) {
        try {
            TotalUpdate update = objectMapper.readValue(message, TotalUpdate.class);
            String key = update.getKey();

            if ("HALTED".equals(update.getStatus())) {
                redisTemplate.opsForHash().put("inventory:status", key, "HALTED");
                log.info("Updated Redis [HALT]: {}", key);
            } else {
                redisTemplate.opsForHash().put("inventory:totals", key, String.valueOf(update.getTotal()));
                redisTemplate.opsForHash().put("inventory:status", key, "OK");
                log.info("Updated Redis [OK]: {} = {}", key, update.getTotal());
            }

        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
        }
    }
}

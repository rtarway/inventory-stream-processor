package com.inventory.stream.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@Profile("api")
public class ReadController {

    private final StringRedisTemplate redisTemplate;

    public ReadController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/total/{key}")
    public ResponseEntity<?> getTotal(@PathVariable String key) {
        Object statusObj = redisTemplate.opsForHash().get("inventory:status", key);
        String status = statusObj != null ? statusObj.toString() : null;

        if (status == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Not Found");
        }

        if ("HALTED".equals(status)) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Processing Halted for this ID due to integrity violation. Manual intervention required.");
        }

        Object totalObj = redisTemplate.opsForHash().get("inventory:totals", key);
        String total = totalObj != null ? totalObj.toString() : "0.0";

        return ResponseEntity.ok(new Response(key, total));
    }

    record Response(String key, String total) {
    }
}

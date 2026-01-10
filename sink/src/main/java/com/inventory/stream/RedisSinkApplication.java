package com.inventory.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class RedisSinkApplication {
    public static void main(String[] args) {
        SpringApplication.run(RedisSinkApplication.class, args);
    }
}

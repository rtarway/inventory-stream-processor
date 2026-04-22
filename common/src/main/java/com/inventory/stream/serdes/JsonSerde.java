package com.inventory.stream.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inventory.stream.json.JsonMapperFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * JSON Serde for Kafka. Always prefer {@link #JsonSerde(Class)} with an explicit bound type.
 * The no-arg constructor binds to {@link JsonNode} (not {@link Object}) to avoid broad deserialization.
 */
public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> targetType;

    @SuppressWarnings("unchecked")
    public JsonSerde() {
        this((Class<T>) JsonNode.class);
    }

    public JsonSerde(Class<T> targetType) {
        if (targetType == null) {
            throw new IllegalArgumentException("targetType must not be null");
        }
        this.targetType = targetType;
        this.objectMapper = JsonMapperFactory.create();
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return objectMapper.readValue(data, targetType);
                } catch (IOException e) {
                    throw new SerializationException("Error deserializing JSON", e);
                }
            }
        };
    }
}

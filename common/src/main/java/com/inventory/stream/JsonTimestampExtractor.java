package com.inventory.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;
import com.inventory.stream.model.Event;

public class JsonTimestampExtractor implements TimestampExtractor {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            if (record.value() instanceof Event) {
                Event event = (Event) record.value();
                long ts = event.getTimestamp().toEpochMilli();
                return ts;
            } else if (record.value() instanceof byte[]) {
                JsonNode node = mapper.readTree((byte[]) record.value());
                String timestampStr = node.get("timestamp").asText();
                return Instant.parse(timestampStr).toEpochMilli();
            }
            return partitionTime;
        } catch (Exception e) {
            System.err.println("EXTRACTOR ERROR: " + e.getMessage());
            e.printStackTrace();
            return partitionTime;
        }
    }
}

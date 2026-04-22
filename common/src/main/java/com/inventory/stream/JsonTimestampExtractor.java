package com.inventory.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inventory.stream.json.JsonMapperFactory;
import com.inventory.stream.model.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class JsonTimestampExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(JsonTimestampExtractor.class);

    private final ObjectMapper mapper = JsonMapperFactory.create();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            if (record.value() instanceof Event event) {
                if (event.getTimestamp() == null) {
                    log.warn("Event timestamp is null; using partition time");
                    return partitionTime;
                }
                return event.getTimestamp().toEpochMilli();
            }
            if (record.value() instanceof byte[] bytes) {
                JsonNode node = mapper.readTree(bytes);
                JsonNode tsNode = node.get("timestamp");
                if (tsNode == null || tsNode.isNull()) {
                    log.warn("JSON payload missing timestamp; using partition time");
                    return partitionTime;
                }
                if (!tsNode.isTextual()) {
                    log.warn("JSON timestamp must be an ISO-8601 string; using partition time");
                    return partitionTime;
                }
                return Instant.parse(tsNode.asText()).toEpochMilli();
            }
            return partitionTime;
        } catch (Exception e) {
            log.warn("Timestamp extraction failed; using partition time: {}", e.toString());
            return partitionTime;
        }
    }
}

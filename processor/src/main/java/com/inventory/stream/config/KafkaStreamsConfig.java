package com.inventory.stream.config;

import com.inventory.stream.JsonTimestampExtractor;
import com.inventory.stream.model.*;
import com.inventory.stream.processor.CoreLogicProcessor;
import com.inventory.stream.processor.ResequencingProcessor;
import com.inventory.stream.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Profile("processor")
public class KafkaStreamsConfig {

        public static final String INPUT_TOPIC = "events-input";
        public static final String OUTPUT_TOPIC = "running-totals-updates";
        public static final String AUDIT_TOPIC = "audit-log";
        public static final String STATE_STORE_NAME = "running-totals-store";

        @org.springframework.beans.factory.annotation.Value("${app.processor.grace-period-ms:5000}")
        private long gracePeriodMs;

        @Bean
        public KStream<String, Event> kStream(StreamsBuilder builder) {
                // 1. Serdes
                JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class);
                JsonSerde<ProcessingState> stateSerde = new JsonSerde<>(ProcessingState.class);
                JsonSerde<TotalUpdate> totalSerde = new JsonSerde<>(TotalUpdate.class);
                JsonSerde<AuditLog> auditSerde = new JsonSerde<>(AuditLog.class);

                // 2. Define State Store
                StoreBuilder<KeyValueStore<String, ProcessingState>> storeBuilder = Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                                Serdes.String(),
                                stateSerde);
                builder.addStateStore(storeBuilder);

                // 3. Source
                KStream<String, Event> sourceStream = builder.stream(INPUT_TOPIC,
                                Consumed.with(Serdes.String(), eventSerde)
                                                .withTimestampExtractor(new JsonTimestampExtractor()));

                // 4. Fan-Out (FlatMap)
                KStream<String, Event> fannedOut = sourceStream.flatMap((key, event) -> {
                        List<KeyValue<String, Event>> result = new ArrayList<>();

                        // Base Dimension: ID
                        if (event.getId() != null) {
                                result.add(KeyValue.pair("ID:" + event.getId(), event));

                                // Generic Dimensions (Max 3)
                                if (event.getMetadata() != null) {
                                        event.getMetadata().stream()
                                                        .limit(3)
                                                        .forEach(dim -> {
                                                                result.add(KeyValue.pair(
                                                                                "ID:" + event.getId() + "#" + dim,
                                                                                event));
                                                        });
                                }
                        }
                        return result;
                });

                // 5. Repartition to ensure co-location of keys
                KStream<String, Event> repartitioned = fannedOut.repartition(
                                Repartitioned.with(Serdes.String(), eventSerde).withName("fanout-repartition"));

                // 6. Process: Resequencing -> Core Logic
                KStream<String, ProcessingResult> results = repartitioned
                                .process(() -> new ResequencingProcessor(gracePeriodMs))
                                .process(() -> new CoreLogicProcessor(STATE_STORE_NAME), STATE_STORE_NAME);

                // 7. Split Output
                results.filter((k, v) -> v.getUpdate() != null)
                                .mapValues(ProcessingResult::getUpdate)
                                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), totalSerde));

                results.filter((k, v) -> v.getAudit() != null)
                                .mapValues(ProcessingResult::getAudit)
                                .to(AUDIT_TOPIC, Produced.with(Serdes.String(), auditSerde));

                return sourceStream;
        }
}

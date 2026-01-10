package com.inventory.stream.processor;

import com.inventory.stream.model.Event;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;

public class ResequencingProcessor implements Processor<String, Event, String, Event> {

    private static final Logger log = LoggerFactory.getLogger(ResequencingProcessor.class);

    private final long gracePeriodMs;
    private ProcessorContext<String, Event> context;
    private PriorityQueue<Record<String, Event>> buffer;
    private long lastSystemTime;
    private long streamTime = Long.MIN_VALUE;

    public ResequencingProcessor(long gracePeriodMs) {
        this.gracePeriodMs = gracePeriodMs;
    }

    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
        this.lastSystemTime = System.currentTimeMillis();
        // Sort by timestamp asc
        this.buffer = new PriorityQueue<>(Comparator.comparingLong(Record::timestamp));

        // 1. Primary: STREAM_TIME punctuation (Standard Kafka Streams)
        context.schedule(Duration.ofMillis(100), PunctuationType.STREAM_TIME, this::punctuate);

        // 2. Hybrid: WALL_CLOCK check for idle streams
        // If we haven't seen a message for a while, assume stream time has advanced by
        // the idle duration.
        context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, this::punctuateHybrid);
        log.info("ResequencingProcessor initialized with gracePeriodMs={}", gracePeriodMs);
    }

    @Override
    public void process(Record<String, Event> record) {
        // Optimization: Immediate Forwarding if Grace Period is 0
        if (gracePeriodMs == 0) {
            context.forward(record);
            return;
        }

        lastSystemTime = System.currentTimeMillis();

        if (record.timestamp() > streamTime) {
            streamTime = record.timestamp();
        }

        // Add to buffer
        buffer.add(record);
        log.debug("Buffered event: key={}, timestamp={}", record.key(), record.timestamp());
    }

    // Standard Punctuate (driven by incoming data timestamps)
    private void punctuate(long currentStreamTime) {
        // Update local high-water mark if needed (though usually currentStreamTime >=
        // streamTime)
        if (currentStreamTime > streamTime) {
            streamTime = currentStreamTime;
        }
        drain(streamTime);
    }

    // Hybrid Punctuate (driven by system clock)
    private void punctuateHybrid(long wallClockTime) {
        long idleDuration = System.currentTimeMillis() - lastSystemTime;

        // If we are idle longer than the grace period, FORCE advance the stream time
        if (idleDuration > gracePeriodMs && !buffer.isEmpty()) {
            // Assume the stream has moved forward by the amount of time we've been waiting
            // This is "Pattern: Idle Flush"
            long inferredStreamTime = streamTime + idleDuration;
            log.info("Idle flush triggered. Idle duration: {}ms. Inferred stream time: {}", idleDuration,
                    inferredStreamTime);
            drain(inferredStreamTime);
        }
    }

    private void drain(long thresholdTime) {
        long safeThreshold = thresholdTime - gracePeriodMs;

        while (!buffer.isEmpty()) {
            Record<String, Event> oldest = buffer.peek();
            if (oldest.timestamp() <= safeThreshold) {
                Record<String, Event> record = buffer.poll();
                context.forward(record);
                log.debug("Forwarded event: key={}, timestamp={}", record.key(), record.timestamp());
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        // flush on close? Or drop?
        // For now, let's flush everything remaining to avoid data loss on clean
        // shutdown
        int remaining = buffer.size();
        while (!buffer.isEmpty()) {
            context.forward(buffer.poll());
        }
        log.info("ResequencingProcessor closed. Flushed {} remaining events.", remaining);
    }
}

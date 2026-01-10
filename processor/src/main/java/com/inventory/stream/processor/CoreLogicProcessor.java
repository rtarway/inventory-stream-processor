package com.inventory.stream.processor;

import com.inventory.stream.model.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;

public class CoreLogicProcessor implements Processor<String, Event, String, ProcessingResult> {

    private static final Logger log = LoggerFactory.getLogger(CoreLogicProcessor.class);

    private final String stateStoreName;
    private ProcessorContext<String, ProcessingResult> context;
    private KeyValueStore<String, ProcessingState> stateStore;

    public CoreLogicProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult> context) {
        this.context = context;
        this.stateStore = context.getStateStore(stateStoreName);
        log.info("CoreLogicProcessor initialized using state store: {}", stateStoreName);
    }

    @Override
    public void process(Record<String, Event> record) {
        String key = record.key();
        Event event = record.value();
        ProcessingState state = stateStore.get(key);

        if (state == null) {
            state = ProcessingState.initial();
        }

        Instant eventTime = event.getTimestamp();

        // 1. Check Halted State
        if (state.isHalted()) {
            // Only accept RESET. If it's a late RESET, we now handle it instead of
            // dropping/halting further.
            if ("RESET".equalsIgnoreCase(event.getType())) {
                state.setHalted(false);
                log.info("Processing RESUMED for key={} due to RESET at {}", key, eventTime);
                // Proceed to logic
            } else {
                // Drop/Ignore other events while halted
                log.debug("Dropped event for HALTED key={}. Event type={}, timestamp={}", key, event.getType(),
                        eventTime);
                return;
            }
        }

        boolean isLateReset = false;
        String notes = null;

        // 2. Strict Sequencing Checks (Relaxed for RESET)
        if ("RESET".equalsIgnoreCase(event.getType())) {
            // If Reset is older than max processed, IT IS NO LONGER A CRITICAL MISS.
            // We apply retrospective correction.
            if (eventTime.isBefore(state.getMaxProcessedTimestamp())) {
                isLateReset = true;
                log.info("LATE RESET detected for key={}. EventTime={}, MaxProcessed={}. Applying correction.",
                        key, eventTime, state.getMaxProcessedTimestamp());
            }
        } else {
            // Normal Event
            // If older than last reset, it's stale -> DROP
            if (eventTime.isBefore(state.getLastResetTimestamp())) {
                log.warn("Skipped STALE event for key={}. EventTime={}, LastReset={}", key, eventTime,
                        state.getLastResetTimestamp());
                AuditLog audit = AuditLog.builder()
                        .eventId(event.getId())
                        .entityId(key)
                        .operation("SKIPPED_STALE")
                        .timestamp(eventTime)
                        .processedAt(Instant.now())
                        .notes("Event older than last reset")
                        .build();

                context.forward(record.withValue(new ProcessingResult(null, audit)));
                return;
            }
        }

        // 3. Apply Logic
        BigDecimal previousTotal = state.getRunningTotal();
        BigDecimal newTotal = previousTotal;
        String operation = event.getType();

        if ("RESET".equalsIgnoreCase(event.getType())) {
            if (isLateReset) {
                // COMPLEX LOGIC: Retrospective Correction
                // 1. Find Total at T_reset
                BigDecimal snapshotTotal = state.getTotalAtOrBefore(eventTime);
                // 2. Diff = Current - Snapshot
                BigDecimal diff = previousTotal.subtract(snapshotTotal);
                // 3. New = ResetVal + Diff
                newTotal = event.getValue().add(diff);

                operation = "LATE_RESET";
                notes = String.format("Retrospective correction. Snapshot@%s used val=%s. Diff=%s applied.", eventTime,
                        snapshotTotal, diff);

                // We do NOT update LastResetTimestamp to the old time, because we have
                // processed newer data.
                // Or maybe we should? Strict logic says last valid reset was at T_reset.
                // But strict logic implies everything before T_reset is invalid.
                // Since T_reset is in the past, setting it might invalidate valid interleaved
                // events if we aren't careful?
                // Actually, if we set lastResetTimestamp to T_reset, then any future late
                // events *before* T_reset will be correctly dropped.
                // So yes, we should update it.
                if (eventTime.isAfter(state.getLastResetTimestamp())) {
                    state.setLastResetTimestamp(eventTime);
                }
            } else {
                // Normal In-Order Reset
                newTotal = event.getValue();
                state.setLastResetTimestamp(eventTime);
                operation = "RESET";
            }
        } else {
            newTotal = newTotal.add(event.getValue());
            operation = "ADD";
        }

        // Update Max Processed
        if (eventTime.isAfter(state.getMaxProcessedTimestamp())) {
            state.setMaxProcessedTimestamp(eventTime);
        }

        state.setRunningTotal(newTotal);

        // 4. Update History
        state.addHistory(eventTime, newTotal);
        // Prune older than 1 hour relative to DATA time
        if (state.getMaxProcessedTimestamp() != null && state.getMaxProcessedTimestamp() != Instant.MIN) {
            state.pruneHistory(state.getMaxProcessedTimestamp().minus(java.time.Duration.ofHours(1)));
        }

        stateStore.put(key, state);

        // 5. Emit Results
        AuditLog audit = AuditLog.builder()
                .eventId(event.getId())
                .entityId(key)
                .previousTotal(previousTotal)
                .newTotal(newTotal)
                .changeAmount(event.getValue())
                .operation(operation)
                .timestamp(eventTime)
                .processedAt(Instant.now())
                .notes(notes)
                .build();

        TotalUpdate update = TotalUpdate.builder()
                .key(key)
                .total(newTotal)
                .status("OK")
                .build();

        context.forward(record.withValue(new ProcessingResult(update, audit)));
    }
}

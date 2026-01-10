package com.inventory.stream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingState {
    private BigDecimal runningTotal;
    private Instant lastResetTimestamp;
    private Instant maxProcessedTimestamp;
    private boolean isHalted; // Safety breaker for missed resets
    private java.util.List<HistoryEntry> history;

    public static ProcessingState initial() {
        return ProcessingState.builder()
                .runningTotal(BigDecimal.ZERO)
                .lastResetTimestamp(Instant.MIN)
                .maxProcessedTimestamp(Instant.MIN)
                .isHalted(false)
                .history(new java.util.ArrayList<>())
                .build();
    }

    public void addHistory(Instant ts, BigDecimal total) {
        if (history == null) {
            history = new java.util.ArrayList<>();
        }
        history.add(new HistoryEntry(ts, total));
        // Sort by timestamp to ensure binary search or correct iteration
        history.sort(java.util.Comparator.comparing(HistoryEntry::getTimestamp));
    }

    public BigDecimal getTotalAtOrBefore(Instant ts) {
        if (history == null || history.isEmpty())
            return BigDecimal.ZERO;

        // Find the latest entry <= ts
        BigDecimal bestMatch = BigDecimal.ZERO;
        for (HistoryEntry entry : history) {
            if (!entry.getTimestamp().isAfter(ts)) {
                bestMatch = entry.getTotal();
            } else {
                // Since sorted, once we pass ts, we stop (if we want strict performance,
                // otherwise simple linear scan is fine for small N)
                break;
            }
        }
        return bestMatch;
    }

    public void pruneHistory(Instant cutoff) {
        if (history != null) {
            history.removeIf(e -> e.getTimestamp().isBefore(cutoff));
        }
    }
}

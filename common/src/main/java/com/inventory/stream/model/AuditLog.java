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
public class AuditLog {
    private String eventId; // ID of the triggering event (if available, or generated)
    private String entityId; // The ID of the aggregate (e.g. "id:1" or "id:1#group:2")
    private BigDecimal previousTotal;
    private BigDecimal newTotal;
    private BigDecimal changeAmount;
    private String operation; // ADD, RESET, HALTED, SKIPPED
    private Instant timestamp; // Event timestamp
    private Instant processedAt; // Wall clock time
    private String notes; // e.g. "Late Correction"
}

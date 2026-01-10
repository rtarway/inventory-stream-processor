package com.inventory.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event {
    private String id;
    private BigDecimal value;
    private Instant timestamp;
    private String type; // "NORMAL" or "RESET"
    private java.util.List<String> metadata; // Changed to generic list of identifiers
}

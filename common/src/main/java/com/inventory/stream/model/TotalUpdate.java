package com.inventory.stream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TotalUpdate {
    private String key; // The composite key
    private BigDecimal total;
    private String status; // "OK" or "HALTED"
}

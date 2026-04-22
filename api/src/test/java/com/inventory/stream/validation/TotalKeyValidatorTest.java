package com.inventory.stream.validation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TotalKeyValidatorTest {

    @Test
    void acceptsProducerStyleKeys() {
        assertTrue(TotalKeyValidator.isValid("ID:abc-123"));
        assertTrue(TotalKeyValidator.isValid("ID:abc-123#dim-1"));
    }

    @Test
    void rejectsInjectionLikeKeys() {
        assertFalse(TotalKeyValidator.isValid("ID:foo bar"));
        assertFalse(TotalKeyValidator.isValid("ID:foo\nbar"));
        assertFalse(TotalKeyValidator.isValid(""));
        assertFalse(TotalKeyValidator.isValid(null));
    }
}

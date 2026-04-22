package com.inventory.stream.validation;

import java.util.regex.Pattern;

/**
 * Validates Redis hash keys used by the read API path segment (injection / abuse resistance).
 */
public final class TotalKeyValidator {

    private static final int MAX_LENGTH = 512;
    /** Keys are produced as {@code ID:...} with optional {@code #dimension} suffix. */
    private static final Pattern ALLOWED = Pattern.compile("^[A-Za-z0-9_:#.\\-]+$");

    private TotalKeyValidator() {
    }

    public static boolean isValid(String key) {
        return key != null && !key.isEmpty() && key.length() <= MAX_LENGTH && ALLOWED.matcher(key).matches();
    }
}

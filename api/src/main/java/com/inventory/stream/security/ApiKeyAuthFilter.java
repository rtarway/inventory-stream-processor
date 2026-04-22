package com.inventory.stream.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Optional API-key gate for production-style deployments.
 * Enable with {@code app.api.require-api-key=true} and set {@code app.api.api-key}.
 */
@Component
@ConditionalOnProperty(name = "app.api.require-api-key", havingValue = "true")
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    @Value("${app.api.api-key:}")
    private String apiKey;

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain) throws ServletException, IOException {
        if (apiKey == null || apiKey.isBlank()) {
            response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "API key not configured");
            return;
        }
        String provided = request.getHeader("X-API-Key");
        if (!apiKey.equals(provided)) {
            response.sendError(HttpStatus.UNAUTHORIZED.value(), "Unauthorized");
            return;
        }
        filterChain.doFilter(request, response);
    }
}

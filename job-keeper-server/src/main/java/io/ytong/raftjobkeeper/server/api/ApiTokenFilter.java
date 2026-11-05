package io.ytong.raftjobkeeper.server.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Component
public class ApiTokenFilter extends OncePerRequestFilter {
    private final String token;

    public ApiTokenFilter(@Value("${API_TOKEN:}") String token) {
        this.token = token;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!requiresToken(request)) {
            chain.doFilter(request, response);
            return;
        }
        if (token.isBlank()) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "API_TOKEN must be configured before write operations are enabled");
            return;
        }
        String authorization = request.getHeader("Authorization");
        String supplied = authorization != null && authorization.startsWith("Bearer ")
                ? authorization.substring("Bearer ".length()) : "";
        if (!MessageDigest.isEqual(token.getBytes(StandardCharsets.UTF_8),
                supplied.getBytes(StandardCharsets.UTF_8))) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "A valid bearer token is required");
            return;
        }
        chain.doFilter(request, response);
    }

    private static boolean requiresToken(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path.startsWith("/admin/")
                || (path.startsWith("/jobs") && !HttpMethod.GET.matches(request.getMethod())
                    && !HttpMethod.HEAD.matches(request.getMethod())
                    && !HttpMethod.OPTIONS.matches(request.getMethod()));
    }
}

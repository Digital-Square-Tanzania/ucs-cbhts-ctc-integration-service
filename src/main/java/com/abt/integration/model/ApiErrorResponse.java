package com.abt.integration.model;

import java.util.List;

public class ApiErrorResponse {
    private final String message;
    private final List<String> details;

    public ApiErrorResponse(String message, List<String> details) {
        this.message = message;
        this.details = details;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getDetails() {
        return details;
    }
}

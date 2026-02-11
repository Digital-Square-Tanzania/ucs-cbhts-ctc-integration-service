package com.abt.integration.exception;

import java.util.Collections;
import java.util.List;

public class ValidationException extends RuntimeException {
    private final List<String> errors;

    public ValidationException(List<String> errors) {
        super("Request validation failed");
        this.errors = errors == null ? List.of() : List.copyOf(errors);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }
}

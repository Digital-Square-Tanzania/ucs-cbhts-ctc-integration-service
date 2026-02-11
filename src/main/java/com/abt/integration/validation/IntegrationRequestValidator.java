package com.abt.integration.validation;

import com.abt.integration.model.IntegrationRequest;

import java.util.ArrayList;
import java.util.List;

public class IntegrationRequestValidator {

    public List<String> validate(IntegrationRequest request) {
        List<String> errors = new ArrayList<>();

        if (request == null) {
            errors.add("Request body is required");
            return errors;
        }

        if (isBlank(request.getHfrCode())) {
            errors.add("hfrCode is required");
        }

        if (request.getStartDate() == null) {
            errors.add("startDate is required");
        }

        if (request.getEndDate() == null) {
            errors.add("endDate is required");
        }

        if (request.getStartDate() != null && request.getEndDate() != null && request.getStartDate() > request.getEndDate()) {
            errors.add("startDate must be less than or equal to endDate");
        }

        if (request.getPageIndex() == null) {
            errors.add("pageIndex is required");
        } else if (request.getPageIndex() < 1) {
            errors.add("pageIndex must be greater than or equal to 1");
        }

        if (request.getPageSize() == null) {
            errors.add("pageSize is required");
        } else if (request.getPageSize() < 1) {
            errors.add("pageSize must be greater than or equal to 1");
        }

        return errors;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

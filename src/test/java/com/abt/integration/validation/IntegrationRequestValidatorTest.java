package com.abt.integration.validation;

import com.abt.integration.model.IntegrationRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrationRequestValidatorTest {

    private final IntegrationRequestValidator validator = new IntegrationRequestValidator();

    @Test
    void validate_shouldReturnErrorsForInvalidRequest() {
        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode(" ");
        request.setStartDate(200L);
        request.setEndDate(100L);
        request.setPageIndex(0);
        request.setPageSize(0);

        List<String> errors = validator.validate(request);

        assertTrue(errors.contains("hfrCode is required"));
        assertTrue(errors.contains("startDate must be less than or equal to endDate"));
        assertTrue(errors.contains("pageIndex must be greater than or equal to 1"));
        assertTrue(errors.contains("pageSize must be greater than or equal to 1"));
    }

    @Test
    void validate_shouldPassForValidRequest() {
        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode("124899-6");
        request.setStartDate(1768262400L);
        request.setEndDate(1768262800L);
        request.setPageIndex(1);
        request.setPageSize(100);

        List<String> errors = validator.validate(request);

        assertEquals(0, errors.size());
    }
}

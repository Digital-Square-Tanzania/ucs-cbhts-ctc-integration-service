package com.abt.integration.validation;

import com.abt.integration.model.VerificationResultsRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerificationResultsRequestValidatorTest {

    private final VerificationResultsRequestValidator validator = new VerificationResultsRequestValidator();

    @Test
    void validate_shouldReturnErrorsForInvalidRequest() {
        VerificationResultsRequest request = new VerificationResultsRequest();
        request.setHfrCode(" ");

        VerificationResultsRequest.VerificationResultItem item = new VerificationResultsRequest.VerificationResultItem();
        item.setClientCode(" ");
        item.setVerificationDate("2026/01/01");
        item.setHivFinalVerificationResultCode("unknown");
        item.setVisitId(" ");
        request.setData(List.of(item));

        List<String> errors = validator.validate(request);

        assertTrue(errors.contains("hfrCode is required"));
        assertTrue(errors.contains("data[0].clientCode is required"));
        assertTrue(errors.contains("data[0].verificationDate must use yyyy-MM-dd format"));
        assertTrue(errors.contains("data[0].hivFinalVerificationResultCode must be one of: POSITIVE, NEGATIVE, INCONCLUSIVE"));
        assertTrue(errors.contains("data[0].visitId is required"));
    }

    @Test
    void validate_shouldPassForValidRequest() {
        VerificationResultsRequest request = new VerificationResultsRequest();
        request.setHfrCode("12123-1");

        VerificationResultsRequest.VerificationResultItem item = new VerificationResultsRequest.VerificationResultItem();
        item.setClientCode("CLT123456");
        item.setVerificationDate("2026-01-01");
        item.setHivFinalVerificationResultCode("POSITIVE");
        item.setCtcId("12-11-2132-133214");
        item.setVisitId("B0452823-F078-4CAC-8746-4A11733E942A");
        request.setData(List.of(item));

        List<String> errors = validator.validate(request);
        assertEquals(0, errors.size());
    }
}

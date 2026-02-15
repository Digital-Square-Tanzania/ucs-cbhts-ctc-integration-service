package com.abt.integration.validation;

import com.abt.integration.model.VerificationResultsRequest;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class VerificationResultsRequestValidator {
    private static final Set<String> ALLOWED_RESULTS = Set.of("POSITIVE", "NEGATIVE", "INCONCLUSIVE");

    public List<String> validate(VerificationResultsRequest request) {
        List<String> errors = new ArrayList<>();

        if (request == null) {
            errors.add("Request body is required");
            return errors;
        }

        if (isBlank(request.getHfrCode())) {
            errors.add("hfrCode is required");
        }

        if (request.getData() == null || request.getData().isEmpty()) {
            errors.add("data is required");
            return errors;
        }

        for (int index = 0; index < request.getData().size(); index++) {
            VerificationResultsRequest.VerificationResultItem item = request.getData().get(index);
            String fieldPrefix = "data[" + index + "]";

            if (item == null) {
                errors.add(fieldPrefix + " is required");
                continue;
            }

            if (isBlank(item.getClientCode())) {
                errors.add(fieldPrefix + ".clientCode is required");
            }

            if (isBlank(item.getVerificationDate())) {
                errors.add(fieldPrefix + ".verificationDate is required");
            } else if (!isIsoLocalDate(item.getVerificationDate())) {
                errors.add(fieldPrefix + ".verificationDate must use yyyy-MM-dd format");
            }

            if (isBlank(item.getHivFinalVerificationResultCode())) {
                errors.add(fieldPrefix + ".hivFinalVerificationResultCode is required");
            } else if (!ALLOWED_RESULTS.contains(item.getHivFinalVerificationResultCode().trim().toUpperCase(Locale.ROOT))) {
                errors.add(fieldPrefix + ".hivFinalVerificationResultCode must be one of: POSITIVE, NEGATIVE, INCONCLUSIVE");
            }

            if (isBlank(item.getVisitId())) {
                errors.add(fieldPrefix + ".visitId is required");
            }
        }

        return errors;
    }

    private boolean isIsoLocalDate(String value) {
        try {
            LocalDate.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

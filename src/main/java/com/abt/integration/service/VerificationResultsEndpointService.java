package com.abt.integration.service;

import com.abt.integration.model.VerificationResultsRequest;

import java.util.Map;

public interface VerificationResultsEndpointService {
    Map<String, Object> process(VerificationResultsRequest request);
}

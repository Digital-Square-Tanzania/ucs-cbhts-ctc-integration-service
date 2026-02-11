package com.abt.integration.service;

import com.abt.integration.model.IntegrationRequest;

import java.util.Map;

public interface IntegrationEndpointService {
    Map<String, Object> fetch(IntegrationRequest request);
}

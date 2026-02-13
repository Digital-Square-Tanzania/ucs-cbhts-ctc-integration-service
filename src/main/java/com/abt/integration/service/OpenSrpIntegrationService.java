package com.abt.integration.service;

import com.abt.integration.config.PostgresConnectionFactory;
import com.abt.integration.db.OpenSrpIntegrationRepository;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.mapping.IntegrationDataMapper;
import com.abt.integration.model.IntegrationRequest;
import com.abt.integration.validation.IntegrationRequestValidator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OpenSrpIntegrationService implements IntegrationEndpointService {

    private final PostgresConnectionFactory connectionFactory;
    private final OpenSrpIntegrationRepository repository;
    private final IntegrationDataMapper dataMapper;
    private final IntegrationRequestValidator validator;

    public OpenSrpIntegrationService() {
        this.connectionFactory = new PostgresConnectionFactory();
        this.repository = new OpenSrpIntegrationRepository(connectionFactory.schema());
        this.dataMapper = new IntegrationDataMapper();
        this.validator = new IntegrationRequestValidator();
    }

    public OpenSrpIntegrationService(PostgresConnectionFactory connectionFactory,
                                     OpenSrpIntegrationRepository repository,
                                     IntegrationDataMapper dataMapper,
                                     IntegrationRequestValidator validator) {
        this.connectionFactory = connectionFactory;
        this.repository = repository;
        this.dataMapper = dataMapper;
        this.validator = validator;
    }

    @Override
    public Map<String, Object> fetch(IntegrationRequest request) {
        List<String> validationErrors = validator.validate(request);
        if (!validationErrors.isEmpty()) {
            throw new ValidationException(validationErrors);
        }

        try (Connection connection = connectionFactory.openConnection()) {
            long totalRecords = repository.countServices(connection, request);
            List<OpenSrpIntegrationRepository.ServiceRow> serviceRows = totalRecords == 0
                    ? List.of()
                    : repository.findServices(connection, request);

            Map<String, List<OpenSrpIntegrationRepository.TestRow>> testsByKey = serviceRows.isEmpty()
                    ? Map.of()
                    : repository.findTestsForServices(connection, serviceRows, request.getStartDate(), request.getEndDate());

            Map<String, List<OpenSrpIntegrationRepository.HivstSelfTestRow>> hivstRowsByBaseEntity = serviceRows.isEmpty()
                    ? Map.of()
                    : repository.findHivstSelfTestsByBaseEntity(connection, serviceRows);

            List<Map<String, Object>> data = new ArrayList<>();
            for (OpenSrpIntegrationRepository.ServiceRow serviceRow : serviceRows) {
                String key = OpenSrpIntegrationRepository.serviceKey(serviceRow);
                List<OpenSrpIntegrationRepository.TestRow> tests = testsByKey.getOrDefault(key, List.of());
                List<OpenSrpIntegrationRepository.HivstSelfTestRow> hivstRows = hivstRowsByBaseEntity.getOrDefault(serviceRow.baseEntityId(), List.of());
                data.add(dataMapper.mapServiceRow(serviceRow, tests, hivstRows));
            }

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("pageNumber", request.getPageIndex());
            response.put("pageSize", request.getPageSize());
            response.put("totalRecords", totalRecords);
            response.put("data", data);
            return response;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query OpenSRP database", e);
        }
    }
}

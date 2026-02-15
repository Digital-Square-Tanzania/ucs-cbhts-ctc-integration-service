package com.abt.integration.service;

import com.abt.integration.config.PostgresConnectionFactory;
import com.abt.integration.db.OpenSrpIntegrationRepository;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.mapping.IntegrationDataMapper;
import com.abt.integration.model.IntegrationRequest;
import com.abt.integration.validation.IntegrationRequestValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSrpIntegrationServiceTest {

    @Mock
    private PostgresConnectionFactory connectionFactory;

    @Mock
    private OpenSrpIntegrationRepository repository;

    @Mock
    private Connection connection;

    @SuppressWarnings("unchecked")
    @Test
    void fetch_shouldReturnPaginatedResponse() throws SQLException {
        OpenSrpIntegrationService service = new OpenSrpIntegrationService(
                connectionFactory,
                repository,
                new IntegrationDataMapper(),
                new IntegrationRequestValidator()
        );

        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode("124899-6");
        request.setStartDate(1768262400L);
        request.setEndDate(1768262800L);
        request.setPageIndex(2);
        request.setPageSize(10);

        OpenSrpIntegrationRepository.ServiceRow serviceRow = new OpenSrpIntegrationRepository.ServiceRow(
                "event-1",
                "base-1",
                "visit-group-1",
                "20-12-2025",
                "20-12-2025",
                1768262800000L,
                "provider-1",
                "cbhts",
                "new_client",
                "no",
                "self",
                "sto",
                "reactive",
                "normal_client",
                "cbhts",
                "individual",
                "tb_suspect",
                "yes",
                "relative",
                "yes",
                10,
                5,
                "prep_services",
                "positive",
                "CLT123456",
                "Asha",
                "Salum",
                "Hassan",
                "0712345678",
                "1990123456789012",
                null,
                null,
                null,
                "Female",
                "1995-06-20T03:00:00.000+03:00",
                "Single",
                "yes",
                "13211-1",
                "TZ.NT.MY",
                "TZ.NT.MY.ML",
                "TZ.NT.MY.ML.4",
                "TZ.NT.MY.ML.4.8",
                "TZ.NT.MY.ML.4.8.1",
                "TZ.NT.MY.ML.4.8.1.3",
                "John Doe"
        );

        OpenSrpIntegrationRepository.TestRow testRow = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "hiv_syphilis_dual",
                "RB001",
                "2026-08-31",
                "non_reactive",
                "positive",
                "first",
                1768262800000L
        );

        when(connectionFactory.openConnection()).thenReturn(connection);
        when(repository.countServices(connection, request)).thenReturn(11L);
        when(repository.findServices(connection, request)).thenReturn(List.of(serviceRow));
        when(repository.findTestsForServices(connection, List.of(serviceRow), 1768262400L, 1768262800L))
                .thenReturn(Map.of(OpenSrpIntegrationRepository.serviceKey(serviceRow), List.of(testRow)));
        when(repository.findHivstTestByBaseEntity(connection, List.of(serviceRow)))
                .thenReturn(Map.of());
        when(repository.findEnrollmentEligibilityByBaseEntity(connection, List.of(serviceRow)))
                .thenReturn(Map.of());

        Map<String, Object> response = service.fetch(request);

        assertEquals(2, response.get("pageNumber"));
        assertEquals(10, response.get("pageSize"));
        assertEquals(11L, response.get("totalRecords"));

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.get("data");
        assertEquals(1, data.size());
        assertEquals("CBHTS", data.get(0).get("htcApproach"));
        assertEquals("POSITIVE", data.get(0).get("hivResultCode"));
        Map<String, Object> clientClassification = (Map<String, Object>) data.get(0).get("clientClassification");
        assertEquals(true, clientClassification.get("eligibleForTesting"));

        verify(repository).countServices(connection, request);
        verify(repository).findServices(connection, request);
        verify(repository).findTestsForServices(connection, List.of(serviceRow), 1768262400L, 1768262800L);
        verify(repository).findHivstTestByBaseEntity(connection, List.of(serviceRow));
        verify(repository).findEnrollmentEligibilityByBaseEntity(connection, List.of(serviceRow));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetch_shouldReturnEmptySelfTestingWhenHivstBaseEntityDoesNotMatchServiceClient() throws SQLException {
        OpenSrpIntegrationService service = new OpenSrpIntegrationService(
                connectionFactory,
                repository,
                new IntegrationDataMapper(),
                new IntegrationRequestValidator()
        );

        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode("124899-6");
        request.setStartDate(1768262400L);
        request.setEndDate(1768262800L);
        request.setPageIndex(1);
        request.setPageSize(10);

        OpenSrpIntegrationRepository.ServiceRow serviceRow = new OpenSrpIntegrationRepository.ServiceRow(
                "event-1",
                "base-1",
                "visit-group-1",
                "2026-01-14",
                "2026-01-14",
                1768262800000L,
                "provider-1",
                "cbhts",
                "new_client",
                "no",
                "self",
                "sto",
                "reactive",
                "normal_client",
                "cbhts",
                "individual",
                "tb_suspect",
                "yes",
                "relative",
                "yes",
                10,
                5,
                "prep_services",
                "CLT123456",
                "Asha",
                "Salum",
                "Hassan",
                "0712345678",
                "1990123456789012",
                null,
                null,
                null,
                "Female",
                "1995-06-20T03:00:00.000+03:00",
                "Single",
                "yes",
                "13211-1",
                "TZ.NT.MY",
                "TZ.NT.MY.ML",
                "TZ.NT.MY.ML.4",
                "TZ.NT.MY.ML.4.8",
                "TZ.NT.MY.ML.4.8.1",
                "TZ.NT.MY.ML.4.8.1.3",
                "John Doe"
        );

        OpenSrpIntegrationRepository.TestRow reagentTestRow = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "hiv_syphilis_dual",
                "RB001",
                "2026-08-31",
                "non_reactive",
                "positive",
                "First HIV Test",
                1768262800000L
        );

        OpenSrpIntegrationRepository.HivstSelfTestRow nonMatchingHivstRow = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-1",
                "2026-01-14",
                "base-2",
                "client",
                "KIT-001",
                "reactive",
                "2026-01-14",
                null,
                "issue-event-1",
                "2026-01-14",
                "BATCH-CLIENT",
                "2027-01-11"
        );

        when(connectionFactory.openConnection()).thenReturn(connection);
        when(repository.countServices(connection, request)).thenReturn(1L);
        when(repository.findServices(connection, request)).thenReturn(List.of(serviceRow));
        when(repository.findTestsForServices(connection, List.of(serviceRow), 1768262400L, 1768262800L))
                .thenReturn(Map.of(OpenSrpIntegrationRepository.serviceKey(serviceRow), List.of(reagentTestRow)));
        when(repository.findHivstTestByBaseEntity(connection, List.of(serviceRow)))
                .thenReturn(Map.of("base-2", List.of(nonMatchingHivstRow)));
        when(repository.findEnrollmentEligibilityByBaseEntity(connection, List.of(serviceRow)))
                .thenReturn(Map.of("base-1", false));

        Map<String, Object> response = service.fetch(request);

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.get("data");
        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) data.get(0).get("selfTesting");
        assertEquals(0, selfTesting.size());
        Map<String, Object> clientClassification = (Map<String, Object>) data.get(0).get("clientClassification");
        assertEquals(false, clientClassification.get("eligibleForTesting"));
    }

    @Test
    void fetch_shouldFailForInvalidRequest() {
        OpenSrpIntegrationService service = new OpenSrpIntegrationService(
                connectionFactory,
                repository,
                new IntegrationDataMapper(),
                new IntegrationRequestValidator()
        );

        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode(null);

        assertThrows(ValidationException.class, () -> service.fetch(request));
    }
}

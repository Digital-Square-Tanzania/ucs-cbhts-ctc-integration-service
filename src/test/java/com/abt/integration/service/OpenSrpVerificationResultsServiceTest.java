package com.abt.integration.service;

import com.abt.domain.Event;
import com.abt.domain.EventRequest;
import com.abt.domain.Obs;
import com.abt.integration.config.PostgresConnectionFactory;
import com.abt.integration.db.OpenSrpIntegrationRepository;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.model.VerificationResultsRequest;
import com.abt.integration.validation.VerificationResultsRequestValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSrpVerificationResultsServiceTest {

    @Mock
    private PostgresConnectionFactory connectionFactory;

    @Mock
    private OpenSrpIntegrationRepository repository;

    @Mock
    private OpenSrpEventSender eventSender;

    @Mock
    private Connection connection;

    @SuppressWarnings("unchecked")
    @Test
    void process_shouldReturnPartialSuccessAndSendVerificationEvent() throws SQLException {
        OpenSrpVerificationResultsService service = new OpenSrpVerificationResultsService(
                connectionFactory,
                repository,
                new VerificationResultsRequestValidator(),
                eventSender,
                "http://opensrp/events",
                "user",
                "pass"
        );

        VerificationResultsRequest request = buildRequest();

        OpenSrpIntegrationRepository.VerificationServiceMetadataRow metadataRow =
                new OpenSrpIntegrationRepository.VerificationServiceMetadataRow(
                        "base-1",
                        "provider-1",
                        "Team A",
                        "team-1",
                        "loc-1",
                        "ec_client"
                );

        when(connectionFactory.openConnection()).thenReturn(connection);
        when(repository.findLatestServiceMetadataByClientCode(connection, "12123-1", "CLT123456"))
                .thenReturn(java.util.Optional.of(metadataRow));
        when(repository.findLatestServiceMetadataByClientCode(connection, "12123-1", "CLT000000"))
                .thenReturn(java.util.Optional.empty());
        when(eventSender.send(any(EventRequest.class), eq("http://opensrp/events"), eq("user"), eq("pass")))
                .thenReturn("sending successful");

        Map<String, Object> response = service.process(request);

        assertEquals(2, response.get("processedCount"));
        assertEquals(1, response.get("successCount"));
        assertEquals(1, response.get("failureCount"));

        List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
        assertEquals(1, errors.size());
        assertEquals("CLT000000", errors.get(0).get("clientCode"));

        ArgumentCaptor<EventRequest> requestCaptor = ArgumentCaptor.forClass(EventRequest.class);
        verify(eventSender).send(requestCaptor.capture(), eq("http://opensrp/events"), eq("user"), eq("pass"));

        Event sentEvent = requestCaptor.getValue().getEvents().get(0);
        assertEquals("HIV Verification Test Results", sentEvent.getEventType());
        assertEquals("base-1", sentEvent.getBaseEntityId());
        assertEquals("provider-1", sentEvent.getProviderId());
        assertEquals("team-1", sentEvent.getTeamId());
        assertEquals("Team A", sentEvent.getTeam());
        assertTrue(hasObs(sentEvent, "hiv_final_verification_result_code", "positive"));
        assertTrue(hasObs(sentEvent, "verification_date", "2026-01-01"));
        assertTrue(hasObs(sentEvent, "visit_id", "B0452823-F078-4CAC-8746-4A11733E942A"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void process_shouldRecordFailureWhenSendFails() throws SQLException {
        OpenSrpVerificationResultsService service = new OpenSrpVerificationResultsService(
                connectionFactory,
                repository,
                new VerificationResultsRequestValidator(),
                eventSender,
                "http://opensrp/events",
                "user",
                "pass"
        );

        VerificationResultsRequest request = new VerificationResultsRequest();
        request.setHfrCode("12123-1");
        request.setData(List.of(buildItem("CLT123456", "2026-01-01", "POSITIVE", "CTC-1", "VISIT-1")));

        OpenSrpIntegrationRepository.VerificationServiceMetadataRow metadataRow =
                new OpenSrpIntegrationRepository.VerificationServiceMetadataRow(
                        "base-1",
                        "provider-1",
                        "Team A",
                        "team-1",
                        "loc-1",
                        "ec_client"
                );

        when(connectionFactory.openConnection()).thenReturn(connection);
        when(repository.findLatestServiceMetadataByClientCode(connection, "12123-1", "CLT123456"))
                .thenReturn(java.util.Optional.of(metadataRow));
        when(eventSender.send(any(EventRequest.class), eq("http://opensrp/events"), eq("user"), eq("pass")))
                .thenReturn("Error: Sending data to UCS failed");

        Map<String, Object> response = service.process(request);

        assertEquals(1, response.get("processedCount"));
        assertEquals(0, response.get("successCount"));
        assertEquals(1, response.get("failureCount"));

        List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
        assertEquals(1, errors.size());
        assertTrue(((String) errors.get(0).get("message")).contains("Error"));
    }

    @Test
    void process_shouldFailForInvalidRequest() {
        OpenSrpVerificationResultsService service = new OpenSrpVerificationResultsService(
                connectionFactory,
                repository,
                new VerificationResultsRequestValidator(),
                eventSender,
                "http://opensrp/events",
                "user",
                "pass"
        );

        VerificationResultsRequest request = new VerificationResultsRequest();
        request.setHfrCode(null);

        assertThrows(ValidationException.class, () -> service.process(request));
    }

    private VerificationResultsRequest buildRequest() {
        VerificationResultsRequest request = new VerificationResultsRequest();
        request.setHfrCode("12123-1");
        request.setData(List.of(
                buildItem("CLT123456", "2026-01-01", "POSITIVE", "12-11-2132-133214", "B0452823-F078-4CAC-8746-4A11733E942A"),
                buildItem("CLT000000", "2026-01-02", "NEGATIVE", "12-11-2132-133215", "B0452823-F078-4CAC-8746-4A11733E942B")
        ));
        return request;
    }

    private VerificationResultsRequest.VerificationResultItem buildItem(String clientCode,
                                                                        String verificationDate,
                                                                        String resultCode,
                                                                        String ctcId,
                                                                        String visitId) {
        VerificationResultsRequest.VerificationResultItem item = new VerificationResultsRequest.VerificationResultItem();
        item.setClientCode(clientCode);
        item.setVerificationDate(verificationDate);
        item.setHivFinalVerificationResultCode(resultCode);
        item.setCtcId(ctcId);
        item.setVisitId(visitId);
        return item;
    }

    private boolean hasObs(Event event, String fieldCode, String expectedValue) {
        if (event.getObs() == null) {
            return false;
        }

        for (Obs obs : event.getObs()) {
            if (fieldCode.equals(obs.getFieldCode()) && expectedValue.equals(obs.getValue())) {
                return true;
            }
        }

        return false;
    }
}

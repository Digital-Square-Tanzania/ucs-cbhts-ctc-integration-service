package com.abt;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.model.VerificationResultsRequest;
import com.abt.integration.service.IntegrationEndpointService;
import com.abt.integration.service.VerificationResultsEndpointService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UcsCbhtsCtsIntegrationRoutesTest extends JUnitRouteTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private IntegrationEndpointService integrationEndpointService;

    private VerificationResultsEndpointService verificationResultsEndpointService;

    @BeforeEach
    void setUp() {
        systemResource().before();
        integrationEndpointService = mock(IntegrationEndpointService.class);
        verificationResultsEndpointService = mock(VerificationResultsEndpointService.class);
    }

    @AfterEach
    void tearDown() {
        systemResource().after();
    }

    @Test
    void verificationResultsRoute_shouldReturnOkWithProcessingSummary() throws Exception {
        UcsCbhtsCtsIntegrationRoutes routes = new UcsCbhtsCtsIntegrationRoutes(
                integrationEndpointService,
                verificationResultsEndpointService
        );

        when(verificationResultsEndpointService.process(any(VerificationResultsRequest.class)))
                .thenReturn(Map.of(
                        "processedCount", 1,
                        "successCount", 1,
                        "failureCount", 0,
                        "errors", List.of()
                ));

        String payload = "{\n" +
                "  \"hfrCode\": \"12123-1\",\n" +
                "  \"data\": [\n" +
                "    {\n" +
                "      \"clientCode\": \"CLT123456\",\n" +
                "      \"verificationDate\": \"2026-01-01\",\n" +
                "      \"hivFinalVerificationResultCode\": \"POSITIVE\",\n" +
                "      \"ctcId\": \"12-11-2132-133214\",\n" +
                "      \"visitId\": \"B0452823-F078-4CAC-8746-4A11733E942A\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String responseBody = testRoute(routes.integrationRoutes())
                .run(HttpRequest.POST("/integration/verification-results")
                        .withEntity(ContentTypes.APPLICATION_JSON, payload))
                .assertStatusCode(StatusCodes.OK)
                .entityString();

        JsonNode response = OBJECT_MAPPER.readTree(responseBody);
        assertEquals(1, response.get("processedCount").asInt());
        assertEquals(1, response.get("successCount").asInt());
        assertEquals(0, response.get("failureCount").asInt());
        assertTrue(response.get("errors").isArray());
    }

    @Test
    void verificationResultsRoute_shouldReturnBadRequestForValidationErrors() throws Exception {
        UcsCbhtsCtsIntegrationRoutes routes = new UcsCbhtsCtsIntegrationRoutes(
                integrationEndpointService,
                verificationResultsEndpointService
        );

        when(verificationResultsEndpointService.process(any(VerificationResultsRequest.class)))
                .thenThrow(new ValidationException(List.of("data[0].visitId is required")));

        String payload = "{\n" +
                "  \"hfrCode\": \"12123-1\",\n" +
                "  \"data\": [\n" +
                "    {\n" +
                "      \"clientCode\": \"CLT123456\",\n" +
                "      \"verificationDate\": \"2026-01-01\",\n" +
                "      \"hivFinalVerificationResultCode\": \"POSITIVE\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String responseBody = testRoute(routes.integrationRoutes())
                .run(HttpRequest.POST("/integration/verification-results")
                        .withEntity(ContentTypes.APPLICATION_JSON, payload))
                .assertStatusCode(StatusCodes.BAD_REQUEST)
                .entityString();

        JsonNode response = OBJECT_MAPPER.readTree(responseBody);
        assertEquals("Invalid request payload", response.get("message").asText());
        assertTrue(response.get("details").toString().contains("visitId"));
    }
}

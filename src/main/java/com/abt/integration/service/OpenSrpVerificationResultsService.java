package com.abt.integration.service;

import com.abt.domain.Event;
import com.abt.domain.EventRequest;
import com.abt.domain.Obs;
import com.abt.integration.config.PostgresConnectionFactory;
import com.abt.integration.db.OpenSrpIntegrationRepository;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.model.VerificationResultsRequest;
import com.abt.util.EnvConfig;
import com.abt.integration.validation.VerificationResultsRequestValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class OpenSrpVerificationResultsService implements VerificationResultsEndpointService {
    private static final Logger log = LoggerFactory.getLogger(OpenSrpVerificationResultsService.class);

    private static final String HIV_VERIFICATION_EVENT_TYPE = "HIV Verification Test Results";
    private static final String OPENSRP_EVENT_PATH = "/opensrp/rest/event/add";
    private static final String DEFAULT_ENTITY_TYPE = "ec_client";
    private static final int CLIENT_DATABASE_VERSION = 17;
    private static final int CLIENT_APPLICATION_VERSION = 2;

    private final PostgresConnectionFactory connectionFactory;
    private final OpenSrpIntegrationRepository repository;
    private final VerificationResultsRequestValidator validator;
    private final OpenSrpEventSender eventSender;
    private final String openSrpEventUrl;
    private final String openSrpUsername;
    private final String openSrpPassword;

    public OpenSrpVerificationResultsService() {
        this(defaultDependencies());
    }

    private OpenSrpVerificationResultsService(DefaultDependencies dependencies) {
        this(
                dependencies.connectionFactory(),
                dependencies.repository(),
                new VerificationResultsRequestValidator(),
                new DefaultOpenSrpEventSender(),
                resolveOpenSrpEventUrl(),
                EnvConfig.getFirstOrDefault(null, "OPENSRP_SERVER_USERNAME"),
                EnvConfig.getFirstOrDefault(null, "OPENSRP_SERVER_PASSWORD")
        );
    }

    public OpenSrpVerificationResultsService(PostgresConnectionFactory connectionFactory,
                                             OpenSrpIntegrationRepository repository,
                                             VerificationResultsRequestValidator validator,
                                             OpenSrpEventSender eventSender,
                                             String openSrpEventUrl,
                                             String openSrpUsername,
                                             String openSrpPassword) {
        this.connectionFactory = connectionFactory;
        this.repository = repository;
        this.validator = validator;
        this.eventSender = eventSender;
        this.openSrpEventUrl = openSrpEventUrl;
        this.openSrpUsername = openSrpUsername;
        this.openSrpPassword = openSrpPassword;
    }

    @Override
    public Map<String, Object> process(VerificationResultsRequest request) {
        List<String> validationErrors = validator.validate(request);
        if (!validationErrors.isEmpty()) {
            throw new ValidationException(validationErrors);
        }

        if (isBlank(openSrpEventUrl)) {
            throw new IllegalStateException("Missing OpenSRP destination URL. Set OPENSRP_SERVER_EVENT_URL or OPENSRP_SERVER_URL.");
        }

        int processedCount = request.getData().size();
        int successCount = 0;
        List<Map<String, Object>> errors = new ArrayList<>();

        try (Connection connection = connectionFactory.openConnection()) {
            for (int index = 0; index < request.getData().size(); index++) {
                VerificationResultsRequest.VerificationResultItem item = request.getData().get(index);
                try {
                    Optional<OpenSrpIntegrationRepository.VerificationServiceMetadataRow> metadataOptional =
                            repository.findLatestServiceMetadataByClientCode(connection, request.getHfrCode(), item.getClientCode());

                    if (metadataOptional.isEmpty()) {
                        errors.add(errorItem(index, item, "No cbhts_services record found for clientCode and hfrCode"));
                        continue;
                    }

                    Event event = buildVerificationEvent(request.getHfrCode(), item, metadataOptional.get());
                    String sendResult = eventSender.send(
                            new EventRequest(List.of(event)),
                            openSrpEventUrl,
                            openSrpUsername,
                            openSrpPassword
                    );

                    if (isSuccessfulSend(sendResult)) {
                        successCount++;
                    } else {
                        errors.add(errorItem(index, item, firstNonBlank(sendResult, "Failed to send event to OpenSRP")));
                    }
                } catch (Exception e) {
                    log.error("Failed to process verification result for clientCode={}", item.getClientCode(), e);
                    errors.add(errorItem(index, item, firstNonBlank(e.getMessage(), "Unexpected processing error")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query OpenSRP database", e);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processedCount", processedCount);
        response.put("successCount", successCount);
        response.put("failureCount", processedCount - successCount);
        response.put("errors", errors);
        return response;
    }

    private Event buildVerificationEvent(String hfrCode,
                                         VerificationResultsRequest.VerificationResultItem item,
                                         OpenSrpIntegrationRepository.VerificationServiceMetadataRow metadata) {
        Event event = new Event();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType(HIV_VERIFICATION_EVENT_TYPE);
        event.setEntityType(firstNonBlank(metadata.entityType(), DEFAULT_ENTITY_TYPE));
        event.setProviderId(metadata.providerId());
        event.setLocationId(metadata.locationId());
        event.setTeam(metadata.team());
        event.setTeamId(metadata.teamId());
        event.setBaseEntityId(metadata.baseEntityId());
        event.setType("Event");
        event.setFormSubmissionId(UUID.randomUUID().toString());
        event.setEventDate(new Date());
        event.setDateCreated(new Date());
        event.setClientApplicationVersion(CLIENT_APPLICATION_VERSION);
        event.setClientDatabaseVersion(CLIENT_DATABASE_VERSION);
        event.setDuration(0);
        event.setIdentifiers(new HashMap<>());
        event.addDetails("hfr_code", hfrCode);

        event.addObs(startObs());
        event.addObs(endObs());
        event.addObs(obs("verification_date", item.getVerificationDate()));
        event.addObs(obs("hiv_final_verification_result_code", normalizeResult(item.getHivFinalVerificationResultCode()).toLowerCase()));
        if (hasText(item.getCtcId())) {
            event.addObs(obs("ctc_id", item.getCtcId()));
        }
        event.addObs(obs("visit_id", item.getVisitId()));

        return event;
    }

    private Date toDate(String dateValue) {
        LocalDate localDate = LocalDate.parse(dateValue);
        return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    private Obs obs(String fieldCode, Object value) {
        return new Obs(
                "concept",
                "text",
                fieldCode,
                "",
                List.of(value),
                null,
                null,
                fieldCode
        );
    }

    private Obs startObs() {
        return new Obs(
                "concept",
                "start",
                "163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "",
                List.of(new Date()),
                null,
                null,
                "start"
        );
    }

    private Obs endObs() {
        return new Obs(
                "concept",
                "end",
                "163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "",
                List.of(new Date()),
                null,
                null,
                "end"
        );
    }

    private Map<String, Object> errorItem(int index,
                                          VerificationResultsRequest.VerificationResultItem item,
                                          String message) {
        Map<String, Object> errorItem = new LinkedHashMap<>();
        errorItem.put("itemIndex", index + 1);
        errorItem.put("clientCode", item == null ? null : item.getClientCode());
        errorItem.put("visitId", item == null ? null : item.getVisitId());
        errorItem.put("message", message);
        return errorItem;
    }

    private boolean isSuccessfulSend(String sendResult) {
        return hasText(sendResult) && sendResult.toLowerCase(Locale.ROOT).contains("successful");
    }

    private String normalizeResult(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String firstNonBlank(String first, String second) {
        if (!isBlank(first)) {
            return first;
        }
        return second;
    }

    private static String resolveOpenSrpEventUrl() {
        String eventUrl = EnvConfig.getFirstOrDefault(null, "OPENSRP_SERVER_EVENT_URL");
        if (!isBlank(eventUrl)) {
            return eventUrl;
        }

        String serverUrl = EnvConfig.getFirstOrDefault(null, "OPENSRP_SERVER_URL");
        if (isBlank(serverUrl)) {
            return null;
        }

        String normalizedUrl = serverUrl.trim();
        while (normalizedUrl.endsWith("/")) {
            normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - 1);
        }

        if (normalizedUrl.toLowerCase(Locale.ROOT).endsWith(OPENSRP_EVENT_PATH)) {
            return normalizedUrl;
        }
        return normalizedUrl + OPENSRP_EVENT_PATH;
    }

    private static boolean hasText(String value) {
        return !isBlank(value);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static DefaultDependencies defaultDependencies() {
        PostgresConnectionFactory connectionFactory = new PostgresConnectionFactory();
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository(connectionFactory.schema());
        return new DefaultDependencies(connectionFactory, repository);
    }

    private record DefaultDependencies(
            PostgresConnectionFactory connectionFactory,
            OpenSrpIntegrationRepository repository
    ) {
    }
}

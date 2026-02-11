package com.abt;

import akka.actor.typed.ActorSystem;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import com.abt.integration.exception.ValidationException;
import com.abt.integration.model.ApiErrorResponse;
import com.abt.integration.model.IntegrationRequest;
import com.abt.integration.service.IntegrationEndpointService;
import com.abt.integration.service.OpenSrpIntegrationService;
import com.abt.util.CustomJacksonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static akka.http.javadsl.server.Directives.complete;
import static akka.http.javadsl.server.Directives.concat;
import static akka.http.javadsl.server.Directives.entity;
import static akka.http.javadsl.server.Directives.get;
import static akka.http.javadsl.server.Directives.path;
import static akka.http.javadsl.server.Directives.pathPrefix;
import static akka.http.javadsl.server.Directives.post;

public class UcsCbhtsCtsIntegrationRoutes {
    private static final Logger log = LoggerFactory.getLogger(UcsCbhtsCtsIntegrationRoutes.class);

    private final IntegrationEndpointService integrationEndpointService;

    public UcsCbhtsCtsIntegrationRoutes(ActorSystem<?> system) {
        this(new OpenSrpIntegrationService());
    }

    public UcsCbhtsCtsIntegrationRoutes(IntegrationEndpointService integrationEndpointService) {
        this.integrationEndpointService = integrationEndpointService;
    }

    public Route integrationRoutes() {
        return concat(
                path("health", () -> get(() -> complete(StatusCodes.OK, Map.of("status", "ok"), Jackson.marshaller()))),
                pathPrefix("integration", () ->
                        path("ctc2hts", () ->
                                post(() ->
                                        entity(CustomJacksonSupport.customJacksonUnmarshaller(IntegrationRequest.class), request -> {
                                            try {
                                                Map<String, Object> response = integrationEndpointService.fetch(request);
                                                return complete(StatusCodes.OK, response, Jackson.marshaller());
                                            } catch (ValidationException e) {
                                                return complete(
                                                        StatusCodes.BAD_REQUEST,
                                                        new ApiErrorResponse("Invalid request payload", e.getErrors()),
                                                        Jackson.marshaller()
                                                );
                                            } catch (Exception e) {
                                                log.error("Failed to process CTC2HTS integration request", e);
                                                return complete(
                                                        StatusCodes.INTERNAL_SERVER_ERROR,
                                                        new ApiErrorResponse("Failed to process integration request", List.of(e.getMessage())),
                                                        Jackson.marshaller()
                                                );
                                            }
                                        })
                                )
                        )
                )
        );
    }
}

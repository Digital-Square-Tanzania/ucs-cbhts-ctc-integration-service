package com.abt;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.AskPattern;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import com.abt.domain.IndexContactRequest;
import com.abt.domain.LtfClientRequest;
import com.abt.util.CustomJacksonSupport;
import com.abt.util.EnvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.*;

/**
 * Routes can be defined in separated classes like shown in here
 */
public class UcsCtcIntegrationRoutes {
    //#routes-class
    private final static Logger log =
            LoggerFactory.getLogger(UcsCtcIntegrationRoutes.class);
    private static final String DEFAULT_DESTINATION_URL = "http://127.0.0.1:8080";
    private static final String DEFAULT_DESTINATION_USERNAME = "username";
    private static final String DEFAULT_DESTINATION_PASSWORD = "password";
    private static final Duration DEFAULT_ASK_TIMEOUT = Duration.ofSeconds(60);
    private static final String OPENSRP_EVENT_PATH = "/opensrp/rest/event/add";

    private final ActorRef<UcsCtcIntegrationRegistry.Command> ctcIntegrationActor;
    private final Duration askTimeout;
    private final Scheduler scheduler;
    private final String url;
    private final String username;
    private final String password;

    public UcsCtcIntegrationRoutes(ActorSystem<?> system,
                                   ActorRef<UcsCtcIntegrationRegistry.Command> ctcIntegrationActor) {
        this.ctcIntegrationActor = ctcIntegrationActor;
        scheduler = system.scheduler();
        askTimeout = EnvConfig.getDurationOrDefault("INTEGRATION_SERVICE_ROUTES_ASK_TIMEOUT", DEFAULT_ASK_TIMEOUT);
        String eventOrBaseUrl = EnvConfig.getFirstOrDefault(
                DEFAULT_DESTINATION_URL,
                "OPENSRP_SERVER_EVENT_URL",
                "OPENSRP_SERVER_URL"
        );
        url = extractBaseUrl(eventOrBaseUrl);
        username = EnvConfig.getFirstOrDefault(
                DEFAULT_DESTINATION_USERNAME,
                "OPENSRP_SERVER_USERNAME"
        );
        password = EnvConfig.getFirstOrDefault(
                DEFAULT_DESTINATION_PASSWORD,
                "OPENSRP_SERVER_PASSWORD"
        );

    }

    private String extractBaseUrl(String eventOrBaseUrl) {
        String normalizedUrl = eventOrBaseUrl.trim();
        while (normalizedUrl.endsWith("/")) {
            normalizedUrl = normalizedUrl.substring(0, normalizedUrl.length() - 1);
        }

        String normalizedLower = normalizedUrl.toLowerCase(Locale.ROOT);
        String eventPathLower = OPENSRP_EVENT_PATH.toLowerCase(Locale.ROOT);
        if (normalizedLower.endsWith(eventPathLower)) {
            return normalizedUrl.substring(0, normalizedUrl.length() - OPENSRP_EVENT_PATH.length());
        }
        return normalizedUrl;
    }

    private CompletionStage<UcsCtcIntegrationRegistry.ActionPerformed> sendLtFMissapClients(LtfClientRequest ltfClientRequest) {
        return AskPattern.ask(ctcIntegrationActor,
                ref -> new UcsCtcIntegrationRegistry.SendLtfMissapRequests(ltfClientRequest, url, username, password, ref), askTimeout, scheduler);
    }

    private CompletionStage<UcsCtcIntegrationRegistry.ActionPerformed> sendIndexContactsRequest(IndexContactRequest indexContactRequest) {
        return AskPattern.ask(ctcIntegrationActor,
                ref -> new UcsCtcIntegrationRegistry.SendIndexContacts(indexContactRequest, url, username, password, ref), askTimeout, scheduler);
    }

    /**
     * This method creates one route (of possibly many more that will be part
     * of your Web App)
     */
    public Route ucsIntegrationRoutes() {
        return pathPrefix("send", () ->
                concat(
                        //#send-results
                        pathSuffix("-ltf-missap-clients", () ->
                                concat(
                                        post(() ->
                                                entity(
                                                        CustomJacksonSupport.customJacksonUnmarshaller(LtfClientRequest.class),
                                                        ltfClientRequest ->
                                                                onSuccess(sendLtFMissapClients(ltfClientRequest), performed -> {
                                                                    log.info(
                                                                            "Sent LTF/MISSAP: {}", performed.response());
                                                                    if (performed.response().getDescription().toLowerCase().contains("error")) {
                                                                        return complete(StatusCodes.BAD_REQUEST, performed, Jackson.marshaller());
                                                                    } else {
                                                                        return complete(StatusCodes.OK, performed, Jackson.marshaller());
                                                                    }
                                                                })
                                                )
                                        )
                                )
                        ),
                        //#send-rejections
                        pathSuffix("-index-contacts", () ->
                                concat(
                                        post(() ->
                                                entity(
                                                        Jackson.unmarshaller(IndexContactRequest.class),
                                                        indexContactRequest ->
                                                                onSuccess(sendIndexContactsRequest(indexContactRequest), performed -> {
                                                                    log.info(
                                                                            "Sent Index Contacts: {}", performed.response().getDescription());
                                                                    if (performed.response().getDescription().toLowerCase().contains("error")) {
                                                                        return complete(StatusCodes.BAD_REQUEST, performed, Jackson.marshaller());
                                                                    } else {
                                                                        return complete(StatusCodes.OK, performed, Jackson.marshaller());
                                                                    }
                                                                })
                                                )
                                        )
                                )
                        )
                )
        );
    }
}

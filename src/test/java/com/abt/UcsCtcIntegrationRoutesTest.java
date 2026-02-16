package com.abt;

import akka.actor.typed.ActorRef;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import com.abt.domain.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UcsCtcIntegrationRoutesTest extends akka.http.javadsl.testkit.JUnitRouteTest {

    private ActorTestKit actorTestKit;

    @BeforeEach
    void setUp() {
        systemResource().before();
        System.setProperty("INTEGRATION_SERVICE_ROUTES_ASK_TIMEOUT", "3s");
        System.setProperty("OPENSRP_SERVER_EVENT_URL", "http://localhost/opensrp/rest/event/add");
        System.clearProperty("OPENSRP_SERVER_URL");
        System.setProperty("OPENSRP_SERVER_USERNAME", "user");
        System.setProperty("OPENSRP_SERVER_PASSWORD", "pass");
        actorTestKit = ActorTestKit.create();
    }

    @AfterEach
    void tearDown() {
        actorTestKit.shutdownTestKit();
        System.clearProperty("INTEGRATION_SERVICE_ROUTES_ASK_TIMEOUT");
        System.clearProperty("OPENSRP_SERVER_EVENT_URL");
        System.clearProperty("OPENSRP_SERVER_URL");
        System.clearProperty("OPENSRP_SERVER_USERNAME");
        System.clearProperty("OPENSRP_SERVER_PASSWORD");
        systemResource().after();
    }

    @Test
    void sendLtfMissapRoute_shouldReturnOkWhenActorResponseIsSuccessful() {
        Route route = buildRouteWithDescription("sending successful");

        testRoute(route)
                .run(HttpRequest.POST("/send-ltf-missap-clients")
                        .withEntity(ContentTypes.APPLICATION_JSON, "{ }"))
                .assertStatusCode(StatusCodes.OK);
    }

    @Test
    void sendLtfMissapRoute_shouldReturnBadRequestWhenActorResponseHasError() {
        Route route = buildRouteWithDescription("Error: downstream failure");

        testRoute(route)
                .run(HttpRequest.POST("/send-ltf-missap-clients")
                        .withEntity(ContentTypes.APPLICATION_JSON, "{ }"))
                .assertStatusCode(StatusCodes.BAD_REQUEST);
    }

    @Test
    void sendIndexContactsRoute_shouldReturnOkWhenActorResponseIsSuccessful() {
        Route route = buildRouteWithDescription("sending successful");

        testRoute(route)
                .run(HttpRequest.POST("/send-index-contacts")
                        .withEntity(ContentTypes.APPLICATION_JSON, "{ }"))
                .assertStatusCode(StatusCodes.OK);
    }

    @Test
    void sendIndexContactsRoute_shouldReturnBadRequestWhenActorResponseHasError() {
        Route route = buildRouteWithDescription("Error: downstream failure");

        testRoute(route)
                .run(HttpRequest.POST("/send-index-contacts")
                        .withEntity(ContentTypes.APPLICATION_JSON, "{ }"))
                .assertStatusCode(StatusCodes.BAD_REQUEST);
    }

    @Test
    void sendRoutes_shouldDeriveBaseUrlFromOpenSrpEventUrl() {
        AtomicReference<String> observedUrl = new AtomicReference<>();
        Route route = buildRouteWithDescriptionAndObservedUrl("sending successful", observedUrl);

        testRoute(route)
                .run(HttpRequest.POST("/send-ltf-missap-clients")
                        .withEntity(ContentTypes.APPLICATION_JSON, "{ }"))
                .assertStatusCode(StatusCodes.OK);

        assertEquals("http://localhost", observedUrl.get());
    }

    private Route buildRouteWithDescription(String description) {
        return buildRouteWithDescriptionAndObservedUrl(description, new AtomicReference<>());
    }

    private Route buildRouteWithDescriptionAndObservedUrl(String description, AtomicReference<String> observedUrl) {
        ActorRef<UcsCtcIntegrationRegistry.Command> registryActor = actorTestKit.spawn(
                Behaviors.receive(UcsCtcIntegrationRegistry.Command.class)
                        .onMessage(UcsCtcIntegrationRegistry.SendLtfMissapRequests.class, command -> {
                            observedUrl.set(command.url());
                            Response response = new Response();
                            response.setDescription(description);
                            command.replyTo().tell(new UcsCtcIntegrationRegistry.ActionPerformed(response));
                            return Behaviors.same();
                        })
                        .onMessage(UcsCtcIntegrationRegistry.SendIndexContacts.class, command -> {
                            observedUrl.set(command.url());
                            Response response = new Response();
                            response.setDescription(description);
                            command.replyTo().tell(new UcsCtcIntegrationRegistry.ActionPerformed(response));
                            return Behaviors.same();
                        })
                        .build()
        );

        return new UcsCtcIntegrationRoutes(actorTestKit.system(), registryActor).ucsIntegrationRoutes();
    }
}

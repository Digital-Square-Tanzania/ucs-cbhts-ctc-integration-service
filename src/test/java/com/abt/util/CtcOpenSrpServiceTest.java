package com.abt.util;

import com.abt.domain.Client;
import com.abt.domain.LtfClientRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CtcOpenSrpServiceTest {

    private static final String USE_LTF_EVENTS_VERSION_2_ENV_KEY = "USE_LTF_EVENTS_VERSION_2";

    @AfterEach
    void clearProperties() {
        System.clearProperty(USE_LTF_EVENTS_VERSION_2_ENV_KEY);
    }

    @Test
    void eventAddUrl_shouldBuildExpectedEventEndpoint() {
        assertEquals(
                "http://localhost:8080/opensrp/rest/event/add",
                CtcOpenSrpService.eventAddUrl("http://localhost:8080/")
        );
    }

    @Test
    void taskCreateUrl_shouldBuildExpectedTaskEndpoint() {
        assertEquals(
                "http://localhost:8080/opensrp/rest/task",
                CtcOpenSrpService.taskCreateUrl("http://localhost:8080/")
        );
    }

    @Test
    void uniqueIdsUrl_shouldBuildExpectedUniqueIdsEndpoint() {
        assertEquals(
                "http://localhost:8080/opensrp/uniqueids/get?source=2&numberToGenerate=3",
                CtcOpenSrpService.uniqueIdsUrl("http://localhost:8080/", 3)
        );
    }

    @Test
    void isLtfEventVersion2Enabled_shouldDefaultToFalse() {
        assertFalse(CtcOpenSrpService.isLtfEventVersion2Enabled());
        assertTrue(CtcOpenSrpService.shouldGenerateAndSendLtfTask());
    }

    @Test
    void isLtfEventVersion2Enabled_shouldUseConfiguredFlag() {
        System.setProperty(USE_LTF_EVENTS_VERSION_2_ENV_KEY, "true");

        assertTrue(CtcOpenSrpService.isLtfEventVersion2Enabled());
        assertFalse(CtcOpenSrpService.shouldGenerateAndSendLtfTask());
    }

    @Test
    void resolveLtfEvent_shouldUseCurrentImplementationWhenFlagDisabled() {
        System.setProperty(USE_LTF_EVENTS_VERSION_2_ENV_KEY, "false");

        assertEquals(
                "Referral Registration",
                CtcOpenSrpService.resolveLtfEvent(testClient(), testLtfRequest()).getEventType()
        );
    }

    @Test
    void resolveLtfEvent_shouldUseVersion2ImplementationWhenFlagEnabled() {
        System.setProperty(USE_LTF_EVENTS_VERSION_2_ENV_KEY, "true");

        assertEquals(
                "LTF Referral Registration",
                CtcOpenSrpService.resolveLtfEvent(testClient(), testLtfRequest()).getEventType()
        );
    }

    private static Client testClient() {
        return new Client("base-entity-id");
    }

    private static LtfClientRequest testLtfRequest() {
        LtfClientRequest request = new LtfClientRequest();
        request.setBaseEntityId("base-entity-id");
        request.setUniqueId("unique-id");
        request.setLocationId("location-id");
        request.setProviderId("provider-id");
        request.setTeamId("team-id");
        request.setTeam("team");
        request.setRecGuid("rec-guid");
        request.setLastAppointmentDate("2026-03-20");
        return request;
    }
}

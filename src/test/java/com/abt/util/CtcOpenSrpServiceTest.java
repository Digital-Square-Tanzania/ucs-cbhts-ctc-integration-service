package com.abt.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CtcOpenSrpServiceTest {

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
}

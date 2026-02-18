package com.abt.integration.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IntegrationRequestTest {

    @Test
    void setStartDate_shouldKeepUnixSeconds() {
        IntegrationRequest request = new IntegrationRequest();

        request.setStartDate(1768262400L);

        assertEquals(1768262400L, request.getStartDate());
    }

    @Test
    void setStartDate_shouldKeepUnixMilliseconds() {
        IntegrationRequest request = new IntegrationRequest();

        request.setStartDate(1768262400000L);

        assertEquals(1768262400000L, request.getStartDate());
    }

    @Test
    void setEndDate_shouldKeepUnixSeconds() {
        IntegrationRequest request = new IntegrationRequest();

        request.setEndDate(1768262800L);

        assertEquals(1768262800L, request.getEndDate());
    }

    @Test
    void setEndDate_shouldKeepUnixMilliseconds() {
        IntegrationRequest request = new IntegrationRequest();

        request.setEndDate(1768262800000L);

        assertEquals(1768262800000L, request.getEndDate());
    }

    @Test
    void setDates_shouldAllowNull() {
        IntegrationRequest request = new IntegrationRequest();

        request.setStartDate(null);
        request.setEndDate(null);

        assertNull(request.getStartDate());
        assertNull(request.getEndDate());
    }
}

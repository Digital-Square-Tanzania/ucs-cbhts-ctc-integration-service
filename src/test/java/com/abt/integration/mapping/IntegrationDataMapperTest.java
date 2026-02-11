package com.abt.integration.mapping;

import com.abt.integration.db.OpenSrpIntegrationRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrationDataMapperTest {

    private final IntegrationDataMapper mapper = new IntegrationDataMapper();

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldApplyMappingsAndOutputShape() {
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
                "normal_client",
                "cbhts",
                "individual",
                "tb_suspect",
                "yes",
                "relative,friend",
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

        OpenSrpIntegrationRepository.TestRow selfTest = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "bioline",
                "BATCH001",
                "2026-12-31",
                "reactive",
                null,
                "self_test",
                1768262800000L
        );

        OpenSrpIntegrationRepository.TestRow reagentTest = new OpenSrpIntegrationRepository.TestRow(
                "test-2",
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

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(selfTest, reagentTest));

        assertEquals("CBHTS", mapped.get("htcApproach"));
        assertEquals("2025-12-20", mapped.get("visitDate"));

        Map<String, Object> demographics = (Map<String, Object>) mapped.get("demographics");
        assertEquals("HO", demographics.get("maritalStatusCode"));
        assertEquals("MO", demographics.get("pregnancyStatusCode"));

        Map<String, Object> clientClassification = (Map<String, Object>) mapped.get("clientClassification");
        assertEquals("MB", clientClassification.get("previousTestClientType"));
        assertEquals("MK", clientClassification.get("clientType"));
        assertEquals("MP", clientClassification.get("attendanceCode"));
        assertTrue((Boolean) clientClassification.get("eligibleForTesting"));

        Map<String, Object> currentTesting = (Map<String, Object>) mapped.get("currentTesting");
        assertEquals("HW", currentTesting.get("referredFromCode"));
        assertEquals("MY", currentTesting.get("counsellingTypeCode"));
        assertEquals("KU", currentTesting.get("tbScreeningDetails"));

        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");
        assertEquals(1, selfTesting.size());
        assertEquals("R", selfTesting.get(0).get("selfTestingResults"));

        List<Map<String, Object>> reagentTesting = (List<Map<String, Object>>) mapped.get("reagentTesting");
        assertEquals(1, reagentTesting.size());
        assertEquals("DUAL", reagentTesting.get(0).get("reagentTest"));
        assertEquals("NR", reagentTesting.get(0).get("reagentResult"));
        assertEquals("POSITIVE", reagentTesting.get(0).get("syphilisResult"));

        Map<String, Object> preventionServices = (Map<String, Object>) mapped.get("preventionServices");
        assertTrue((Boolean) preventionServices.get("condomGiven"));

        Map<String, Object> referralAndOutcome = (Map<String, Object>) mapped.get("referralAndOutcome");
        assertEquals("DK", referralAndOutcome.get("referredToCode"));
        assertEquals("13211-1", referralAndOutcome.get("toFacility"));
    }
}

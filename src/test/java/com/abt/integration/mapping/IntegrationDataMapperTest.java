package com.abt.integration.mapping;

import com.abt.integration.db.OpenSrpIntegrationRepository;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
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
        assertEquals("SINGLE", demographics.get("maritalStatusCode"));
        assertEquals("PREGNANT", demographics.get("pregnancyStatusCode"));

        Map<String, Object> clientClassification = (Map<String, Object>) mapped.get("clientClassification");
        assertEquals("SELF", clientClassification.get("previousTestClientType"));
        assertEquals("GENERAL_CLIENT", clientClassification.get("clientType"));
        assertEquals("NEW_CLIENT", clientClassification.get("attendanceCode"));
        assertTrue((Boolean) clientClassification.get("eligibleForTesting"));

        Map<String, Object> currentTesting = (Map<String, Object>) mapped.get("currentTesting");
        assertEquals("COMMUNITY_TESTING_SERVICE", currentTesting.get("referredFromCode"));
        assertEquals("INDIVIDUAL", currentTesting.get("counsellingTypeCode"));
        assertEquals("TB_PRESUMPTIVE", currentTesting.get("tbScreeningDetails"));

        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");
        assertEquals(1, selfTesting.size());
        assertEquals("REACTIVE", selfTesting.get(0).get("selfTestingResults"));

        List<Map<String, Object>> reagentTesting = (List<Map<String, Object>>) mapped.get("reagentTesting");
        assertEquals(1, reagentTesting.size());
        assertEquals("DUAL", reagentTesting.get(0).get("reagentTest"));
        assertEquals("NON_REACTIVE", reagentTesting.get(0).get("reagentResult"));
        assertEquals("POSITIVE", reagentTesting.get(0).get("syphilisResult"));

        Map<String, Object> preventionServices = (Map<String, Object>) mapped.get("preventionServices");
        assertTrue((Boolean) preventionServices.get("condomGiven"));

        Map<String, Object> referralAndOutcome = (Map<String, Object>) mapped.get("referralAndOutcome");
        assertEquals("PREP_SERVICE", referralAndOutcome.get("referredToCode"));
        assertEquals("13211-1", referralAndOutcome.get("toFacility"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapSupportedDatabaseMaritalStatuses() {
        Map<String, String> expectedValuesByMaritalStatus = new LinkedHashMap<>();
        expectedValuesByMaritalStatus.put("Single", "SINGLE");
        expectedValuesByMaritalStatus.put("Married", "MARRIED_MONOGAMOUS");
        expectedValuesByMaritalStatus.put("Divorced", "SEPARATED_DIVORCED");
        expectedValuesByMaritalStatus.put("Widowed", "WIDOWED");
        expectedValuesByMaritalStatus.put("Cohabitation", "COHABITING");

        for (Map.Entry<String, String> entry : expectedValuesByMaritalStatus.entrySet()) {
            OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow(entry.getKey());

            Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
            Map<String, Object> demographics = (Map<String, Object>) mapped.get("demographics");

            assertEquals(entry.getValue(), demographics.get("maritalStatusCode"),
                    "Unexpected marital value for status " + entry.getKey());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapReagentTestsToIntegrationValues() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.TestRow multiTest = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "multiTest",
                "B1",
                "2026-01-01",
                "non_reactive",
                null,
                "first",
                1768262800000L
        );

        OpenSrpIntegrationRepository.TestRow bioline = new OpenSrpIntegrationRepository.TestRow(
                "test-2",
                "visit-group-1",
                "base-1",
                "bioline",
                "B2",
                "2026-01-01",
                "non_reactive",
                null,
                "first",
                1768262801000L
        );

        OpenSrpIntegrationRepository.TestRow unigold = new OpenSrpIntegrationRepository.TestRow(
                "test-3",
                "visit-group-1",
                "base-1",
                "unigold",
                "B3",
                "2026-01-01",
                "non_reactive",
                null,
                "first",
                1768262802000L
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(multiTest, bioline, unigold));
        List<Map<String, Object>> reagentTesting = (List<Map<String, Object>>) mapped.get("reagentTesting");

        assertEquals(3, reagentTesting.size());
        assertEquals("DUAL", reagentTesting.get(0).get("reagentTest"));
        assertEquals("SD_BIOLINE", reagentTesting.get(1).get("reagentTest"));
        assertEquals("UNIGOLD", reagentTesting.get(2).get("reagentTest"));
    }

    private OpenSrpIntegrationRepository.ServiceRow buildServiceRow(String maritalStatus) {
        return new OpenSrpIntegrationRepository.ServiceRow(
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
                maritalStatus,
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
    }
}

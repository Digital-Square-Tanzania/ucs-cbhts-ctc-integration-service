package com.abt.integration.mapping;

import com.abt.integration.db.OpenSrpIntegrationRepository;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
                "reactive",
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

        OpenSrpIntegrationRepository.TestRow reagentTest = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "hiv_syphilis_dual",
                "RB001",
                "2026-08-31",
                "non_reactive",
                "positive",
                "First HIV Test",
                1768262800000L
        );

        OpenSrpIntegrationRepository.HivstSelfTestRow hivstSelfTestRow = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-1",
                "2025-12-20",
                "base-1",
                "client",
                "KIT001",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-1",
                "2025-12-20T11:30:15.000+03:00",
                "BATCH001",
                "2026-12-31"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(reagentTest), List.of(hivstSelfTestRow));

        assertEquals("CBHTS", mapped.get("htcApproach"));
        assertEquals("2025-12-20", mapped.get("visitDate"));
        Map<String, Object> counsellor = (Map<String, Object>) mapped.get("counsellor");
        assertEquals("provider-1", counsellor.get("counsellorID"));
        assertEquals("John Doe", counsellor.get("counsellorName"));

        Map<String, Object> demographics = (Map<String, Object>) mapped.get("demographics");
        assertEquals("SINGLE", demographics.get("maritalStatusCode"));
        assertEquals("UNKNOWN", demographics.get("pregnancyStatusCode"));

        Map<String, Object> clientClassification = (Map<String, Object>) mapped.get("clientClassification");
        assertEquals("SELF", clientClassification.get("previousTestClientType"));
        assertEquals("GENERAL_CLIENT", clientClassification.get("clientType"));
        assertEquals("NEW_CLIENT", clientClassification.get("attendanceCode"));
        assertTrue((Boolean) clientClassification.get("eligibleForTesting"));

        Map<String, Object> currentTesting = (Map<String, Object>) mapped.get("currentTesting");
        assertTrue(!currentTesting.containsKey("referredFromCode"));
        assertEquals("INDIVIDUAL", currentTesting.get("counsellingTypeCode"));
        assertEquals("TB_PRESUMPTIVE", currentTesting.get("tbScreeningDetails"));

        Map<String, Object> residence = (Map<String, Object>) mapped.get("residence");
        assertEquals(1, residence.size());
        assertEquals("TZ.NT.MY.ML.4.8.1", residence.get("villageStreet"));

        Map<String, Object> testingHistory = (Map<String, Object>) mapped.get("testingHistory");
        assertEquals("SELF_TEST_ORAL", testingHistory.get("testingTypePrevious"));
        assertEquals("REACTIVE", testingHistory.get("previousTestResult"));

        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");
        assertEquals(1, selfTesting.size());
        assertEquals("KIT001", selfTesting.get(0).get("selfTestKitCode"));
        assertEquals("BATCH001", selfTesting.get(0).get("selfTestBatchNo"));
        assertEquals("2026-12-31", selfTesting.get(0).get("selfTestExpiryDate"));
        assertEquals("SELF", selfTesting.get(0).get("selfTestKitName"));
        assertNotEquals("Client", selfTesting.get(0).get("selfTestKitName"));
        assertEquals("REACTIVE", selfTesting.get(0).get("selfTestingResults"));

        List<Map<String, Object>> reagentTesting = (List<Map<String, Object>>) mapped.get("reagentTesting");
        assertEquals(1, reagentTesting.size());
        assertEquals("DUAL", reagentTesting.get(0).get("reagentTest"));
        assertEquals("NON_REACTIVE", reagentTesting.get(0).get("reagentResult"));
        assertEquals("POSITIVE", reagentTesting.get(0).get("syphilisResult"));

        Map<String, Object> preventionServices = (Map<String, Object>) mapped.get("preventionServices");
        assertTrue((Boolean) preventionServices.get("condomGiven"));

        List<Map<String, Object>> referralAndOutcome = (List<Map<String, Object>>) mapped.get("referralAndOutcome");
        assertEquals(1, referralAndOutcome.size());
        assertEquals("PREP_SERVICE", referralAndOutcome.get(0).get("referredToCode"));
        assertEquals("13211-1", referralAndOutcome.get(0).get("toFacility"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapEligibleForTestingFromEnrollmentValueWithDefaults() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        Map<String, Object> mappedTrue = mapper.mapServiceRow(serviceRow, List.of(), List.of(), true);
        Map<String, Object> mappedFalse = mapper.mapServiceRow(serviceRow, List.of(), List.of(), false);
        Map<String, Object> mappedNull = mapper.mapServiceRow(serviceRow, List.of(), List.of(), null);
        Map<String, Object> mappedMissing = mapper.mapServiceRow(serviceRow, List.of());

        Map<String, Object> clientClassificationTrue = (Map<String, Object>) mappedTrue.get("clientClassification");
        Map<String, Object> clientClassificationFalse = (Map<String, Object>) mappedFalse.get("clientClassification");
        Map<String, Object> clientClassificationNull = (Map<String, Object>) mappedNull.get("clientClassification");
        Map<String, Object> clientClassificationMissing = (Map<String, Object>) mappedMissing.get("clientClassification");

        assertEquals(true, clientClassificationTrue.get("eligibleForTesting"));
        assertEquals(false, clientClassificationFalse.get("eligibleForTesting"));
        assertEquals(true, clientClassificationNull.get("eligibleForTesting"));
        assertEquals(true, clientClassificationMissing.get("eligibleForTesting"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapResidenceToVillageStreetWhenHouseholdVillageCodeExists() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = withResidenceCodes(
                buildServiceRow("Single"),
                "VLG-123",
                "COUNCIL-1"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
        Map<String, Object> residence = (Map<String, Object>) mapped.get("residence");

        assertEquals(1, residence.size());
        assertEquals("VLG-123", residence.get("villageStreet"));
        assertTrue(!residence.containsKey("council"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapResidenceToCouncilWhenHouseholdLocationIsNull() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = withResidenceCodes(
                buildServiceRow("Single"),
                null,
                "COUNCIL-2"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
        Map<String, Object> residence = (Map<String, Object>) mapped.get("residence");

        assertEquals(1, residence.size());
        assertEquals("COUNCIL-2", residence.get("council"));
        assertTrue(!residence.containsKey("villageStreet"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapResidenceToCouncilWhenHouseholdLocationIsUnmatched() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = withResidenceCodes(
                buildServiceRow("Single"),
                "   ",
                "COUNCIL-3"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
        Map<String, Object> residence = (Map<String, Object>) mapped.get("residence");

        assertEquals(1, residence.size());
        assertEquals("COUNCIL-3", residence.get("council"));
        assertTrue(!residence.containsKey("villageStreet"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapResidenceToCouncilWhenNoHouseholdRowIsFound() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = withResidenceCodes(
                buildServiceRow("Single"),
                null,
                "COUNCIL-4"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
        Map<String, Object> residence = (Map<String, Object>) mapped.get("residence");

        assertEquals(1, residence.size());
        assertEquals("COUNCIL-4", residence.get("council"));
        assertTrue(!residence.containsKey("villageStreet"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldIncludeMultipleHivstResultsForSameClientOnSameDay() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.HivstSelfTestRow clientResult = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-1",
                "2025-12-20",
                "base-1",
                "client",
                "KIT-CLIENT",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-1",
                "2025-12-20T00:15:00+03:00",
                "CLIENT-BATCH",
                "2027-01-11"
        );

        OpenSrpIntegrationRepository.HivstSelfTestRow peerResult = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-2",
                "2025-12-20",
                "base-1",
                "peer_friend",
                "KIT-PEER",
                "non_reactive",
                "2025-12-20",
                null,
                "issue-event-2",
                "2025-12-20T23:59:59+03:00",
                "PEER-BATCH",
                "2027-03-01"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(), List.of(clientResult, peerResult));
        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");

        assertEquals(2, selfTesting.size());
        assertEquals("CLIENT-BATCH", selfTesting.get(0).get("selfTestBatchNo"));
        assertEquals("PEER-BATCH", selfTesting.get(1).get("selfTestBatchNo"));
        assertEquals("2027-01-11", selfTesting.get(0).get("selfTestExpiryDate"));
        assertEquals("2027-03-01", selfTesting.get(1).get("selfTestExpiryDate"));
        assertEquals("SELF", selfTesting.get(0).get("selfTestKitName"));
        assertEquals("PEER_FRIEND", selfTesting.get(1).get("selfTestKitName"));
        assertNotEquals("Client", selfTesting.get(0).get("selfTestKitName"));
        assertNotEquals("Peer Friend", selfTesting.get(1).get("selfTestKitName"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapSelfTestKitNameToIntegrationValuesOnly() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.HivstSelfTestRow selfRow = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-self",
                "2025-12-20",
                "base-1",
                "client",
                "KIT-SELF",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-self",
                "2025-12-20T10:00:00+03:00",
                "BATCH-SELF",
                "2027-01-11"
        );

        OpenSrpIntegrationRepository.HivstSelfTestRow sexualPartnerRow = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-sp",
                "2025-12-20",
                "base-1",
                "sexual_partner",
                "KIT-SP",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-sp",
                "2025-12-20T11:00:00+03:00",
                "BATCH-SP",
                "2027-01-11"
        );

        OpenSrpIntegrationRepository.HivstSelfTestRow peerFriendTypoRow = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-pf",
                "2025-12-20",
                "base-1",
                "peer_fried",
                "KIT-PF",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-pf",
                "2025-12-20T12:00:00+03:00",
                "BATCH-PF",
                "2027-01-11"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(
                serviceRow,
                List.of(),
                List.of(selfRow, sexualPartnerRow, peerFriendTypoRow)
        );
        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");

        assertEquals(3, selfTesting.size());
        assertEquals("SELF", selfTesting.get(0).get("selfTestKitName"));
        assertEquals("SEXUAL_PARTNER", selfTesting.get(1).get("selfTestKitName"));
        assertEquals("PEER_FRIEND", selfTesting.get(2).get("selfTestKitName"));

        assertNotEquals("Client", selfTesting.get(0).get("selfTestKitName"));
        assertNotEquals("Sexual Partner", selfTesting.get(1).get("selfTestKitName"));
        assertNotEquals("Peer Friend", selfTesting.get(2).get("selfTestKitName"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldExcludeHivstResultsWhenDateIsDifferentFromHtsEventDate() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.HivstSelfTestRow differentDay = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-3",
                "2025-12-21",
                "base-1",
                "client",
                "KIT-CLIENT",
                "reactive",
                "2025-12-21",
                null,
                "issue-event-3",
                "2025-12-21T01:00:00+03:00",
                "CLIENT-BATCH",
                "2027-01-11"
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(), List.of(differentDay));
        List<Map<String, Object>> selfTesting = (List<Map<String, Object>>) mapped.get("selfTesting");

        assertEquals(0, selfTesting.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldSkipSelfTestingWhenVisitDateOrIssueEventDateIsMissing() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");
        OpenSrpIntegrationRepository.ServiceRow missingVisitDateServiceRow = withVisitDate(baseServiceRow, null);

        OpenSrpIntegrationRepository.HivstSelfTestRow validIssueDate = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-4",
                "2025-12-20",
                "base-1",
                "client",
                "KIT-CLIENT",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-4",
                "2025-12-20T11:30:15.000+03:00",
                "CLIENT-BATCH",
                "2027-01-11"
        );

        Map<String, Object> missingVisitDateMapped = mapper.mapServiceRow(
                missingVisitDateServiceRow,
                List.of(),
                List.of(validIssueDate)
        );
        List<Map<String, Object>> missingVisitDateSelfTesting = (List<Map<String, Object>>) missingVisitDateMapped.get("selfTesting");
        assertEquals(0, missingVisitDateSelfTesting.size());

        OpenSrpIntegrationRepository.HivstSelfTestRow blankIssueDate = new OpenSrpIntegrationRepository.HivstSelfTestRow(
                "result-event-5",
                "2025-12-20",
                "base-1",
                "client",
                "KIT-CLIENT",
                "reactive",
                "2025-12-20",
                null,
                "issue-event-5",
                "   ",
                "CLIENT-BATCH",
                "2027-01-11"
        );

        Map<String, Object> blankIssueDateMapped = mapper.mapServiceRow(
                baseServiceRow,
                List.of(),
                List.of(blankIssueDate)
        );
        List<Map<String, Object>> blankIssueDateSelfTesting = (List<Map<String, Object>>) blankIssueDateMapped.get("selfTesting");
        assertEquals(0, blankIssueDateSelfTesting.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapClientIdentificationByPriorityAcrossSupportedIds() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.ServiceRow withNational = withIdentificationDocuments(
                baseServiceRow,
                "NAT123",
                "VOT123",
                "DL123",
                "P123"
        );
        OpenSrpIntegrationRepository.ServiceRow withVoter = withIdentificationDocuments(
                baseServiceRow,
                null,
                "VOT123",
                "DL123",
                "P123"
        );
        OpenSrpIntegrationRepository.ServiceRow withDriverLicense = withIdentificationDocuments(
                baseServiceRow,
                null,
                null,
                "DL123",
                "P123"
        );
        OpenSrpIntegrationRepository.ServiceRow withPassport = withIdentificationDocuments(
                baseServiceRow,
                null,
                null,
                null,
                "P123"
        );

        Map<String, Object> mappedNational = mapper.mapServiceRow(withNational, List.of());
        Map<String, Object> mappedVoter = mapper.mapServiceRow(withVoter, List.of());
        Map<String, Object> mappedDriverLicense = mapper.mapServiceRow(withDriverLicense, List.of());
        Map<String, Object> mappedPassport = mapper.mapServiceRow(withPassport, List.of());

        Map<String, Object> nationalIdentification = (Map<String, Object>) mappedNational.get("clientIdentification");
        assertEquals("NIDA", nationalIdentification.get("clientUniqueIdentifierType"));
        assertEquals("NAT123", nationalIdentification.get("clientUniqueIdentifierCode"));

        Map<String, Object> voterIdentification = (Map<String, Object>) mappedVoter.get("clientIdentification");
        assertEquals("VOTER_ID", voterIdentification.get("clientUniqueIdentifierType"));
        assertEquals("VOT123", voterIdentification.get("clientUniqueIdentifierCode"));

        Map<String, Object> driverLicenseIdentification = (Map<String, Object>) mappedDriverLicense.get("clientIdentification");
        assertEquals("DRIVER_LICENSE", driverLicenseIdentification.get("clientUniqueIdentifierType"));
        assertEquals("DL123", driverLicenseIdentification.get("clientUniqueIdentifierCode"));

        Map<String, Object> passportIdentification = (Map<String, Object>) mappedPassport.get("clientIdentification");
        assertEquals("PASSPORT", passportIdentification.get("clientUniqueIdentifierType"));
        assertEquals("P123", passportIdentification.get("clientUniqueIdentifierCode"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldReturnEmptyClientIdentificationWhenAllIdsAreMissing() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = withIdentificationDocuments(
                buildServiceRow("Single"),
                null,
                null,
                null,
                null
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
        assertTrue(mapped.get("clientIdentification") instanceof List<?>);

        List<Object> clientIdentification = (List<Object>) mapped.get("clientIdentification");
        assertTrue(clientIdentification.isEmpty());
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
                "First HIV Test",
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
                "Repeat of First HIV Test",
                1768262801000L
        );

        OpenSrpIntegrationRepository.TestRow firstResponse = new OpenSrpIntegrationRepository.TestRow(
                "test-3",
                "visit-group-1",
                "base-1",
                " First Response ",
                "B3",
                "2026-01-01",
                "non_reactive",
                null,
                "Second HIV Test",
                1768262802000L
        );

        OpenSrpIntegrationRepository.TestRow unigold = new OpenSrpIntegrationRepository.TestRow(
                "test-4",
                "visit-group-1",
                "base-1",
                "unigold",
                "B4",
                "2026-01-01",
                "non_reactive",
                null,
                "Unigold HIV Test Result",
                1768262803000L
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(multiTest, bioline, firstResponse, unigold));
        List<Map<String, Object>> reagentTesting = (List<Map<String, Object>>) mapped.get("reagentTesting");

        assertEquals(4, reagentTesting.size());
        assertEquals("DUAL", reagentTesting.get(0).get("reagentTest"));
        assertEquals("FIRST", reagentTesting.get(0).get("testType"));
        assertEquals("SD_BIOLINE", reagentTesting.get(1).get("reagentTest"));
        assertEquals("REPEAT_FIRST", reagentTesting.get(1).get("testType"));
        assertEquals("FIRST_RESPONSE", reagentTesting.get(2).get("reagentTest"));
        assertEquals("SECOND", reagentTesting.get(2).get("testType"));
        assertEquals("UNIGOLD", reagentTesting.get(3).get("reagentTest"));
        assertEquals("THIRD", reagentTesting.get(3).get("testType"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldOverrideReferralToCtcClinicWhenUnigoldIsReactive() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.TestRow unigoldReactive = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "bioline",
                "B1",
                "2026-01-01",
                " reactive ",
                null,
                " unigold ",
                1768262800000L
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(unigoldReactive));
        List<Map<String, Object>> referralAndOutcome = (List<Map<String, Object>>) mapped.get("referralAndOutcome");

        assertEquals(1, referralAndOutcome.size());
        assertEquals("CTC_CLINIC", referralAndOutcome.get(0).get("referredToCode"));
        assertEquals("13211-1", referralAndOutcome.get(0).get("toFacility"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldKeepDefaultReferralWhenUnigoldIsNotReactive() {
        OpenSrpIntegrationRepository.ServiceRow serviceRow = buildServiceRow("Single");

        OpenSrpIntegrationRepository.TestRow unigoldNonReactive = new OpenSrpIntegrationRepository.TestRow(
                "test-1",
                "visit-group-1",
                "base-1",
                "bioline",
                "B1",
                "2026-01-01",
                "non_reactive",
                null,
                "unigold",
                1768262800000L
        );

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(unigoldNonReactive));
        List<Map<String, Object>> referralAndOutcome = (List<Map<String, Object>>) mapped.get("referralAndOutcome");

        assertEquals(1, referralAndOutcome.size());
        assertEquals("PREP_SERVICE", referralAndOutcome.get(0).get("referredToCode"));
        assertEquals("13211-1", referralAndOutcome.get(0).get("toFacility"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldSetPregnancyStatusNotApplicableForMaleClients() {
        OpenSrpIntegrationRepository.ServiceRow femaleServiceRow = buildServiceRow("Single");
        OpenSrpIntegrationRepository.ServiceRow maleServiceRow = new OpenSrpIntegrationRepository.ServiceRow(
                femaleServiceRow.eventId(),
                femaleServiceRow.baseEntityId(),
                femaleServiceRow.htsVisitGroup(),
                femaleServiceRow.visitDate(),
                femaleServiceRow.htsVisitDate(),
                femaleServiceRow.dateCreated(),
                femaleServiceRow.providerId(),
                femaleServiceRow.htsTestingApproach(),
                femaleServiceRow.htsVisitType(),
                femaleServiceRow.htsHasTheClientRecentlyTestedWithHivst(),
                femaleServiceRow.htsPreviousHivstClientType(),
                femaleServiceRow.htsPreviousHivstTestType(),
                femaleServiceRow.htsPreviousHivstTestResults(),
                femaleServiceRow.htsClientType(),
                femaleServiceRow.htsTestingPoint(),
                femaleServiceRow.htsTypeOfCounsellingProvided(),
                femaleServiceRow.htsClientsTbScreeningOutcome(),
                femaleServiceRow.htsHasPostTestCounsellingBeenProvided(),
                femaleServiceRow.htsHivResultsDisclosure(),
                femaleServiceRow.htsWereCondomsDistributed(),
                femaleServiceRow.htsNumberOfMaleCondomsProvided(),
                femaleServiceRow.htsNumberOfFemaleCondomsProvided(),
                femaleServiceRow.htsPreventiveServices(),
                femaleServiceRow.uniqueId(),
                femaleServiceRow.firstName(),
                femaleServiceRow.middleName(),
                femaleServiceRow.lastName(),
                femaleServiceRow.phoneNumber(),
                femaleServiceRow.nationalId(),
                femaleServiceRow.voterId(),
                femaleServiceRow.driverLicense(),
                femaleServiceRow.passport(),
                "Male",
                femaleServiceRow.birthDate(),
                femaleServiceRow.maritalStatus(),
                femaleServiceRow.pregnancyStatus(),
                femaleServiceRow.hfrCode(),
                femaleServiceRow.region(),
                femaleServiceRow.district(),
                femaleServiceRow.districtCouncil(),
                femaleServiceRow.ward(),
                femaleServiceRow.healthFacility(),
                femaleServiceRow.village(),
                femaleServiceRow.counsellorName()
        );

        Map<String, Object> femaleMapped = mapper.mapServiceRow(femaleServiceRow, List.of());
        Map<String, Object> maleMapped = mapper.mapServiceRow(maleServiceRow, List.of());

        Map<String, Object> femaleDemographics = (Map<String, Object>) femaleMapped.get("demographics");
        Map<String, Object> maleDemographics = (Map<String, Object>) maleMapped.get("demographics");

        assertEquals("UNKNOWN", femaleDemographics.get("pregnancyStatusCode"));
        assertEquals("NOT_APPLICABLE", maleDemographics.get("pregnancyStatusCode"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapPreviousTestClientTypeFromHtsPreviousHivstClientType() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");
        Map<String, String> expectedValues = new LinkedHashMap<>();
        expectedValues.put("  self  ", "SELF");
        expectedValues.put("SEXUAL_PARTNER", "SEXUAL_PARTNER");
        expectedValues.put("peer_friend", "PEER_FRIEND");

        for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
            OpenSrpIntegrationRepository.ServiceRow serviceRow = withPreviousHivstClientType(baseServiceRow, entry.getKey());
            Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of());
            Map<String, Object> clientClassification = (Map<String, Object>) mapped.get("clientClassification");

            assertEquals(entry.getValue(), clientClassification.get("previousTestClientType"));
            assertEquals("GENERAL_CLIENT", clientClassification.get("clientType"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldDefaultPreviousTestClientTypeForUnknownOrBlankValues() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");
        OpenSrpIntegrationRepository.ServiceRow unknownValue = withPreviousHivstClientType(baseServiceRow, "friend");
        OpenSrpIntegrationRepository.ServiceRow blankValue = withPreviousHivstClientType(baseServiceRow, "   ");

        Map<String, Object> unknownMapped = mapper.mapServiceRow(unknownValue, List.of());
        Map<String, Object> blankMapped = mapper.mapServiceRow(blankValue, List.of());

        Map<String, Object> unknownClientClassification = (Map<String, Object>) unknownMapped.get("clientClassification");
        Map<String, Object> blankClientClassification = (Map<String, Object>) blankMapped.get("clientClassification");

        assertEquals("NOT_APPLICABLE", unknownClientClassification.get("previousTestClientType"));
        assertEquals("NOT_APPLICABLE", blankClientClassification.get("previousTestClientType"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapTestingTypePreviousFromHtsPreviousHivstTestType() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");
        OpenSrpIntegrationRepository.ServiceRow serviceRow = new OpenSrpIntegrationRepository.ServiceRow(
                baseServiceRow.eventId(),
                baseServiceRow.baseEntityId(),
                baseServiceRow.htsVisitGroup(),
                baseServiceRow.visitDate(),
                baseServiceRow.htsVisitDate(),
                baseServiceRow.dateCreated(),
                baseServiceRow.providerId(),
                baseServiceRow.htsTestingApproach(),
                baseServiceRow.htsVisitType(),
                baseServiceRow.htsHasTheClientRecentlyTestedWithHivst(),
                baseServiceRow.htsPreviousHivstClientType(),
                "stb",
                baseServiceRow.htsPreviousHivstTestResults(),
                baseServiceRow.htsClientType(),
                baseServiceRow.htsTestingPoint(),
                baseServiceRow.htsTypeOfCounsellingProvided(),
                baseServiceRow.htsClientsTbScreeningOutcome(),
                baseServiceRow.htsHasPostTestCounsellingBeenProvided(),
                baseServiceRow.htsHivResultsDisclosure(),
                baseServiceRow.htsWereCondomsDistributed(),
                baseServiceRow.htsNumberOfMaleCondomsProvided(),
                baseServiceRow.htsNumberOfFemaleCondomsProvided(),
                baseServiceRow.htsPreventiveServices(),
                baseServiceRow.uniqueId(),
                baseServiceRow.firstName(),
                baseServiceRow.middleName(),
                baseServiceRow.lastName(),
                baseServiceRow.phoneNumber(),
                baseServiceRow.nationalId(),
                baseServiceRow.voterId(),
                baseServiceRow.driverLicense(),
                baseServiceRow.passport(),
                baseServiceRow.sex(),
                baseServiceRow.birthDate(),
                baseServiceRow.maritalStatus(),
                baseServiceRow.pregnancyStatus(),
                baseServiceRow.hfrCode(),
                baseServiceRow.region(),
                baseServiceRow.district(),
                baseServiceRow.districtCouncil(),
                baseServiceRow.ward(),
                baseServiceRow.healthFacility(),
                baseServiceRow.village(),
                baseServiceRow.counsellorName()
        );

        OpenSrpIntegrationRepository.TestRow testRow = new OpenSrpIntegrationRepository.TestRow(
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

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(testRow));
        Map<String, Object> testingHistory = (Map<String, Object>) mapped.get("testingHistory");

        assertEquals("SELF_TEST_BLOOD", testingHistory.get("testingTypePrevious"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void mapServiceRow_shouldMapPreviousTestResultFromHtsPreviousHivstTestResults() {
        OpenSrpIntegrationRepository.ServiceRow baseServiceRow = buildServiceRow("Single");
        OpenSrpIntegrationRepository.ServiceRow serviceRow = new OpenSrpIntegrationRepository.ServiceRow(
                baseServiceRow.eventId(),
                baseServiceRow.baseEntityId(),
                baseServiceRow.htsVisitGroup(),
                baseServiceRow.visitDate(),
                baseServiceRow.htsVisitDate(),
                baseServiceRow.dateCreated(),
                baseServiceRow.providerId(),
                baseServiceRow.htsTestingApproach(),
                baseServiceRow.htsVisitType(),
                baseServiceRow.htsHasTheClientRecentlyTestedWithHivst(),
                baseServiceRow.htsPreviousHivstClientType(),
                baseServiceRow.htsPreviousHivstTestType(),
                "non_reactive",
                baseServiceRow.htsClientType(),
                baseServiceRow.htsTestingPoint(),
                baseServiceRow.htsTypeOfCounsellingProvided(),
                baseServiceRow.htsClientsTbScreeningOutcome(),
                baseServiceRow.htsHasPostTestCounsellingBeenProvided(),
                baseServiceRow.htsHivResultsDisclosure(),
                baseServiceRow.htsWereCondomsDistributed(),
                baseServiceRow.htsNumberOfMaleCondomsProvided(),
                baseServiceRow.htsNumberOfFemaleCondomsProvided(),
                baseServiceRow.htsPreventiveServices(),
                baseServiceRow.uniqueId(),
                baseServiceRow.firstName(),
                baseServiceRow.middleName(),
                baseServiceRow.lastName(),
                baseServiceRow.phoneNumber(),
                baseServiceRow.nationalId(),
                baseServiceRow.voterId(),
                baseServiceRow.driverLicense(),
                baseServiceRow.passport(),
                baseServiceRow.sex(),
                baseServiceRow.birthDate(),
                baseServiceRow.maritalStatus(),
                baseServiceRow.pregnancyStatus(),
                baseServiceRow.hfrCode(),
                baseServiceRow.region(),
                baseServiceRow.district(),
                baseServiceRow.districtCouncil(),
                baseServiceRow.ward(),
                baseServiceRow.healthFacility(),
                baseServiceRow.village(),
                baseServiceRow.counsellorName()
        );

        OpenSrpIntegrationRepository.TestRow conflictingTestRow = new OpenSrpIntegrationRepository.TestRow(
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

        Map<String, Object> mapped = mapper.mapServiceRow(serviceRow, List.of(conflictingTestRow));
        Map<String, Object> testingHistory = (Map<String, Object>) mapped.get("testingHistory");

        assertEquals("NON_REACTIVE", testingHistory.get("previousTestResult"));
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
                "reactive",
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

    private OpenSrpIntegrationRepository.ServiceRow withIdentificationDocuments(
            OpenSrpIntegrationRepository.ServiceRow serviceRow,
            String nationalId,
            String voterId,
            String driverLicense,
            String passport
    ) {
        return new OpenSrpIntegrationRepository.ServiceRow(
                serviceRow.eventId(),
                serviceRow.baseEntityId(),
                serviceRow.htsVisitGroup(),
                serviceRow.visitDate(),
                serviceRow.htsVisitDate(),
                serviceRow.dateCreated(),
                serviceRow.providerId(),
                serviceRow.htsTestingApproach(),
                serviceRow.htsVisitType(),
                serviceRow.htsHasTheClientRecentlyTestedWithHivst(),
                serviceRow.htsPreviousHivstClientType(),
                serviceRow.htsPreviousHivstTestType(),
                serviceRow.htsPreviousHivstTestResults(),
                serviceRow.htsClientType(),
                serviceRow.htsTestingPoint(),
                serviceRow.htsTypeOfCounsellingProvided(),
                serviceRow.htsClientsTbScreeningOutcome(),
                serviceRow.htsHasPostTestCounsellingBeenProvided(),
                serviceRow.htsHivResultsDisclosure(),
                serviceRow.htsWereCondomsDistributed(),
                serviceRow.htsNumberOfMaleCondomsProvided(),
                serviceRow.htsNumberOfFemaleCondomsProvided(),
                serviceRow.htsPreventiveServices(),
                serviceRow.uniqueId(),
                serviceRow.firstName(),
                serviceRow.middleName(),
                serviceRow.lastName(),
                serviceRow.phoneNumber(),
                nationalId,
                voterId,
                driverLicense,
                passport,
                serviceRow.sex(),
                serviceRow.birthDate(),
                serviceRow.maritalStatus(),
                serviceRow.pregnancyStatus(),
                serviceRow.hfrCode(),
                serviceRow.region(),
                serviceRow.district(),
                serviceRow.districtCouncil(),
                serviceRow.ward(),
                serviceRow.healthFacility(),
                serviceRow.village(),
                serviceRow.counsellorName()
        );
    }

    private OpenSrpIntegrationRepository.ServiceRow withVisitDate(
            OpenSrpIntegrationRepository.ServiceRow serviceRow,
            String visitDate
    ) {
        return new OpenSrpIntegrationRepository.ServiceRow(
                serviceRow.eventId(),
                serviceRow.baseEntityId(),
                serviceRow.htsVisitGroup(),
                visitDate,
                serviceRow.htsVisitDate(),
                serviceRow.dateCreated(),
                serviceRow.providerId(),
                serviceRow.htsTestingApproach(),
                serviceRow.htsVisitType(),
                serviceRow.htsHasTheClientRecentlyTestedWithHivst(),
                serviceRow.htsPreviousHivstClientType(),
                serviceRow.htsPreviousHivstTestType(),
                serviceRow.htsPreviousHivstTestResults(),
                serviceRow.htsClientType(),
                serviceRow.htsTestingPoint(),
                serviceRow.htsTypeOfCounsellingProvided(),
                serviceRow.htsClientsTbScreeningOutcome(),
                serviceRow.htsHasPostTestCounsellingBeenProvided(),
                serviceRow.htsHivResultsDisclosure(),
                serviceRow.htsWereCondomsDistributed(),
                serviceRow.htsNumberOfMaleCondomsProvided(),
                serviceRow.htsNumberOfFemaleCondomsProvided(),
                serviceRow.htsPreventiveServices(),
                serviceRow.uniqueId(),
                serviceRow.firstName(),
                serviceRow.middleName(),
                serviceRow.lastName(),
                serviceRow.phoneNumber(),
                serviceRow.nationalId(),
                serviceRow.voterId(),
                serviceRow.driverLicense(),
                serviceRow.passport(),
                serviceRow.sex(),
                serviceRow.birthDate(),
                serviceRow.maritalStatus(),
                serviceRow.pregnancyStatus(),
                serviceRow.hfrCode(),
                serviceRow.region(),
                serviceRow.district(),
                serviceRow.districtCouncil(),
                serviceRow.ward(),
                serviceRow.healthFacility(),
                serviceRow.village(),
                serviceRow.counsellorName()
        );
    }

    private OpenSrpIntegrationRepository.ServiceRow withPreviousHivstClientType(
            OpenSrpIntegrationRepository.ServiceRow serviceRow,
            String previousHivstClientType
    ) {
        return new OpenSrpIntegrationRepository.ServiceRow(
                serviceRow.eventId(),
                serviceRow.baseEntityId(),
                serviceRow.htsVisitGroup(),
                serviceRow.visitDate(),
                serviceRow.htsVisitDate(),
                serviceRow.dateCreated(),
                serviceRow.providerId(),
                serviceRow.htsTestingApproach(),
                serviceRow.htsVisitType(),
                serviceRow.htsHasTheClientRecentlyTestedWithHivst(),
                previousHivstClientType,
                serviceRow.htsPreviousHivstTestType(),
                serviceRow.htsPreviousHivstTestResults(),
                serviceRow.htsClientType(),
                serviceRow.htsTestingPoint(),
                serviceRow.htsTypeOfCounsellingProvided(),
                serviceRow.htsClientsTbScreeningOutcome(),
                serviceRow.htsHasPostTestCounsellingBeenProvided(),
                serviceRow.htsHivResultsDisclosure(),
                serviceRow.htsWereCondomsDistributed(),
                serviceRow.htsNumberOfMaleCondomsProvided(),
                serviceRow.htsNumberOfFemaleCondomsProvided(),
                serviceRow.htsPreventiveServices(),
                serviceRow.uniqueId(),
                serviceRow.firstName(),
                serviceRow.middleName(),
                serviceRow.lastName(),
                serviceRow.phoneNumber(),
                serviceRow.nationalId(),
                serviceRow.voterId(),
                serviceRow.driverLicense(),
                serviceRow.passport(),
                serviceRow.sex(),
                serviceRow.birthDate(),
                serviceRow.maritalStatus(),
                serviceRow.pregnancyStatus(),
                serviceRow.hfrCode(),
                serviceRow.region(),
                serviceRow.district(),
                serviceRow.districtCouncil(),
                serviceRow.ward(),
                serviceRow.healthFacility(),
                serviceRow.village(),
                serviceRow.counsellorName()
        );
    }

    private OpenSrpIntegrationRepository.ServiceRow withResidenceCodes(
            OpenSrpIntegrationRepository.ServiceRow serviceRow,
            String householdVillageCode,
            String providerCouncilCode
    ) {
        return new OpenSrpIntegrationRepository.ServiceRow(
                serviceRow.eventId(),
                serviceRow.baseEntityId(),
                serviceRow.htsVisitGroup(),
                serviceRow.visitDate(),
                serviceRow.htsVisitDate(),
                serviceRow.dateCreated(),
                serviceRow.providerId(),
                serviceRow.htsTestingApproach(),
                serviceRow.htsVisitType(),
                serviceRow.htsHasTheClientRecentlyTestedWithHivst(),
                serviceRow.htsPreviousHivstClientType(),
                serviceRow.htsPreviousHivstTestType(),
                serviceRow.htsPreviousHivstTestResults(),
                serviceRow.htsClientType(),
                serviceRow.htsTestingPoint(),
                serviceRow.htsTypeOfCounsellingProvided(),
                serviceRow.htsClientsTbScreeningOutcome(),
                serviceRow.htsHasPostTestCounsellingBeenProvided(),
                serviceRow.htsHivResultsDisclosure(),
                serviceRow.htsWereCondomsDistributed(),
                serviceRow.htsNumberOfMaleCondomsProvided(),
                serviceRow.htsNumberOfFemaleCondomsProvided(),
                serviceRow.htsPreventiveServices(),
                serviceRow.uniqueId(),
                serviceRow.firstName(),
                serviceRow.middleName(),
                serviceRow.lastName(),
                serviceRow.phoneNumber(),
                serviceRow.nationalId(),
                serviceRow.voterId(),
                serviceRow.driverLicense(),
                serviceRow.passport(),
                serviceRow.sex(),
                serviceRow.birthDate(),
                serviceRow.maritalStatus(),
                serviceRow.pregnancyStatus(),
                serviceRow.hfrCode(),
                serviceRow.region(),
                serviceRow.district(),
                providerCouncilCode,
                serviceRow.ward(),
                householdVillageCode,
                serviceRow.village(),
                serviceRow.counsellorName()
        );
    }
}

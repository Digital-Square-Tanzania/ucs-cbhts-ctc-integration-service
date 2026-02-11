package com.abt.integration.mapping;

import com.abt.integration.db.OpenSrpIntegrationRepository;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class IntegrationDataMapper {

    private static final String DEFAULT_NOT_APPLICABLE_VALUE = "NOT_APPLICABLE";

    private static final Map<String, String> MARITAL_ALIASES = aliases(
            "single", "SINGLE",
            "never_married", "SINGLE",
            "married", "MARRIED_MONOGAMOUS",
            "polygamous", "MARRIED_POLYGAMOUS",
            "cohabiting", "COHABITING",
            "cohabitation", "COHABITING",
            "living_with_partner", "COHABITING",
            "divorced", "SEPARATED_DIVORCED",
            "separated", "SEPARATED_DIVORCED",
            "widowed", "WIDOWED",
            "not_applicable", "NOT_APPLICABLE"
    );

    private static final Map<String, String> PREGNANCY_ALIASES = aliases(
            "yes", "PREGNANT",
            "pregnant", "PREGNANT",
            "no", "NOT_PREGNANT",
            "not_pregnant", "NOT_PREGNANT",
            "breastfeeding", "BREASTFEEDING",
            "unknown", "UNKNOWN",
            "not_applicable", "NOT_APPLICABLE"
    );

    private static final Map<String, String> CLIENT_TYPE_ALIASES = aliases(
            "normal_client", "GENERAL_CLIENT",
            "general_client", "GENERAL_CLIENT",
            "index_contact", "INDEX_CONTACT",
            "social_network_contact", "SOCIAL_NETWORK_CONTACT",
            "verification", "VERIFICATION_CLIENT",
            "verification_client", "VERIFICATION_CLIENT"
    );

    private static final Map<String, String> ATTENDANCE_ALIASES = aliases(
            "new_client", "NEW_CLIENT",
            "returning", "REPEAT_VISIT",
            "repeat_visit", "REPEAT_VISIT",
            "verification", "VERIFICATION_VISIT",
            "verification_visit", "VERIFICATION_VISIT"
    );

    private static final Map<String, String> PREVIOUS_CLIENT_TYPE_ALIASES = aliases(
            "self", "SELF",
            "sexual_partner", "SEXUAL_PARTNER",
            "peer_friend", "PEER_FRIEND"
    );

    private static final Map<String, String> TESTING_TYPE_ALIASES = aliases(
            "sto", "SELF_TEST_ORAL",
            "stb", "SELF_TEST_BLOOD",
            "st", "SELF_TEST",
            "initial_test", "INITIAL_TEST",
            "verification_test", "VERIFICATION_TEST",
            "self_test_oral", "SELF_TEST_ORAL",
            "self_test_blood", "SELF_TEST_BLOOD"
    );

    private static final Map<String, String> COUNSELLING_TYPE_ALIASES = aliases(
            "individual", "INDIVIDUAL",
            "couple", "COUPLE",
            "group", "GROUP",
            "with_parent_or_guardian", "CLIENT_WITH_GUARDIAN",
            "family", "FAMILY",
            "assisted_self_testing", "ASSISTED_SELF_TESTING",
            "unassisted_self_testing", "UNASSISTED_SELF_TESTING",
            "onsite_assisted_self_testing", "ONSITE_ASSISTED_SELF_TESTING",
            "onsite_unassisted_self_testing", "ONSITE_UNASSISTED_SELF_TESTING",
            "offsite_assisted_self_testing", "OFFSITE_ASSISTED_SELF_TESTING",
            "offsite_unassisted_self_testing", "OFFSITE_UNASSISTED_SELF_TESTING"
    );

    private static final Map<String, String> TB_SCREENING_ALIASES = aliases(
            "tb_suspect", "TB_PRESUMPTIVE",
            "screened_negative", "TB_NO_SYMPTOMS",
            "on_tb_treatment", "TB_CONFIRMED",
            "not_screened", "NOT_SCREENED"
    );

    private static final Map<String, String> DISCLOSURE_ALIASES = aliases(
            "husband", "SPOUSE",
            "wife", "SPOUSE",
            "partner_who_is_not_wife_or_husband", "NON_SPOUSE_PARTNER",
            "relative", "FAMILY_MEMBER",
            "friend", "FRIEND",
            "religious_leader", "RELIGIOUS_LEADER",
            "others", "OTHER",
            "not_ready_to_share", "NOT_READY_TO_DISCLOSE",
            "parent_guardian", "PARENT_GUARDIAN"
    );

    private static final Map<String, String> REFERRED_TO_ALIASES = aliases(
            "prep_services", "PREP_SERVICE",
            "gbv_services", "GBV_SERVICE",
            "none", "NOT_APPLICABLE",
            "others", "OTHER_SERVICE",
            "care_and_treatment_clinic", "CTC_CLINIC",
            "tuberculosis_clinic", "TB_CLINIC",
            "prevention_of_mother_to_child_transmission", "PMTCT_SERVICE",
            "sexual_transmitted_infections_clinic", "STI_CLINIC",
            "sputum_testing_laboratory", "LABORATORY_SERVICE",
            "pep_services", "PEP_SERVICE",
            "voluntary_medical_male_circumcision_vmmc", "VMMC_SERVICE",
            "family_planning", "FAMILY_PLANNING_CLINIC",
            "adolescent_and_youth_people_friendly_services", "YOUTH_FRIENDLY_SERVICE"
    );

    private static final Map<String, String> HIV_RESULT_ALIASES = aliases(
            "reactive", "REACTIVE",
            "non_reactive", "NON_REACTIVE",
            "invalid", "INVALID",
            "wastage", "WASTAGE",
            "positive", "REACTIVE",
            "negative", "NON_REACTIVE"
    );

    private static final Set<String> SELF_TEST_MARKERS = Set.of("SELF", "HIVST", "STO", "STB", "SELF_TEST");

    private final MappingReferenceCatalog catalog;

    public IntegrationDataMapper() {
        this(new MappingReferenceCatalog());
    }

    public IntegrationDataMapper(MappingReferenceCatalog catalog) {
        this.catalog = catalog;
    }

    public Map<String, Object> mapServiceRow(OpenSrpIntegrationRepository.ServiceRow serviceRow,
                                             List<OpenSrpIntegrationRepository.TestRow> testRows) {
        List<OpenSrpIntegrationRepository.TestRow> safeTestRows = testRows == null ? List.of() : testRows;

        Map<String, Object> item = new LinkedHashMap<>();
        item.put("htcApproach", mapTestingApproach(serviceRow.htsTestingApproach()));
        item.put("visitDate", normalizeDate(firstNonBlank(serviceRow.htsVisitDate(), serviceRow.visitDate())));
        item.put("counsellor", mapCounsellor(serviceRow));
        item.put("clientCode", firstNonBlank(serviceRow.uniqueId(), serviceRow.baseEntityId()));
        item.put("cellPhoneNumber", normalizePhoneNumber(serviceRow.phoneNumber()));

        item.put("clientIdentification", mapClientIdentification(serviceRow));
        item.put("clientName", mapClientName(serviceRow));
        item.put("demographics", mapDemographics(serviceRow));
        item.put("residence", mapResidence(serviceRow));
        item.put("clientClassification", mapClientClassification(serviceRow));
        item.put("testingHistory", mapTestingHistory(serviceRow));
        item.put("currentTesting", mapCurrentTesting(serviceRow));
        item.put("selfTesting", mapSelfTesting(safeTestRows));
        item.put("reagentTesting", mapReagentTesting(safeTestRows));
        item.put("preventionServices", mapPreventionServices(serviceRow));
        item.put("referralAndOutcome", mapReferralAndOutcome(serviceRow));
        item.put("remarks", "Generated from cbhts_services event " + serviceRow.eventId());
        item.put("createdAt", toEpochSeconds(serviceRow.dateCreated()));

        return item;
    }

    private Map<String, Object> mapCounsellor(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> counsellor = new LinkedHashMap<>();
        counsellor.put("counsellorID", serviceRow.providerId());
        counsellor.put("counsellorName", firstNonBlank(serviceRow.counsellorName(), serviceRow.providerId()));
        return counsellor;
    }

    private String mapTestingApproach(String rawApproach) {
        if (isBlank(rawApproach)) {
            return "CBHTS";
        }

        String normalized = MappingReferenceCatalog.normalize(rawApproach);
        if (catalog.isKnownFormOption("hts_testing_approach", rawApproach)) {
            return normalized;
        }

        return normalized;
    }

    private Map<String, Object> mapClientIdentification(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> clientIdentification = new LinkedHashMap<>();

        String idType;
        String idValue;

        if (hasText(serviceRow.nationalId())) {
            idType = "NIDA";
            idValue = serviceRow.nationalId();
        } else if (hasText(serviceRow.voterId())) {
            idType = "VOTER_ID";
            idValue = serviceRow.voterId();
        } else if (hasText(serviceRow.driverLicense())) {
            idType = "DRIVER_LICENSE";
            idValue = serviceRow.driverLicense();
        } else if (hasText(serviceRow.passport())) {
            idType = "PASSPORT";
            idValue = serviceRow.passport();
        } else {
            idType = "OPENSRP_ID";
            idValue = serviceRow.baseEntityId();
        }

        clientIdentification.put("clientUniqueIdentifierType", idType);
        clientIdentification.put("clientUniqueIdentifierCode", idValue);

        return clientIdentification;
    }

    private Map<String, Object> mapClientName(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> clientName = new LinkedHashMap<>();
        clientName.put("firstName", serviceRow.firstName());
        clientName.put("middleName", serviceRow.middleName());
        clientName.put("lastName", serviceRow.lastName());
        return clientName;
    }

    private Map<String, Object> mapDemographics(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> demographics = new LinkedHashMap<>();

        demographics.put("sexCode", serviceRow.sex());
        demographics.put("dateOfBirth", normalizeDate(serviceRow.birthDate()));
        demographics.put("maritalStatusCode", catalog.mapToIntegrationValue("MaritalStatusCode", serviceRow.maritalStatus(), MARITAL_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE));
        demographics.put("pregnancyStatusCode", mapPregnancyStatusBySex(serviceRow.sex()));
        demographics.put("smsConsent", hasText(serviceRow.phoneNumber()));

        return demographics;
    }

    private String mapPregnancyStatusBySex(String sex) {
        if (isMaleSex(sex)) {
            return "NOT_APPLICABLE";
        }
        return "UNKNOWN";
    }

    private boolean isMaleSex(String sex) {
        String normalized = MappingReferenceCatalog.normalize(sex);
        return "MALE".equals(normalized) || "M".equals(normalized);
    }

    private Map<String, Object> mapResidence(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> residence = new LinkedHashMap<>();
        residence.put("region", serviceRow.region());
        residence.put("district", serviceRow.district());
        residence.put("council", serviceRow.districtCouncil());
        residence.put("ward", serviceRow.ward());
        residence.put("villageStreet", firstNonBlank(serviceRow.healthFacility(), serviceRow.village()));
        residence.put("hamlet", serviceRow.village());
        return residence;
    }

    private Map<String, Object> mapClientClassification(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> clientClassification = new LinkedHashMap<>();

        clientClassification.put("previousTestClientType", catalog.mapToIntegrationValue(
                "PreviousTestClientType",
                serviceRow.htsPreviousHivstClientType(),
                PREVIOUS_CLIENT_TYPE_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE));

        String clientTypeValue = catalog.mapToIntegrationValue("ClientType", serviceRow.htsClientType(), CLIENT_TYPE_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE);
        clientClassification.put("clientType", clientTypeValue);

        clientClassification.put("attendanceCode", catalog.mapToIntegrationValue(
                "AttendanceCode",
                serviceRow.htsVisitType(),
                ATTENDANCE_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE));

        clientClassification.put("relationshipIndexClient",
                "INDEX_CONTACT".equals(clientTypeValue) ? "SEXUAL_PARTNER" : DEFAULT_NOT_APPLICABLE_VALUE);

        boolean recentlySelfTested = toBoolean(serviceRow.htsHasTheClientRecentlyTestedWithHivst());
        clientClassification.put("eligibleForTesting", !recentlySelfTested);

        return clientClassification;
    }

    private Map<String, Object> mapTestingHistory(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> testingHistory = new LinkedHashMap<>();

        testingHistory.put("testingTypePrevious", catalog.mapToIntegrationValue(
                "TestingTypePrevious",
                serviceRow.htsPreviousHivstTestType(),
                TESTING_TYPE_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE));

        String previousResultCode = catalog.mapToIntegrationValue(
                "PreviousTestResult",
                serviceRow.htsPreviousHivstTestResults(),
                HIV_RESULT_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE
        );

        testingHistory.put("previousTestResult", previousResultCode);
        return testingHistory;
    }

    private Map<String, Object> mapCurrentTesting(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> currentTesting = new LinkedHashMap<>();

        String testingTypeSource = serviceRow.htsClientType();
        if (hasText(testingTypeSource) && MappingReferenceCatalog.normalize(testingTypeSource).contains("VERIFICATION")) {
            testingTypeSource = "verification_test";
        } else {
            testingTypeSource = "initial_test";
        }

        currentTesting.put("testingType", catalog.mapToIntegrationValue("TestingType", testingTypeSource, TESTING_TYPE_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE));

        currentTesting.put("counsellingTypeCode", catalog.mapToIntegrationValue(
                "CounsellingTypeCode",
                serviceRow.htsTypeOfCounsellingProvided(),
                COUNSELLING_TYPE_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE));

        currentTesting.put("tbScreeningDetails", catalog.mapToIntegrationValue(
                "TBScreeningDetails",
                serviceRow.htsClientsTbScreeningOutcome(),
                TB_SCREENING_ALIASES,
                DEFAULT_NOT_APPLICABLE_VALUE));

        currentTesting.put("postTestCounsellingAndResultsGiven", toBoolean(serviceRow.htsHasPostTestCounsellingBeenProvided()));

        List<Map<String, Object>> disclosures = new ArrayList<>();
        for (String disclosure : splitValues(serviceRow.htsHivResultsDisclosure())) {
            String code = catalog.mapToIntegrationValue("PostTestCounsellingAndResultsGiven", disclosure, DISCLOSURE_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE);
            if (code != null) {
                Map<String, Object> disclosureItem = new LinkedHashMap<>();
                disclosureItem.put("disclosureCode", code);
                disclosures.add(disclosureItem);
            }
        }

        currentTesting.put("disclosure", disclosures);
        return currentTesting;
    }

    private List<Map<String, Object>> mapSelfTesting(List<OpenSrpIntegrationRepository.TestRow> tests) {
        List<Map<String, Object>> selfTesting = new ArrayList<>();

        for (OpenSrpIntegrationRepository.TestRow test : tests) {
            if (!isSelfTest(test)) {
                continue;
            }

            Map<String, Object> testItem = new LinkedHashMap<>();
            testItem.put("selfTestKitCode", mapKitName(test.typeOfTestKitUsed()));
            testItem.put("selfTestBatchNo", test.testKitBatchNumber());
            testItem.put("selfTestExpiryDate", normalizeDate(test.testKitExpireDate()));
            testItem.put("selfTestKitName", prettifyName(test.typeOfTestKitUsed()));
            testItem.put("selfTestingResults", catalog.mapToIntegrationValue("PreviousTestResult", test.testResult(), HIV_RESULT_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE));
            selfTesting.add(testItem);
        }

        return selfTesting;
    }

    private List<Map<String, Object>> mapReagentTesting(List<OpenSrpIntegrationRepository.TestRow> tests) {
        List<Map<String, Object>> reagentTesting = new ArrayList<>();

        for (OpenSrpIntegrationRepository.TestRow test : tests) {
            if (isSelfTest(test)) {
                continue;
            }

            Map<String, Object> testItem = new LinkedHashMap<>();
            testItem.put("reagentBatch", test.testKitBatchNumber());
            testItem.put("reagentExpiry", normalizeDate(test.testKitExpireDate()));
            testItem.put("reagentTest", mapKitName(test.typeOfTestKitUsed()));
            testItem.put("testType", mapTestType(test.testType()));
            testItem.put("reagentResult", catalog.mapToIntegrationValue("ReagentResultFirst", test.testResult(), HIV_RESULT_ALIASES, DEFAULT_NOT_APPLICABLE_VALUE));
            testItem.put("syphilisResult", mapSyphilisResult(test.syphilisTestResults()));
            reagentTesting.add(testItem);
        }

        return reagentTesting;
    }

    private Map<String, Object> mapPreventionServices(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        Map<String, Object> preventionServices = new LinkedHashMap<>();

        preventionServices.put("condomGiven", toBoolean(serviceRow.htsWereCondomsDistributed()));
        preventionServices.put("condomsIssuedMale", serviceRow.htsNumberOfMaleCondomsProvided());
        preventionServices.put("condomsIssuedFemale", serviceRow.htsNumberOfFemaleCondomsProvided());

        return preventionServices;
    }

    private List<Map<String, Object>> mapReferralAndOutcome(OpenSrpIntegrationRepository.ServiceRow serviceRow) {
        List<Map<String, Object>> referralAndOutcome = new ArrayList<>();

        String referredToCode = DEFAULT_NOT_APPLICABLE_VALUE;
        for (String preventiveService : splitValues(serviceRow.htsPreventiveServices())) {
            referredToCode = catalog.mapToIntegrationValue("ReferredToCode", preventiveService, REFERRED_TO_ALIASES, null);
            if (referredToCode != null) {
                break;
            }
        }

        if (referredToCode == null) {
            referredToCode = DEFAULT_NOT_APPLICABLE_VALUE;
        }

        Map<String, Object> referralItem = new LinkedHashMap<>();
        referralItem.put("referredToCode", referredToCode);
        referralItem.put("toFacility", serviceRow.hfrCode());
        referralAndOutcome.add(referralItem);

        return referralAndOutcome;
    }

    private String mapKitName(String rawValue) {
        if (isBlank(rawValue)) {
            return null;
        }

        String normalized = MappingReferenceCatalog.normalize(rawValue);
        return switch (normalized) {
            case "MULTITEST", "MULTI_TEST", "DUAL", "HIV_SYPHILIS_DUAL", "H_S_DUO" -> "DUAL";
            case "BIOLINE", "SD_BIOLINE" -> "SD_BIOLINE";
            case "UNIGOLD" -> "UNIGOLD";
            default -> normalized;
        };
    }

    private String mapTestType(String rawType) {
        if (isBlank(rawType)) {
            return null;
        }

        String normalized = MappingReferenceCatalog.normalize(rawType);
        return switch (normalized) {
            case "FIRST", "FIRST_HIV_TEST", "FIRST_TEST" -> "FIRST";
            case "SECOND", "SECOND_HIV_TEST", "SECOND_TEST" -> "SECOND";
            case "UNIGOLD", "UNIGOLD_HIV_TEST" -> "UNIGOLD";
            default -> normalized;
        };
    }

    private String mapSyphilisResult(String rawResult) {
        if (isBlank(rawResult)) {
            return null;
        }

        String normalized = MappingReferenceCatalog.normalize(rawResult);
        if (normalized.contains("POSITIVE") || "R".equals(normalized)) {
            return "POSITIVE";
        }
        if (normalized.contains("NEGATIVE") || "NR".equals(normalized)) {
            return "NEGATIVE";
        }

        return normalized;
    }

    private boolean isSelfTest(OpenSrpIntegrationRepository.TestRow test) {
        String testType = MappingReferenceCatalog.normalize(test.testType());
        String kitType = MappingReferenceCatalog.normalize(test.typeOfTestKitUsed());

        if (SELF_TEST_MARKERS.stream().anyMatch(marker -> testType.contains(marker))) {
            return true;
        }

        return kitType.contains("SELF");
    }

    private String normalizePhoneNumber(String phoneNumber) {
        if (isBlank(phoneNumber)) {
            return null;
        }

        String value = phoneNumber.trim().replaceAll("\\s+", "");
        if (value.startsWith("+")) {
            value = value.substring(1);
        }
        if (value.startsWith("0") && value.length() >= 10) {
            return "255" + value.substring(1);
        }

        return value;
    }

    private String normalizeDate(String rawValue) {
        if (isBlank(rawValue)) {
            return null;
        }

        String trimmed = rawValue.trim();

        if (trimmed.matches("^-?\\d+$")) {
            long epoch = Long.parseLong(trimmed);
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(toEpochSeconds(epoch)), ZoneOffset.UTC)
                    .toLocalDate()
                    .toString();
        }

        List<DateTimeFormatter> localDateFormatters = List.of(
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("dd-MM-yyyy")
        );

        for (DateTimeFormatter formatter : localDateFormatters) {
            try {
                LocalDate date = LocalDate.parse(trimmed, formatter);
                return date.toString();
            } catch (DateTimeParseException ignored) {
            }
        }

        List<DateTimeFormatter> localDateTimeFormatters = List.of(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        );

        for (DateTimeFormatter formatter : localDateTimeFormatters) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(trimmed, formatter);
                return dateTime.toLocalDate().toString();
            } catch (DateTimeParseException ignored) {
            }
        }

        List<DateTimeFormatter> offsetFormatters = List.of(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME,
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
        );

        for (DateTimeFormatter formatter : offsetFormatters) {
            try {
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(trimmed, formatter);
                return offsetDateTime.toLocalDate().toString();
            } catch (DateTimeParseException ignored) {
            }
        }

        try {
            return OffsetDateTime.parse(trimmed).toLocalDate().toString();
        } catch (DateTimeParseException ignored) {
        }

        return trimmed;
    }

    private long toEpochSeconds(long rawTimestamp) {
        if (Math.abs(rawTimestamp) > 9_999_999_999L) {
            return rawTimestamp / 1000L;
        }
        return rawTimestamp;
    }

    private boolean toBoolean(String rawValue) {
        if (!hasText(rawValue)) {
            return false;
        }

        String normalized = MappingReferenceCatalog.normalize(rawValue);

        if (normalized.equals("YES") || normalized.equals("TRUE") || normalized.equals("1") || normalized.equals("N")) {
            return true;
        }

        if (normalized.equals("NO") || normalized.equals("FALSE") || normalized.equals("0") || normalized.equals("H")) {
            return false;
        }

        return catalog.isKnownFormOption("hts_has_post_test_counselling_been_provided", rawValue)
                && normalized.equals("YES");
    }

    private List<String> splitValues(String rawValue) {
        if (isBlank(rawValue)) {
            return List.of();
        }

        TreeSet<String> values = new TreeSet<>();
        Arrays.stream(rawValue.split(","))
                .map(String::trim)
                .filter(this::hasText)
                .forEach(values::add);

        return new ArrayList<>(values);
    }

    private String prettifyName(String rawValue) {
        if (isBlank(rawValue)) {
            return null;
        }

        String normalized = MappingReferenceCatalog.normalize(rawValue).toLowerCase(Locale.ROOT);
        String[] parts = normalized.split("_");
        List<String> titled = new ArrayList<>();
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            titled.add(part.substring(0, 1).toUpperCase(Locale.ROOT) + part.substring(1));
        }

        return String.join(" ", titled);
    }

    private static Map<String, String> aliases(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("Alias pairs should have even length");
        }

        Map<String, String> aliases = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            aliases.put(MappingReferenceCatalog.normalize(pairs[i]), pairs[i + 1]);
        }
        return aliases;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }

        return Arrays.stream(values)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .findFirst()
                .orElse(null);
    }

    private boolean hasText(String value) {
        return !isBlank(value);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

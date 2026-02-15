package com.abt.integration.db;

import com.abt.integration.model.IntegrationRequest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

public class OpenSrpIntegrationRepository {

    private final String schema;

    public OpenSrpIntegrationRepository(String schema) {
        this.schema = schema;
    }

    public long countServices(Connection connection, IntegrationRequest request) throws SQLException {
        String sql = "SELECT COUNT(*) " +
                "FROM " + schema + ".cbhts_services s " +
                "JOIN " + schema + ".team_members tm ON tm.identifier = s.provider_id " +
                "JOIN " + schema + ".tanzania_locations l ON l.location_uuid = tm.location_uuid " +
                "WHERE l.hfr_code = ? " +
                "AND ((s.date_created BETWEEN ? AND ?) OR (s.date_created BETWEEN ? AND ?))";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long startSec = request.getStartDate();
            long endSec = request.getEndDate();
            long startMs = secondsToMillis(startSec);
            long endMs = secondsToMillis(endSec);

            statement.setString(1, request.getHfrCode());
            statement.setLong(2, startSec);
            statement.setLong(3, endSec);
            statement.setLong(4, startMs);
            statement.setLong(5, endMs);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                return 0L;
            }
        }
    }

    public Optional<VerificationServiceMetadataRow> findLatestServiceMetadataByClientCode(Connection connection,
                                                                                           String hfrCode,
                                                                                           String clientCode) throws SQLException {
        String sql = "SELECT s.base_entity_id, s.provider_id, s.team, s.team_id, s.location_id, s.entity_type " +
                "FROM " + schema + ".cbhts_services s " +
                "JOIN " + schema + ".client c ON c.base_entity_id = s.base_entity_id " +
                "JOIN " + schema + ".team_members tm ON tm.identifier = s.provider_id " +
                "JOIN " + schema + ".tanzania_locations l ON l.location_uuid = tm.location_uuid " +
                "WHERE c.unique_id = ? " +
                "AND l.hfr_code = ? " +
                "ORDER BY s.date_created DESC NULLS LAST, s.event_id DESC " +
                "LIMIT 1";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, clientCode);
            statement.setString(2, hfrCode);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                return Optional.of(new VerificationServiceMetadataRow(
                        resultSet.getString("base_entity_id"),
                        resultSet.getString("provider_id"),
                        resultSet.getString("team"),
                        resultSet.getString("team_id"),
                        resultSet.getString("location_id"),
                        resultSet.getString("entity_type")
                ));
            }
        }
    }

    public List<ServiceRow> findServices(Connection connection, IntegrationRequest request) throws SQLException {
        String sql = "SELECT " +
                "s.event_id, s.base_entity_id, s.hts_visit_group, s.visit_date, s.hts_visit_date, s.date_created, " +
                "tm.identifier AS provider_id, s.hts_testing_approach, s.hts_visit_type, s.hts_has_the_client_recently_tested_with_hivst, " +
                "s.hts_previous_hivst_client_type, s.hts_previous_hivst_test_type, s.hts_previous_hivst_test_results, s.hts_client_type, s.hts_testing_point, " +
                "s.hts_type_of_counselling_provided, s.hts_clients_tb_screening_outcome, s.hts_has_post_test_counselling_been_provided, " +
                "s.hts_hiv_results_disclosure, s.hts_were_condoms_distributed, s.hts_number_of_male_condoms_provided, " +
                "s.hts_number_of_female_condoms_provided, s.hts_preventive_services, s.final_hiv_test_result, " +
                "c.unique_id, c.first_name, c.middle_name, c.last_name, c.phone_number, c.national_id, c.voter_id, c.driver_license, " +
                "c.passport, c.sex, c.birth_date, c.marital_status, c.preg_1yr, " +
                "l.hfr_code, l.region, l.district, l.council_code AS provider_council_code, l.ward, " +
                "hl.village_code AS household_village_code, l.village, " +
                "COALESCE(tm.name, tm.identifier) AS counsellor_name " +
                "FROM " + schema + ".cbhts_services s " +
                "JOIN " + schema + ".team_members tm ON tm.identifier = s.provider_id " +
                "JOIN " + schema + ".tanzania_locations l ON l.location_uuid = tm.location_uuid " +
                "LEFT JOIN " + schema + ".household h ON h.primary_caregiver = s.base_entity_id " +
                "LEFT JOIN " + schema + ".tanzania_locations hl ON hl.location_uuid = NULLIF(TRIM(h.location_id), '') " +
                "LEFT JOIN " + schema + ".client c ON c.base_entity_id = s.base_entity_id " +
                "WHERE l.hfr_code = ? " +
                "AND ((s.date_created BETWEEN ? AND ?) OR (s.date_created BETWEEN ? AND ?)) " +
                "ORDER BY s.date_created ASC, s.event_id ASC " +
                "LIMIT ? OFFSET ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long startSec = request.getStartDate();
            long endSec = request.getEndDate();
            long startMs = secondsToMillis(startSec);
            long endMs = secondsToMillis(endSec);
            int offset = (request.getPageIndex() - 1) * request.getPageSize();

            statement.setString(1, request.getHfrCode());
            statement.setLong(2, startSec);
            statement.setLong(3, endSec);
            statement.setLong(4, startMs);
            statement.setLong(5, endMs);
            statement.setInt(6, request.getPageSize());
            statement.setInt(7, offset);

            List<ServiceRow> rows = new ArrayList<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    rows.add(new ServiceRow(
                            resultSet.getString("event_id"),
                            resultSet.getString("base_entity_id"),
                            resultSet.getString("hts_visit_group"),
                            resultSet.getString("visit_date"),
                            resultSet.getString("hts_visit_date"),
                            resultSet.getLong("date_created"),
                            resultSet.getString("provider_id"),
                            resultSet.getString("hts_testing_approach"),
                            resultSet.getString("hts_visit_type"),
                            resultSet.getString("hts_has_the_client_recently_tested_with_hivst"),
                            resultSet.getString("hts_previous_hivst_client_type"),
                            resultSet.getString("hts_previous_hivst_test_type"),
                            resultSet.getString("hts_previous_hivst_test_results"),
                            resultSet.getString("hts_client_type"),
                            resultSet.getString("hts_testing_point"),
                            resultSet.getString("hts_type_of_counselling_provided"),
                            resultSet.getString("hts_clients_tb_screening_outcome"),
                            resultSet.getString("hts_has_post_test_counselling_been_provided"),
                            resultSet.getString("hts_hiv_results_disclosure"),
                            resultSet.getString("hts_were_condoms_distributed"),
                            getNullableInteger(resultSet, "hts_number_of_male_condoms_provided"),
                            getNullableInteger(resultSet, "hts_number_of_female_condoms_provided"),
                            resultSet.getString("hts_preventive_services"),
                            resultSet.getString("final_hiv_test_result"),
                            resultSet.getString("unique_id"),
                            resultSet.getString("first_name"),
                            resultSet.getString("middle_name"),
                            resultSet.getString("last_name"),
                            resultSet.getString("phone_number"),
                            resultSet.getString("national_id"),
                            resultSet.getString("voter_id"),
                            resultSet.getString("driver_license"),
                            resultSet.getString("passport"),
                            resultSet.getString("sex"),
                            resultSet.getString("birth_date"),
                            resultSet.getString("marital_status"),
                            resultSet.getString("preg_1yr"),
                            resultSet.getString("hfr_code"),
                            resultSet.getString("region"),
                            resultSet.getString("district"),
                            resultSet.getString("provider_council_code"),
                            resultSet.getString("ward"),
                            resultSet.getString("household_village_code"),
                            resultSet.getString("village"),
                            resultSet.getString("counsellor_name")
                    ));
                }
            }
            return rows;
        }
    }

    public Map<String, List<TestRow>> findTestsForServices(Connection connection,
                                                           List<ServiceRow> serviceRows,
                                                           long startDate,
                                                           long endDate) throws SQLException {
        Set<String> visitGroups = new HashSet<>();
        Set<String> baseEntityIds = new HashSet<>();

        for (ServiceRow serviceRow : serviceRows) {
            if (hasText(serviceRow.htsVisitGroup())) {
                visitGroups.add(serviceRow.htsVisitGroup());
            }
            if (hasText(serviceRow.baseEntityId())) {
                baseEntityIds.add(serviceRow.baseEntityId());
            }
        }

        if (visitGroups.isEmpty() && baseEntityIds.isEmpty()) {
            return Map.of();
        }

        StringBuilder query = new StringBuilder();
        query.append("SELECT t.event_id, t.hts_visit_group, t.base_entity_id, t.type_of_test_kit_used, t.test_kit_batch_number, ")
                .append("t.test_kit_expire_date, t.test_result, t.syphilis_test_results, t.test_type, t.date_created ")
                .append("FROM ").append(schema).append(".cbhts_tests t ")
                .append("WHERE ((t.date_created BETWEEN ? AND ?) OR (t.date_created BETWEEN ? AND ?)) AND (");

        if (!visitGroups.isEmpty()) {
            query.append("t.hts_visit_group IN (").append(placeholders(visitGroups.size())).append(")");
        }

        if (!visitGroups.isEmpty() && !baseEntityIds.isEmpty()) {
            query.append(" OR ");
        }

        if (!baseEntityIds.isEmpty()) {
            query.append("(t.hts_visit_group IS NULL AND t.base_entity_id IN (")
                    .append(placeholders(baseEntityIds.size()))
                    .append("))");
        }

        query.append(") ORDER BY t.date_created ASC, t.event_id ASC");

        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            int index = 1;
            long startMs = secondsToMillis(startDate);
            long endMs = secondsToMillis(endDate);

            statement.setLong(index++, startDate);
            statement.setLong(index++, endDate);
            statement.setLong(index++, startMs);
            statement.setLong(index++, endMs);

            for (String visitGroup : visitGroups) {
                statement.setString(index++, visitGroup);
            }
            for (String baseEntityId : baseEntityIds) {
                statement.setString(index++, baseEntityId);
            }

            Map<String, List<TestRow>> groupedTests = new HashMap<>();

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    TestRow testRow = new TestRow(
                            resultSet.getString("event_id"),
                            resultSet.getString("hts_visit_group"),
                            resultSet.getString("base_entity_id"),
                            resultSet.getString("type_of_test_kit_used"),
                            resultSet.getString("test_kit_batch_number"),
                            resultSet.getString("test_kit_expire_date"),
                            resultSet.getString("test_result"),
                            resultSet.getString("syphilis_test_results"),
                            resultSet.getString("test_type"),
                            resultSet.getLong("date_created")
                    );

                    groupedTests.computeIfAbsent(testKey(testRow.htsVisitGroup(), testRow.baseEntityId()), unused -> new ArrayList<>())
                            .add(testRow);
                }
            }

            return groupedTests;
        }
    }

    public Map<String, List<HivstSelfTestRow>> findHivstTestByBaseEntity(Connection connection,
                                                                          List<ServiceRow> serviceRows) throws SQLException {
        Set<String> baseEntityIds = new HashSet<>();
        for (ServiceRow serviceRow : serviceRows) {
            if (hasText(serviceRow.baseEntityId())) {
                baseEntityIds.add(serviceRow.baseEntityId());
            }
        }

        if (baseEntityIds.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT " +
                "r.event_id AS result_event_id, " +
                "r.event_date AS result_event_date, " +
                "r.base_entity_id, " +
                "r.kit_for, " +
                "r.kit_code AS result_kit_code, " +
                "r.hivst_result, " +
                "r.result_date, " +
                "r.register_to_hts, " +
                "k.event_id AS issue_event_id, " +
                "k.event_date AS issue_event_date, " +
                "CASE " +
                "    WHEN r.kit_for = 'client' THEN k.client_kit_batch_number " +
                "    WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_batch_number " +
                "    WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_batch_number " +
                "END AS kit_batch_number, " +
                "CASE " +
                "    WHEN r.kit_for = 'client' THEN k.client_kit_expiry_date " +
                "    WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_expiry_date " +
                "    WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_expiry_date " +
                "END AS kit_expiry_date " +
                "FROM " + schema + ".hivst_results r " +
                "JOIN " + schema + ".hivst_issue_kits k ON k.base_entity_id = r.base_entity_id " +
                "AND r.kit_code = CASE " +
                "    WHEN r.kit_for = 'client' THEN k.kit_code " +
                "    WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_code " +
                "    WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_code " +
                "END " +
                "WHERE r.base_entity_id IN (" + placeholders(baseEntityIds.size()) + ") " +
                "ORDER BY r.base_entity_id ASC, r.result_date ASC, r.event_id ASC";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            for (String baseEntityId : baseEntityIds) {
                statement.setString(index++, baseEntityId);
            }

            Map<String, List<HivstSelfTestRow>> rowsByBaseEntity = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    HivstSelfTestRow row = new HivstSelfTestRow(
                            resultSet.getString("result_event_id"),
                            resultSet.getString("result_event_date"),
                            resultSet.getString("base_entity_id"),
                            resultSet.getString("kit_for"),
                            resultSet.getString("result_kit_code"),
                            resultSet.getString("hivst_result"),
                            resultSet.getString("result_date"),
                            resultSet.getString("register_to_hts"),
                            resultSet.getString("issue_event_id"),
                            resultSet.getString("issue_event_date"),
                            resultSet.getString("kit_batch_number"),
                            resultSet.getString("kit_expiry_date")
                    );

                    if (!hasText(row.baseEntityId())) {
                        continue;
                    }

                    rowsByBaseEntity.computeIfAbsent(row.baseEntityId(), unused -> new ArrayList<>()).add(row);
                }
            }

            return rowsByBaseEntity;
        }
    }

    public Map<String, List<HivstSelfTestRow>> findHivstSelfTestsByBaseEntity(Connection connection,
                                                                               List<ServiceRow> serviceRows) throws SQLException {
        return findHivstTestByBaseEntity(connection, serviceRows);
    }

    public Map<String, Boolean> findEnrollmentEligibilityByBaseEntity(Connection connection,
                                                                      List<ServiceRow> serviceRows) throws SQLException {
        Set<String> baseEntityIds = new HashSet<>();
        for (ServiceRow serviceRow : serviceRows) {
            if (hasText(serviceRow.baseEntityId())) {
                baseEntityIds.add(serviceRow.baseEntityId());
            }
        }

        if (baseEntityIds.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT e.base_entity_id, e.eligibility_for_testing, e.date_created, e.event_id " +
                "FROM " + schema + ".cbhts_enrollment e " +
                "WHERE e.base_entity_id IN (" + placeholders(baseEntityIds.size()) + ") " +
                "ORDER BY e.base_entity_id ASC, e.date_created DESC NULLS LAST, e.event_id DESC";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            for (String baseEntityId : baseEntityIds) {
                statement.setString(index++, baseEntityId);
            }

            Map<String, Boolean> eligibilityByBaseEntity = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String baseEntityId = resultSet.getString("base_entity_id");
                    if (!hasText(baseEntityId) || eligibilityByBaseEntity.containsKey(baseEntityId)) {
                        continue;
                    }

                    String eligibility = resultSet.getString("eligibility_for_testing");
                    eligibilityByBaseEntity.put(baseEntityId, parseEnrollmentEligibility(eligibility));
                }
            }

            return eligibilityByBaseEntity;
        }
    }

    public static String serviceKey(ServiceRow serviceRow) {
        return testKey(serviceRow.htsVisitGroup(), serviceRow.baseEntityId());
    }

    private static String testKey(String htsVisitGroup, String baseEntityId) {
        if (hasText(htsVisitGroup)) {
            return "visit:" + htsVisitGroup;
        }
        return "entity:" + baseEntityId;
    }

    private static String placeholders(int count) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 0; i < count; i++) {
            joiner.add("?");
        }
        return joiner.toString();
    }

    private static long secondsToMillis(long seconds) {
        return Math.multiplyExact(seconds, 1000L);
    }

    private static Integer getNullableInteger(ResultSet resultSet, String column) throws SQLException {
        int value = resultSet.getInt(column);
        return resultSet.wasNull() ? null : value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static boolean parseEnrollmentEligibility(String value) {
        if (!hasText(value)) {
            return true;
        }

        String normalized = value.trim()
                .replace('-', '_')
                .replace(' ', '_')
                .toUpperCase(Locale.ROOT);

        if (normalized.equals("YES")
                || normalized.equals("TRUE")
                || normalized.equals("1")
                || normalized.equals("N")
                || normalized.equals("Y")
                || normalized.equals("NDIYO")) {
            return true;
        }

        if (normalized.equals("NO")
                || normalized.equals("FALSE")
                || normalized.equals("0")
                || normalized.equals("H")
                || normalized.equals("HAPANA")) {
            return false;
        }

        return true;
    }

    public record VerificationServiceMetadataRow(
            String baseEntityId,
            String providerId,
            String team,
            String teamId,
            String locationId,
            String entityType
    ) {
    }

    public record ServiceRow(
            String eventId,
            String baseEntityId,
            String htsVisitGroup,
            String visitDate,
            String htsVisitDate,
            long dateCreated,
            String providerId,
            String htsTestingApproach,
            String htsVisitType,
            String htsHasTheClientRecentlyTestedWithHivst,
            String htsPreviousHivstClientType,
            String htsPreviousHivstTestType,
            String htsPreviousHivstTestResults,
            String htsClientType,
            String htsTestingPoint,
            String htsTypeOfCounsellingProvided,
            String htsClientsTbScreeningOutcome,
            String htsHasPostTestCounsellingBeenProvided,
            String htsHivResultsDisclosure,
            String htsWereCondomsDistributed,
            Integer htsNumberOfMaleCondomsProvided,
            Integer htsNumberOfFemaleCondomsProvided,
            String htsPreventiveServices,
            String finalHivTestResult,
            String uniqueId,
            String firstName,
            String middleName,
            String lastName,
            String phoneNumber,
            String nationalId,
            String voterId,
            String driverLicense,
            String passport,
            String sex,
            String birthDate,
            String maritalStatus,
            String pregnancyStatus,
            String hfrCode,
            String region,
            String district,
            String districtCouncil,
            String ward,
            String healthFacility,
            String village,
            String counsellorName
    ) {
        public ServiceRow(
                String eventId,
                String baseEntityId,
                String htsVisitGroup,
                String visitDate,
                String htsVisitDate,
                long dateCreated,
                String providerId,
                String htsTestingApproach,
                String htsVisitType,
                String htsHasTheClientRecentlyTestedWithHivst,
                String htsPreviousHivstClientType,
                String htsPreviousHivstTestType,
                String htsPreviousHivstTestResults,
                String htsClientType,
                String htsTestingPoint,
                String htsTypeOfCounsellingProvided,
                String htsClientsTbScreeningOutcome,
                String htsHasPostTestCounsellingBeenProvided,
                String htsHivResultsDisclosure,
                String htsWereCondomsDistributed,
                Integer htsNumberOfMaleCondomsProvided,
                Integer htsNumberOfFemaleCondomsProvided,
                String htsPreventiveServices,
                String uniqueId,
                String firstName,
                String middleName,
                String lastName,
                String phoneNumber,
                String nationalId,
                String voterId,
                String driverLicense,
                String passport,
                String sex,
                String birthDate,
                String maritalStatus,
                String pregnancyStatus,
                String hfrCode,
                String region,
                String district,
                String districtCouncil,
                String ward,
                String healthFacility,
                String village,
                String counsellorName
        ) {
            this(
                    eventId,
                    baseEntityId,
                    htsVisitGroup,
                    visitDate,
                    htsVisitDate,
                    dateCreated,
                    providerId,
                    htsTestingApproach,
                    htsVisitType,
                    htsHasTheClientRecentlyTestedWithHivst,
                    htsPreviousHivstClientType,
                    htsPreviousHivstTestType,
                    htsPreviousHivstTestResults,
                    htsClientType,
                    htsTestingPoint,
                    htsTypeOfCounsellingProvided,
                    htsClientsTbScreeningOutcome,
                    htsHasPostTestCounsellingBeenProvided,
                    htsHivResultsDisclosure,
                    htsWereCondomsDistributed,
                    htsNumberOfMaleCondomsProvided,
                    htsNumberOfFemaleCondomsProvided,
                    htsPreventiveServices,
                    null,
                    uniqueId,
                    firstName,
                    middleName,
                    lastName,
                    phoneNumber,
                    nationalId,
                    voterId,
                    driverLicense,
                    passport,
                    sex,
                    birthDate,
                    maritalStatus,
                    pregnancyStatus,
                    hfrCode,
                    region,
                    district,
                    districtCouncil,
                    ward,
                    healthFacility,
                    village,
                    counsellorName
            );
        }
    }

    public record TestRow(
            String eventId,
            String htsVisitGroup,
            String baseEntityId,
            String typeOfTestKitUsed,
            String testKitBatchNumber,
            String testKitExpireDate,
            String testResult,
            String syphilisTestResults,
            String testType,
            long dateCreated
    ) {
    }

    public record HivstSelfTestRow(
            String resultEventId,
            String resultEventDate,
            String baseEntityId,
            String kitFor,
            String resultKitCode,
            String hivstResult,
            String resultDate,
            String registerToHts,
            String issueEventId,
            String issueEventDate,
            String kitBatchNumber,
            String kitExpiryDate
    ) {
    }
}

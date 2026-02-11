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
import java.util.Map;
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

    public List<ServiceRow> findServices(Connection connection, IntegrationRequest request) throws SQLException {
        String sql = "SELECT " +
                "s.event_id, s.base_entity_id, s.hts_visit_group, s.visit_date, s.hts_visit_date, s.date_created, " +
                "s.provider_id, s.hts_testing_approach, s.hts_visit_type, s.hts_has_the_client_recently_tested_with_hivst, " +
                "s.hts_previous_hivst_client_type, s.hts_previous_hivst_test_type, s.hts_client_type, s.hts_testing_point, " +
                "s.hts_type_of_counselling_provided, s.hts_clients_tb_screening_outcome, s.hts_has_post_test_counselling_been_provided, " +
                "s.hts_hiv_results_disclosure, s.hts_were_condoms_distributed, s.hts_number_of_male_condoms_provided, " +
                "s.hts_number_of_female_condoms_provided, s.hts_preventive_services, " +
                "c.unique_id, c.first_name, c.middle_name, c.last_name, c.phone_number, c.national_id, c.voter_id, c.driver_license, " +
                "c.passport, c.sex, c.birth_date, c.marital_status, c.preg_1yr, " +
                "l.hfr_code, l.region, l.district, l.district_council, l.ward, l.health_facility, l.village, " +
                "COALESCE(tm.name, s.provider_id) AS counsellor_name " +
                "FROM " + schema + ".cbhts_services s " +
                "JOIN " + schema + ".tanzania_locations l ON l.location_uuid = s.location_id " +
                "LEFT JOIN " + schema + ".client c ON c.base_entity_id = s.base_entity_id " +
                "LEFT JOIN LATERAL (" +
                "  SELECT m.name " +
                "  FROM " + schema + ".team_members m " +
                "  WHERE m.identifier = s.provider_id OR m.uuid = s.provider_id " +
                "  LIMIT 1" +
                ") tm ON TRUE " +
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
                            resultSet.getString("district_council"),
                            resultSet.getString("ward"),
                            resultSet.getString("health_facility"),
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
}

package com.abt.integration.db;

import com.abt.integration.model.IntegrationRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenSrpIntegrationRepositoryTest {

    @Test
    void findServices_shouldJoinHouseholdAndSelectResidenceCodesForFallback() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        IntegrationRequest request = new IntegrationRequest();
        request.setHfrCode("124899-6");
        request.setStartDate(1768262400L);
        request.setEndDate(1768262800L);
        request.setPageIndex(1);
        request.setPageSize(10);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        repository.findServices(connection, request);

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("LEFT JOIN public.household h ON h.primary_caregiver = s.base_entity_id"));
        assertTrue(sql.contains("LEFT JOIN public.tanzania_locations hl ON hl.location_uuid = NULLIF(TRIM(h.location_id), '')"));
        assertTrue(sql.contains("l.council_code AS provider_council_code"));
        assertTrue(sql.contains("hl.village_code AS household_village_code"));
        assertTrue(sql.contains("s.final_hiv_test_result"));
    }

    @Test
    void findHivstTestByBaseEntity_shouldUseDynamicKitJoinAndCaseColumns() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        repository.findHivstTestByBaseEntity(connection, List.of(buildServiceRow("base-1")));

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("FROM public.hivst_results r"));
        assertTrue(sql.contains("JOIN public.hivst_issue_kits k ON k.base_entity_id = r.base_entity_id"));
        assertTrue(sql.contains("AND r.kit_code = CASE"));
        assertTrue(sql.contains("WHEN r.kit_for = 'client' THEN k.kit_code"));
        assertTrue(sql.contains("WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_code"));
        assertTrue(sql.contains("WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_code"));
        assertTrue(sql.contains("CASE     WHEN r.kit_for = 'client' THEN k.client_kit_batch_number"));
        assertTrue(sql.contains("WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_batch_number"));
        assertTrue(sql.contains("WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_batch_number"));
        assertTrue(sql.contains("END AS kit_batch_number"));
        assertTrue(sql.contains("CASE     WHEN r.kit_for = 'client' THEN k.client_kit_expiry_date"));
        assertTrue(sql.contains("WHEN r.kit_for = 'sexual_partner' THEN k.sexual_partner_kit_expiry_date"));
        assertTrue(sql.contains("WHEN r.kit_for IN ('peer_friend','peer_fried') THEN k.peer_friend_kit_expiry_date"));
        assertTrue(sql.contains("END AS kit_expiry_date"));
        assertTrue(sql.contains("WHERE r.base_entity_id IN ("));
    }

    @Test
    void findHivstTestByBaseEntity_shouldMapAliasedResultColumns() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("result_event_id")).thenReturn("result-1");
        when(resultSet.getString("result_event_date")).thenReturn("2026-01-14");
        when(resultSet.getString("base_entity_id")).thenReturn("base-1");
        when(resultSet.getString("kit_for")).thenReturn("sexual_partner");
        when(resultSet.getString("result_kit_code")).thenReturn("KIT-SP");
        when(resultSet.getString("hivst_result")).thenReturn("reactive");
        when(resultSet.getString("result_date")).thenReturn("2026-01-14");
        when(resultSet.getString("register_to_hts")).thenReturn("yes");
        when(resultSet.getString("issue_event_id")).thenReturn("issue-1");
        when(resultSet.getString("issue_event_date")).thenReturn("2026-01-14T09:00:00+03:00");
        when(resultSet.getString("kit_batch_number")).thenReturn("BATCH-SP");
        when(resultSet.getString("kit_expiry_date")).thenReturn("2027-01-11");

        Map<String, List<OpenSrpIntegrationRepository.HivstSelfTestRow>> mapped = repository.findHivstTestByBaseEntity(
                connection,
                List.of(buildServiceRow("base-1"))
        );

        assertTrue(mapped.containsKey("base-1"));
        assertEquals(1, mapped.get("base-1").size());
        OpenSrpIntegrationRepository.HivstSelfTestRow row = mapped.get("base-1").get(0);
        assertEquals("result-1", row.resultEventId());
        assertEquals("sexual_partner", row.kitFor());
        assertEquals("KIT-SP", row.resultKitCode());
        assertEquals("BATCH-SP", row.kitBatchNumber());
        assertEquals("2027-01-11", row.kitExpiryDate());
    }

    @Test
    void findEnrollmentEligibilityByBaseEntity_shouldQueryEnrollmentTableInBulk() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        repository.findEnrollmentEligibilityByBaseEntity(connection, List.of(buildServiceRow("base-1")));

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("FROM public.cbhts_enrollment e"));
        assertTrue(sql.contains("WHERE e.base_entity_id IN ("));
        assertTrue(sql.contains("ORDER BY e.base_entity_id ASC, e.date_created DESC NULLS LAST, e.event_id DESC"));
    }

    @Test
    void findEnrollmentEligibilityByBaseEntity_shouldMapAndDefaultEligibilityValues() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, true, false);
        when(resultSet.getString("base_entity_id"))
                .thenReturn("base-1", "base-1", "base-2", "base-3");
        when(resultSet.getString("eligibility_for_testing"))
                .thenReturn("no", "yes", "yes", null);

        Map<String, Boolean> mapped = repository.findEnrollmentEligibilityByBaseEntity(
                connection,
                List.of(buildServiceRow("base-1"), buildServiceRow("base-2"), buildServiceRow("base-3"))
        );

        assertEquals(3, mapped.size());
        assertFalse(mapped.get("base-1"));
        assertTrue(mapped.get("base-2"));
        assertTrue(mapped.get("base-3"));
    }

    private OpenSrpIntegrationRepository.ServiceRow buildServiceRow(String baseEntityId) {
        return new OpenSrpIntegrationRepository.ServiceRow(
                "event-1",
                baseEntityId,
                "visit-group-1",
                "2026-01-14",
                "2026-01-14",
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
                "relative",
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
    }
}

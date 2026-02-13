package com.abt.integration.db;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenSrpIntegrationRepositoryTest {

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

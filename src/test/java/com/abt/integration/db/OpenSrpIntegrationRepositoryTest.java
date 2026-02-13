package com.abt.integration.db;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenSrpIntegrationRepositoryTest {

    @Test
    void findHivstSelfTestsByBaseEntity_shouldJoinOnKitCodeAndBaseEntityId() throws SQLException {
        OpenSrpIntegrationRepository repository = new OpenSrpIntegrationRepository("public");

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        repository.findHivstSelfTestsByBaseEntity(connection, List.of(buildServiceRow("base-1")));

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("JOIN public.hivst_results r ON k.kit_code = r.kit_code AND k.base_entity_id = r.base_entity_id"));
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

package com.abt.integration.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class PostgresConnectionFactory {
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_DB = "opensrp";
    private static final String DEFAULT_SCHEMA = "public";

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("PostgreSQL JDBC driver not found", e);
        }
    }

    public Connection openConnection() throws SQLException {
        String explicitUrl = env("OPENSRP_DB_URL");
        String host = envOrDefault("OPENSRP_DB_HOST", DEFAULT_HOST);
        String port = envOrDefault("OPENSRP_DB_PORT", DEFAULT_PORT);
        String dbName = envOrDefault("OPENSRP_DB_NAME", DEFAULT_DB);
        String schema = envOrDefault("OPENSRP_DB_SCHEMA", DEFAULT_SCHEMA);
        String user = env("OPENSRP_DB_USER");
        String password = env("OPENSRP_DB_PASSWORD");

        String jdbcUrl = explicitUrl;
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s?currentSchema=%s", host, port, dbName, schema);
        }

        Properties properties = new Properties();
        if (user != null && !user.isBlank()) {
            properties.setProperty("user", user);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }

        String sslMode = env("OPENSRP_DB_SSLMODE");
        if (sslMode != null && !sslMode.isBlank()) {
            properties.setProperty("sslmode", sslMode);
        }

        return DriverManager.getConnection(jdbcUrl, properties);
    }

    public String schema() {
        String schema = envOrDefault("OPENSRP_DB_SCHEMA", DEFAULT_SCHEMA);
        if (!schema.matches("^[A-Za-z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid OPENSRP_DB_SCHEMA value: " + schema);
        }
        return schema;
    }

    private String env(String key) {
        return System.getenv(key);
    }

    private String envOrDefault(String key, String fallback) {
        return Objects.requireNonNullElse(env(key), fallback);
    }
}

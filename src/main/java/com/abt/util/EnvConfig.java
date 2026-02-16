package com.abt.util;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.github.cdimascio.dotenv.Dotenv;

import java.time.Duration;
import java.util.Optional;

public final class EnvConfig {

    private static final Dotenv DOTENV = loadDotenv();

    private EnvConfig() {
    }

    public static String getOrDefault(String key, String defaultValue) {
        return get(key).orElse(defaultValue);
    }

    public static String getFirstOrDefault(String defaultValue, String... keys) {
        if (keys == null || keys.length == 0) {
            return defaultValue;
        }
        for (String key : keys) {
            Optional<String> value = get(key);
            if (value.isPresent()) {
                return value.get();
            }
        }
        return defaultValue;
    }

    public static int getIntOrDefault(String key, int defaultValue) {
        Optional<String> value = get(key);
        if (value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.get());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static Duration getDurationOrDefault(String key, Duration defaultValue) {
        Optional<String> value = get(key);
        if (value.isEmpty()) {
            return defaultValue;
        }
        try {
            return ConfigFactory.parseString("value=" + value.get()).getDuration("value");
        } catch (ConfigException e) {
            try {
                return Duration.parse(value.get());
            } catch (Exception ignored) {
                return defaultValue;
            }
        }
    }

    private static Optional<String> get(String key) {
        String value = sanitize(System.getProperty(key));
        if (value != null) {
            return Optional.of(value);
        }

        value = sanitize(System.getenv(key));
        if (value != null) {
            return Optional.of(value);
        }

        if (DOTENV != null) {
            value = sanitize(DOTENV.get(key));
            if (value != null) {
                return Optional.of(value);
            }
        }

        return Optional.empty();
    }

    private static Dotenv loadDotenv() {
        try {
            return Dotenv.configure()
                    .ignoreIfMalformed()
                    .ignoreIfMissing()
                    .load();
        } catch (Exception e) {
            return null;
        }
    }

    private static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
                (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
        }
        return trimmed.isEmpty() ? null : trimmed;
    }
}

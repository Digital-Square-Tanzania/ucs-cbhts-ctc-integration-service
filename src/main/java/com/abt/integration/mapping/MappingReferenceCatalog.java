package com.abt.integration.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MappingReferenceCatalog {
    private static final String CSV_RESOURCE = "CTC2HTSVariables_Integration_mappings.csv";
    private static final String FORMS_DIRECTORY = "reference_openrp_forms";
    private static final List<String> KNOWN_FORM_FILES = List.of(
            "hts_dna_pcr_sample_collection.json",
            "hts_first_hiv_test.json",
            "hts_hiv_testing.json",
            "hts_post_test_services.json",
            "hts_pre_test_services.json",
            "hts_preventive_services.json",
            "hts_repeat_first_hiv_test.json",
            "hts_sample_registration.json",
            "hts_screening_15_and_above_form.json",
            "hts_screening_children_aged_10_to_14_form.json",
            "hts_screening_children_aged_2_to_9_form.json",
            "hts_second_hiv_test.json",
            "hts_unigold_hiv_test.json",
            "hts_visit_type.json"
    );

    private final Map<String, SectionMapping> sectionMappings = new HashMap<>();
    private final Map<String, Set<String>> formOptionsByField = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MappingReferenceCatalog() {
        loadCsvMappings();
        loadFormMappings();
    }

    public String mapToCode(String sectionName, String rawValue, Map<String, String> aliases, String fallbackCode) {
        if (isBlank(rawValue)) {
            return fallbackCode;
        }

        String normalizedSection = normalize(sectionName);
        SectionMapping mapping = sectionMappings.get(normalizedSection);
        if (mapping == null) {
            return fallbackCode;
        }

        String normalizedRaw = normalize(rawValue);

        String byCode = mapping.codeByNormalizedCode().get(normalizedRaw);
        if (byCode != null) {
            return byCode;
        }

        String byIntegrationValue = mapping.codeByNormalizedIntegration().get(normalizedRaw);
        if (byIntegrationValue != null) {
            return byIntegrationValue;
        }

        if (aliases != null) {
            String alias = aliases.get(normalizedRaw);
            if (alias != null) {
                String normalizedAlias = normalize(alias);
                String aliasByCode = mapping.codeByNormalizedCode().get(normalizedAlias);
                if (aliasByCode != null) {
                    return aliasByCode;
                }
                String aliasByIntegration = mapping.codeByNormalizedIntegration().get(normalizedAlias);
                if (aliasByIntegration != null) {
                    return aliasByIntegration;
                }
            }
        }

        return fallbackCode;
    }

    public boolean isKnownFormOption(String fieldKey, String rawOption) {
        if (isBlank(fieldKey) || isBlank(rawOption)) {
            return false;
        }

        Set<String> options = formOptionsByField.get(normalize(fieldKey));
        if (options == null || options.isEmpty()) {
            return false;
        }

        return options.contains(normalize(rawOption));
    }

    public static String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value
                .trim()
                .toUpperCase(Locale.ROOT)
                .replaceAll("[^A-Z0-9]+", "_")
                .replaceAll("^_+", "")
                .replaceAll("_+$", "");
    }

    private void loadCsvMappings() {
        try (InputStream inputStream = openResource(CSV_RESOURCE)) {
            if (inputStream == null) {
                throw new IllegalStateException("Unable to load mapping CSV: " + CSV_RESOURCE);
            }

            List<String> lines;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                lines = reader.lines().collect(Collectors.toList());
            }

            String currentSection = null;

            for (String line : lines) {
                List<String> columns = parseCsvLine(line);

                String secondColumn = column(columns, 1).trim();
                String thirdColumn = column(columns, 2).trim();
                String fourthColumn = column(columns, 3).trim();

                if (secondColumn.contains(":")) {
                    currentSection = secondColumn.substring(0, secondColumn.indexOf(':')).trim();
                    continue;
                }

                if (!isBlank(secondColumn) && isBlank(thirdColumn) && isBlank(fourthColumn)) {
                    currentSection = secondColumn;
                    continue;
                }

                if (isBlank(currentSection)) {
                    continue;
                }

                if (isBlank(secondColumn) || isBlank(fourthColumn)) {
                    continue;
                }

                if (secondColumn.toLowerCase(Locale.ROOT).contains("code") || fourthColumn.equalsIgnoreCase("Integration Values")) {
                    continue;
                }

                String normalizedSection = normalize(currentSection);
                SectionMapping sectionMapping = sectionMappings.computeIfAbsent(normalizedSection,
                        unused -> new SectionMapping(new HashMap<>(), new HashMap<>()));

                sectionMapping.codeByNormalizedCode().put(normalize(secondColumn), secondColumn);
                sectionMapping.codeByNormalizedIntegration().put(normalize(fourthColumn), secondColumn);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load mapping CSV", e);
        }
    }

    private void loadFormMappings() {
        try {
            List<JsonNode> forms = loadFormNodes();
            for (JsonNode form : forms) {
                collectFieldOptions(form);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load reference OpenSRP forms", e);
        }
    }

    private List<JsonNode> loadFormNodes() throws IOException {
        Path formsPath = Paths.get("resources", FORMS_DIRECTORY);
        List<JsonNode> forms = new ArrayList<>();

        if (Files.exists(formsPath) && Files.isDirectory(formsPath)) {
            try (Stream<Path> paths = Files.list(formsPath)) {
                List<Path> files = paths
                        .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".json"))
                        .collect(Collectors.toList());

                for (Path file : files) {
                    forms.add(objectMapper.readTree(file.toFile()));
                }
            }
            return forms;
        }

        for (String formFile : KNOWN_FORM_FILES) {
            String classpathResource = FORMS_DIRECTORY + "/" + formFile;
            try (InputStream stream = openResource(classpathResource)) {
                if (stream != null) {
                    forms.add(objectMapper.readTree(stream));
                }
            }
        }

        return forms;
    }

    private void collectFieldOptions(JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }

        if (node.isObject()) {
            JsonNode keyNode = node.get("key");
            JsonNode optionsNode = node.get("options");

            if (keyNode != null && keyNode.isTextual() && optionsNode != null && optionsNode.isArray()) {
                String fieldKey = normalize(keyNode.asText());
                Set<String> options = formOptionsByField.computeIfAbsent(fieldKey, unused -> new HashSet<>());
                for (JsonNode option : optionsNode) {
                    JsonNode optionKey = option.get("key");
                    if (optionKey != null && optionKey.isTextual()) {
                        options.add(normalize(optionKey.asText()));
                    }
                }
            }

            node.fields().forEachRemaining(entry -> collectFieldOptions(entry.getValue()));
            return;
        }

        if (node.isArray()) {
            for (JsonNode item : node) {
                collectFieldOptions(item);
            }
        }
    }

    private InputStream openResource(String resourcePath) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream classpathStream = classLoader.getResourceAsStream(resourcePath);
        if (classpathStream != null) {
            return classpathStream;
        }

        Path fallbackPath = Paths.get("resources", resourcePath);
        if (Files.exists(fallbackPath) && Files.isRegularFile(fallbackPath)) {
            return Files.newInputStream(fallbackPath);
        }

        return null;
    }

    private List<String> parseCsvLine(String line) {
        List<String> columns = new ArrayList<>();
        if (line == null) {
            return columns;
        }

        StringBuilder currentColumn = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                inQuotes = !inQuotes;
                continue;
            }
            if (ch == ',' && !inQuotes) {
                columns.add(currentColumn.toString());
                currentColumn.setLength(0);
                continue;
            }
            currentColumn.append(ch);
        }

        columns.add(currentColumn.toString());
        return columns;
    }

    private String column(List<String> columns, int index) {
        if (index < 0 || index >= columns.size()) {
            return "";
        }
        return columns.get(index);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private record SectionMapping(Map<String, String> codeByNormalizedCode,
                                  Map<String, String> codeByNormalizedIntegration) {
    }
}

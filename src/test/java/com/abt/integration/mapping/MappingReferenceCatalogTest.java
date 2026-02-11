package com.abt.integration.mapping;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MappingReferenceCatalogTest {

    @Test
    void mapToCode_shouldResolveFromCsvAndFormAliases() {
        MappingReferenceCatalog catalog = new MappingReferenceCatalog();

        assertEquals("HO", catalog.mapToCode("MaritalStatusCode", "SINGLE", null, "HH"));
        assertEquals("MY", catalog.mapToCode(
                "CounsellingTypeCode",
                "individual",
                Map.of(MappingReferenceCatalog.normalize("individual"), "INDIVIDUAL"),
                "HH"
        ));
        assertTrue(catalog.isKnownFormOption("hts_type_of_counselling_provided", "individual"));
    }
}

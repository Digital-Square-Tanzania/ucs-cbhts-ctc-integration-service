package com.abt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UcsCbhtsCtsIntegrationServiceAppTest {

    private static final String NEW_KEY = "LTF_INDEX_PAYLOAD_ENCRYPTION_SECRET_KEY";

    @AfterEach
    void clearSecretKeyProperties() {
        System.clearProperty(NEW_KEY);
    }

    @Test
    void resolveLtfIndexPayloadEncryptionSecretKey_shouldUseNewKeyWhenPresent() {
        System.setProperty(NEW_KEY, "new-secret-value");

        assertEquals(
                "new-secret-value",
                UcsCbhtsCtsIntegrationServiceApp.resolveLtfIndexPayloadEncryptionSecretKey()
        );
    }

    @Test
    void resolveLtfIndexPayloadEncryptionSecretKey_shouldUseDefaultWhenKeyMissing() {
        System.clearProperty(NEW_KEY);

        assertEquals(
                "secret-key",
                UcsCbhtsCtsIntegrationServiceApp.resolveLtfIndexPayloadEncryptionSecretKey()
        );
    }
}

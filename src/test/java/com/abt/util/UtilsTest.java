package com.abt.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UtilsTest {

    private static final String SECRET_KEY = "unit-test-secret-key";

    @Test
    void encryptDataNew_shouldEncryptAndDecryptRoundTrip() throws Exception {
        String plainText = "John Doe 123XyZ";

        String encryptedText = Utils.encryptDataNew(plainText, SECRET_KEY, null);
        String decryptedText = Utils.decryptDataNew(encryptedText, SECRET_KEY, null);

        assertNotNull(encryptedText);
        assertNotEquals(plainText, encryptedText);
        assertEquals(plainText, decryptedText);
    }

    @Test
    void encryptDataNew_shouldPreserveSpacesAndMixedCaseAfterDecryption() throws Exception {
        String plainText = "  MiXeD Name Value 007  ";

        String encryptedText = Utils.encryptDataNew(plainText, SECRET_KEY, null);
        String decryptedText = Utils.decryptDataNew(encryptedText, SECRET_KEY, null);

        assertNotNull(encryptedText);
        assertNotEquals(plainText, encryptedText);
        assertEquals(plainText, decryptedText);
    }
}

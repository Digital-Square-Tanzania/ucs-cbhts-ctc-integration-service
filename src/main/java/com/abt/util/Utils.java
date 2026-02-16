package com.abt.util;

import com.abt.domain.LtfClientRequest;
import com.abt.domain.Task;
import org.joda.time.DateTime;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

public class Utils {

    public static String concatenateFullName(String firstName, String middleName, String surname) {
        // Use a StringBuilder for efficient string concatenation
        StringBuilder fullName = new StringBuilder();

        // Append each part if it's not null or blank
        if (firstName != null && !firstName.isBlank()) {
            fullName.append(firstName.trim());
        }

        if (middleName != null && !middleName.isBlank()) {
            if (!fullName.isEmpty()) fullName.append(" ");
            fullName.append(middleName.trim());
        }

        if (surname != null && !surname.isBlank()) {
            if (!fullName.isEmpty()) fullName.append(" ");
            fullName.append(surname.trim());
        }

        return fullName.toString();
    }

    public static Task generateTask(LtfClientRequest request, String reasonReference) {
        Task task = new Task();
        DateTime now = new DateTime();
        task.setIdentifier(UUID.randomUUID().toString());
        task.setPlanIdentifier("5270285b-5a3b-4647-b772-c0b3c52e2b71");
        task.setGroupIdentifier(request.getLocationId());
        task.setStatus(Task.TaskStatus.READY);
        task.setBusinessStatus("Referred");
        task.setPriority(3);
        task.setCode("Referral");
        task.setReasonReference(reasonReference);

        task.setDescription("CTC");

        task.setFocus("LTFU");
        task.setForEntity(request.getBaseEntityId());
        task.setExecutionStartDate(now);
        task.setAuthoredOn(now);
        task.setLastModified(now);
        task.setOwner(request.getProviderId());
        task.setRequester(request.getProviderId());
        task.setLocation(null);
        return task;
    }

    /**
     * Encrypts the given plain text using AES encryption.
     *
     * @param plainText The plain text to encrypt.
     * @param secretKey The secret key used for encryption (if null or empty, returns an error message).
     * @param iv        An optional initialization vector. If null or empty, a random IV is generated.
     * @return The Base64 encoded string of the IV prepended to the ciphertext, or an error message.
     * @throws Exception if an encryption error occurs.
     */
    public static String encryptDataNew(String plainText, String secretKey, String iv) throws Exception {
        if (secretKey == null || secretKey.isEmpty()) {
            return "Error: No Key Supplied";
        }

        // Generate AES key: using 256 bits (32 bytes)
        byte[] keyBytes = truncateHashNew(secretKey, 32);
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");

        // Determine IV: either use provided value (truncated to 16 bytes) or generate a random one
        byte[] ivBytes;
        if (iv == null || iv.isEmpty()) {
            ivBytes = new byte[16]; // AES block size is 16 bytes
            SecureRandom random = new SecureRandom();
            random.nextBytes(ivBytes);
        } else {
            ivBytes = truncateHashNew(iv, 16);
        }
        IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);

        // Create and initialize cipher for encryption (AES/CBC/PKCS5Padding)
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);

        // Convert plaintext to bytes (UTF-8) and encrypt
        byte[] plainTextBytes = plainText.getBytes(StandardCharsets.UTF_8);
        byte[] cipherTextBytes = cipher.doFinal(plainTextBytes);

        // Prepend the IV to the ciphertext so it can be extracted during decryption
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(ivBytes);
        outputStream.write(cipherTextBytes);
        byte[] finalBytes = outputStream.toByteArray();

        // Return the final byte array as a Base64-encoded string
        return Base64.getEncoder().encodeToString(finalBytes);
    }

    /**
     * Decrypts the given encrypted text (which must be a Base64-encoded string containing the IV and ciphertext).
     *
     * @param encryptedText The Base64-encoded encrypted text.
     * @param secretKey     The secret key used for decryption (if null or empty, returns an error message).
     * @param iv            An optional IV string. If null or empty, the IV is extracted from the encrypted text.
     * @return The decrypted plain text.
     * @throws Exception if a decryption error occurs.
     */
    public static String decryptDataNew(String encryptedText, String secretKey, String iv){
        try {
            if (secretKey == null || secretKey.isEmpty()) {
                return "Error: No Key Supplied";
            }

            // Decode the encrypted text from Base64 to bytes
            byte[] encryptedBytes = Base64.getDecoder().decode(encryptedText);

            // Generate AES key
            byte[] keyBytes = truncateHashNew(secretKey, 32);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");

            byte[] ivBytes = new byte[16];
            byte[] cipherTextBytes;

            if (iv == null || iv.isEmpty()) {
                // Extract the IV from the beginning of the encrypted data
                System.arraycopy(encryptedBytes, 0, ivBytes, 0, ivBytes.length);
                // The remainder is the actual ciphertext
                cipherTextBytes = Arrays.copyOfRange(encryptedBytes, ivBytes.length, encryptedBytes.length);
            } else {
                ivBytes = truncateHashNew(iv, 16);
                cipherTextBytes = encryptedBytes;
            }
            IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);

            // Initialize cipher for decryption
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

            // Decrypt and convert to a UTF-8 string
            byte[] plainTextBytes = cipher.doFinal(cipherTextBytes);
            return new String(plainTextBytes, StandardCharsets.UTF_8);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Mimics the VB TruncateHashNew method by generating a SHA-256 hash of the input string
     * and truncating it to the specified length.
     *
     * @param input  The input string to hash.
     * @param length The desired byte array length.
     * @return A byte array containing the truncated hash.
     * @throws Exception if the hashing algorithm is not available.
     */
    private static byte[] truncateHashNew(String input, int length) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        // Truncate or pad the hash to the desired length
        return Arrays.copyOf(hash, length);
    }
}

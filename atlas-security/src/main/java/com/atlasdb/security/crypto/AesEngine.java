package com.atlasdb.security.crypto;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles AES-GCM encryption and decryption with key rotation support.
 */
public final class AesEngine {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int TAG_LENGTH_BIT = 128;
    private static final int IV_LENGTH_BYTE = 12;

    private final Map<Integer, SecretKey> keyRing = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final SecureRandom random = new SecureRandom();
    private int activeKeyVersion = 0;

    public AesEngine(SecretKey initialKey) {
        Objects.requireNonNull(initialKey, "initialKey cannot be null");
        rotateKey(initialKey);
    }

    /**
     * Generates a random 256-bit AES key.
     */
    public static SecretKey generateKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return keyGen.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate AES key", e);
        }
    }

    /**
     * Rotates the encryption key. Registers the new key as the active key version.
     * Historical keys are kept to decrypt older blocks.
     */
    public void rotateKey(SecretKey newKey) {
        Objects.requireNonNull(newKey, "newKey cannot be null");
        lock.writeLock().lock();
        try {
            activeKeyVersion++;
            keyRing.put(activeKeyVersion, newKey);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Encrypts plaintext bytes using GCM mode and prepends the key version and IV.
     * Output format: [Key Version (4 bytes)] [IV (12 bytes)] [Ciphertext]
     */
    public byte[] encrypt(byte[] plaintext) {
        Objects.requireNonNull(plaintext, "plaintext cannot be null");

        int keyVer;
        SecretKey activeKey;
        lock.readLock().lock();
        try {
            keyVer = activeKeyVersion;
            activeKey = keyRing.get(keyVer);
        } finally {
            lock.readLock().unlock();
        }

        if (activeKey == null) {
            throw new IllegalStateException("No active encryption key registered.");
        }

        try {
            byte[] iv = new byte[IV_LENGTH_BYTE];
            random.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BIT, iv);
            cipher.init(Cipher.ENCRYPT_MODE, activeKey, spec);

            byte[] ciphertext = cipher.doFinal(plaintext);

            // Pack key version, IV, and ciphertext
            ByteBuffer buffer = ByteBuffer.allocate(4 + IV_LENGTH_BYTE + ciphertext.length);
            buffer.putInt(keyVer);
            buffer.put(iv);
            buffer.put(ciphertext);
            return buffer.array();
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed", e);
        }
    }

    /**
     * Decrypts GCM-encrypted payloads using the key version embedded in the header.
     */
    public byte[] decrypt(byte[] payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        if (payload.length < 4 + IV_LENGTH_BYTE) {
            throw new IllegalArgumentException("Payload is too short to contain version and IV headers.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int keyVer = buffer.getInt();
        
        byte[] iv = new byte[IV_LENGTH_BYTE];
        buffer.get(iv);

        byte[] ciphertext = new byte[buffer.remaining()];
        buffer.get(ciphertext);

        SecretKey decryptKey;
        lock.readLock().lock();
        try {
            decryptKey = keyRing.get(keyVer);
        } finally {
            lock.readLock().unlock();
        }

        if (decryptKey == null) {
            throw new IllegalStateException("Missing decryption key for version: " + keyVer);
        }

        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BIT, iv);
            cipher.init(Cipher.DECRYPT_MODE, decryptKey, spec);
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new RuntimeException("Decryption failed", e);
        }
    }
}

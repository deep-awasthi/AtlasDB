package com.atlasdb.security;

import com.atlasdb.security.crypto.AesEngine;
import com.atlasdb.security.rbac.RbacManager;
import com.atlasdb.security.ssl.SslContextHelper;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.net.ssl.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SecurityIntegrationTest {

    @Test
    void testUserAuthenticationAndRbac() {
        RbacManager rbac = new RbacManager();

        // 1. Create users with different roles
        rbac.createUser("adminUser", "adminPass", RbacManager.ROLE_ADMIN);
        rbac.createUser("writerUser", "writerPass", RbacManager.ROLE_READ_WRITE);
        rbac.createUser("readerUser", "readerPass", RbacManager.ROLE_READ_ONLY);

        // 2. Test authentication
        assertTrue(rbac.authenticate("adminUser", "adminPass"));
        assertTrue(rbac.authenticate("writerUser", "writerPass"));
        assertTrue(rbac.authenticate("readerUser", "readerPass"));

        assertFalse(rbac.authenticate("adminUser", "wrongPass"));
        assertFalse(rbac.authenticate("unknownUser", "pass"));

        // Mock SocketChannels to simulate sessions
        SocketChannel adminChannel = null;
        SocketChannel writerChannel = null;
        SocketChannel readerChannel = null;
        SocketChannel guestChannel = null;

        try {
            adminChannel = SocketChannel.open();
            writerChannel = SocketChannel.open();
            readerChannel = SocketChannel.open();
            guestChannel = SocketChannel.open();
        } catch (IOException ignored) {}

        assertNotNull(adminChannel);

        // Register sessions
        rbac.registerSession(adminChannel, "adminUser");
        rbac.registerSession(writerChannel, "writerUser");
        rbac.registerSession(readerChannel, "readerUser");

        // 3. Verify RBAC Permissions
        // Admin Permissions
        assertTrue(rbac.hasPermission(adminChannel, RbacManager.PERM_READ_DATA));
        assertTrue(rbac.hasPermission(adminChannel, RbacManager.PERM_WRITE_DATA));
        assertTrue(rbac.hasPermission(adminChannel, RbacManager.PERM_DDL_ADMIN));

        // Writer Permissions
        assertTrue(rbac.hasPermission(writerChannel, RbacManager.PERM_READ_DATA));
        assertTrue(rbac.hasPermission(writerChannel, RbacManager.PERM_WRITE_DATA));
        assertFalse(rbac.hasPermission(writerChannel, RbacManager.PERM_DDL_ADMIN));

        // Reader Permissions
        assertTrue(rbac.hasPermission(readerChannel, RbacManager.PERM_READ_DATA));
        assertFalse(rbac.hasPermission(readerChannel, RbacManager.PERM_WRITE_DATA));
        assertFalse(rbac.hasPermission(readerChannel, RbacManager.PERM_DDL_ADMIN));

        // Guest Permissions (Unauthenticated)
        assertFalse(rbac.hasPermission(guestChannel, RbacManager.PERM_READ_DATA));

        // Close session
        rbac.closeSession(adminChannel);
        assertFalse(rbac.hasPermission(adminChannel, RbacManager.PERM_READ_DATA));
    }

    @Test
    void testEncryptionAtRestAndKeyRotation() {
        // 1. Generate key and initialize engine
        SecretKey initialKey = AesEngine.generateKey();
        AesEngine aes = new AesEngine(initialKey);

        String secretText1 = "Sensitive database write record 1";
        byte[] plaintext1 = secretText1.getBytes(StandardCharsets.UTF_8);

        // Encrypt with Key Version 1
        byte[] cipher1 = aes.encrypt(plaintext1);
        assertNotNull(cipher1);
        assertTrue(cipher1.length > plaintext1.length);

        // Decrypt and verify
        byte[] decrypted1 = aes.decrypt(cipher1);
        assertEquals(secretText1, new String(decrypted1, StandardCharsets.UTF_8));

        // 2. Rotate Encryption Key
        SecretKey secondKey = AesEngine.generateKey();
        aes.rotateKey(secondKey);

        String secretText2 = "Sensitive database write record 2";
        byte[] plaintext2 = secretText2.getBytes(StandardCharsets.UTF_8);

        // Encrypt with Key Version 2
        byte[] cipher2 = aes.encrypt(plaintext2);
        assertNotNull(cipher2);

        // Decrypt new cipher with Version 2
        byte[] decrypted2 = aes.decrypt(cipher2);
        assertEquals(secretText2, new String(decrypted2, StandardCharsets.UTF_8));

        // Decrypt historical cipher (Version 1) still succeeds
        byte[] decryptedHistorical = aes.decrypt(cipher1);
        assertEquals(secretText1, new String(decryptedHistorical, StandardCharsets.UTF_8));
    }

    @Test
    void testSecureTlsSocketTransmission() throws Exception {
        // Load keystore from test resources
        InputStream keyStoreStream = SecurityIntegrationTest.class.getResourceAsStream("/keystore.p12");
        assertNotNull(keyStoreStream, "Keystore file must be bundled in resources");

        SSLContext sslContext = SslContextHelper.createSSLContext(keyStoreStream, "password");
        assertNotNull(sslContext);

        SSLServerSocketFactory ssf = sslContext.getServerSocketFactory();
        SSLSocketFactory sf = sslContext.getSocketFactory();

        // Start TLS Server Socket on local port
        SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket(0); // auto-bind free port
        int port = serverSocket.getLocalPort();

        Thread.ofVirtual().name("tls-test-server").start(() -> {
            try (SSLSocket clientSocket = (SSLSocket) serverSocket.accept()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);

                String msg = reader.readLine();
                if (msg != null) {
                    writer.println("SECURE_ACK: " + msg);
                }
            } catch (IOException e) {
                // Ignore
            }
        });

        // Connect secure TLS Client
        try (SSLSocket clientSocket = (SSLSocket) sf.createSocket("localhost", port)) {
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            writer.println("Hello TLS 1.3");
            String response = reader.readLine();
            assertEquals("SECURE_ACK: Hello TLS 1.3", response);
        } finally {
            serverSocket.close();
        }
    }
}

package com.atlasdb.security.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Objects;

/**
 * Helper utility for initializing SSLContext instances for secure TLS communications.
 */
public final class SslContextHelper {

    /**
     * Builds an SSLContext using PKCS12 keystores.
     *
     * @param keyStorePath     path to the keystore file
     * @param keyStorePassword password of the keystore
     * @return the configured SSLContext
     */
    public static SSLContext createSSLContext(String keyStorePath, String keyStorePassword) throws Exception {
        Objects.requireNonNull(keyStorePath, "keyStorePath cannot be null");
        Objects.requireNonNull(keyStorePassword, "keyStorePassword cannot be null");

        File file = new File(keyStorePath);
        try (InputStream in = new FileInputStream(file)) {
            return createSSLContext(in, keyStorePassword);
        }
    }

    /**
     * Builds an SSLContext from an input stream reading keystores.
     */
    public static SSLContext createSSLContext(InputStream keyStoreStream, String keyStorePassword) throws Exception {
        Objects.requireNonNull(keyStoreStream, "keyStoreStream cannot be null");
        Objects.requireNonNull(keyStorePassword, "keyStorePassword cannot be null");

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(keyStoreStream, keyStorePassword.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyStorePassword.toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return sslContext;
    }
}

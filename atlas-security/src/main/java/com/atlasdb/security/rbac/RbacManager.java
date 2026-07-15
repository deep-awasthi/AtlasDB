package com.atlasdb.security.rbac;

import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages Role-Based Access Control (RBAC), user authentication,
 * and active session authorization checks.
 */
public final class RbacManager {

    // Permissions
    public static final String PERM_READ_DATA = "READ_DATA";
    public static final String PERM_WRITE_DATA = "WRITE_DATA";
    public static final String PERM_DDL_ADMIN = "DDL_ADMIN";

    // Roles
    public static final String ROLE_ADMIN = "ADMIN";
    public static final String ROLE_READ_WRITE = "READ_WRITE";
    public static final String ROLE_READ_ONLY = "READ_ONLY";

    private static final Map<String, Set<String>> ROLE_PERMISSIONS = new HashMap<>();

    static {
        ROLE_PERMISSIONS.put(ROLE_ADMIN, Set.of(PERM_READ_DATA, PERM_WRITE_DATA, PERM_DDL_ADMIN));
        ROLE_PERMISSIONS.put(ROLE_READ_WRITE, Set.of(PERM_READ_DATA, PERM_WRITE_DATA));
        ROLE_PERMISSIONS.put(ROLE_READ_ONLY, Set.of(PERM_READ_DATA));
    }

    private final Map<String, User> users = new HashMap<>();
    private final Map<SocketChannel, User> activeSessions = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final SecureRandom random = new SecureRandom();

    /**
     * Creates a new user with salted SHA-256 hashed password.
     */
    public void createUser(String username, String password, String role) {
        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");
        Objects.requireNonNull(role, "role cannot be null");

        if (!ROLE_PERMISSIONS.containsKey(role.toUpperCase())) {
            throw new IllegalArgumentException("Unknown role: " + role);
        }

        byte[] salt = new byte[16];
        random.nextBytes(salt);
        String hashedPassword = hashPassword(password, salt);

        lock.lock();
        try {
            users.put(username.toLowerCase(), new User(username, hashedPassword, salt, role.toUpperCase()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Authenticates credentials and returns true if valid.
     */
    public boolean authenticate(String username, String password) {
        if (username == null || password == null) {
            return false;
        }

        lock.lock();
        try {
            User user = users.get(username.toLowerCase());
            if (user == null) {
                return false;
            }
            String testHash = hashPassword(password, user.salt);
            return testHash.equals(user.hashedPassword);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Registers a session socket associated with an authenticated username.
     */
    public void registerSession(SocketChannel channel, String username) {
        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(username, "username cannot be null");

        lock.lock();
        try {
            User user = users.get(username.toLowerCase());
            if (user != null) {
                activeSessions.put(channel, user);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unregisters session sockets when clients disconnect.
     */
    public void closeSession(SocketChannel channel) {
        lock.lock();
        try {
            activeSessions.remove(channel);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks if the session holding the socket channel possesses the required permission.
     */
    public boolean hasPermission(SocketChannel channel, String permission) {
        lock.lock();
        try {
            User user = activeSessions.get(channel);
            if (user == null) {
                return false; // Unauthenticated connection has no permissions
            }
            Set<String> perms = ROLE_PERMISSIONS.get(user.role);
            return perms != null && perms.contains(permission);
        } finally {
            lock.unlock();
        }
    }

    private String hashPassword(String password, byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(salt);
            byte[] hashed = md.digest(password.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hashed);
        } catch (Exception e) {
            throw new RuntimeException("Failed to hash password", e);
        }
    }

    private static record User(
            String username,
            String hashedPassword,
            byte[] salt,
            String role
    ) {}
}

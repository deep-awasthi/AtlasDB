package com.atlasdb.backup;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.security.crypto.AesEngine;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Command-line interface tool for executing database snapshots, logs backup, and PITR recovery.
 */
public final class BackupCli {

    public static void main(String[] args) {
        if (args.length < 4) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];
        String dataDirStr = args[1];
        String targetFileStr = args[2];
        String secretPassphrase = args[3];

        try {
            // Generate standard AES key from passphrase
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] keyBytes = sha.digest(secretPassphrase.getBytes(StandardCharsets.UTF_8));
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            AesEngine aes = new AesEngine(keySpec);

            File dataDir = new File(dataDirStr);
            VersionGenerator vg = new VersionGenerator();
            HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(16, 0.75f), vg);

            if ("--snapshot".equalsIgnoreCase(command)) {
                System.out.println("Generating database snapshot to: " + targetFileStr);
                // Load engine data if exists (using fake or WAL logic)
                BackupManager.captureSnapshot(engine, new File(targetFileStr), aes);
                System.out.println("Snapshot generated successfully.");

            } else if ("--restore".equalsIgnoreCase(command)) {
                if (args.length < 5) {
                    System.out.println("Error: Restore command requires targetTimestamp parameter.");
                    printUsage();
                    System.exit(1);
                }
                long targetTimestamp = Long.parseLong(args[4]);
                System.out.println("Restoring database to point-in-time: " + targetTimestamp);

                File snapshotFile = new File(targetFileStr);
                byte[] snapshotBytes = BackupManager.loadBackupBytes(snapshotFile, aes);

                List<byte[]> incrementals = new ArrayList<>();
                for (int i = 5; i < args.length; i++) {
                    System.out.println("Loading incremental log backup: " + args[i]);
                    incrementals.add(BackupManager.loadBackupBytes(new File(args[i]), aes));
                }

                BackupManager.recoverToPointInTime(engine, snapshotBytes, incrementals, targetTimestamp);
                System.out.println("Database recovery complete. Restored size: " + engine.size() + " records.");

            } else {
                System.out.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
            }

        } catch (Exception e) {
            System.err.println("CLI Execution Failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  BackupCli --snapshot <dataDir> <targetSnapshotFile> <passphrase>");
        System.out.println("  BackupCli --restore <dataDir> <sourceSnapshotFile> <passphrase> <targetTimestamp> [incrementalFiles...]");
    }
}

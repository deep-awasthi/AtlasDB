#!/bin/sh
if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <dataDir> <snapshotFile> <passphrase> <targetTimestamp> [incrementalFiles...]"
  exit 1
fi

if [ -f "/app/atlas-server.jar" ]; then
  JAR="/app/atlas-server.jar"
else
  JAR="$(dirname "$0")/../atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar"
fi

if [ ! -f "$JAR" ]; then
  echo "ERROR: Server JAR not found at $JAR"
  echo "Run 'mvn clean package -DskipTests' from the project root first."
  exit 1
fi

java -cp "$JAR" com.atlasdb.backup.BackupCli --restore "$@"


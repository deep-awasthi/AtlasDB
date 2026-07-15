#!/bin/sh
HOST=${1:-127.0.0.1}
PORT=${2:-7601}
REFRESH_MS=${3:-1000}

JAR="$(dirname "$0")/../atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Server JAR not found at $JAR"
  echo "Run 'mvn clean package -DskipTests' from the project root first."
  exit 1
fi

java -cp "$JAR" com.atlasdb.server.TerminalDashboard "$HOST" "$PORT" "$REFRESH_MS"

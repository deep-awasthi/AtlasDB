# Stage 1: Build the Maven multi-module project
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Stage 2: Runtime image containing executable JRE
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Copy the fat executable JAR built by the assembly plugin
COPY --from=builder /app/atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar /app/atlas-server.jar

# Copy scripts and make them executable
COPY deployment/backup.sh /app/backup.sh
COPY deployment/restore.sh /app/restore.sh
COPY deployment/diagnose.sh /app/diagnose.sh
RUN chmod +x /app/backup.sh /app/restore.sh /app/diagnose.sh

# Expose ports: Client socket, Cluster discovery, Monitoring HTTP
EXPOSE 8601 9601 7601

# Run the boot loader class
ENTRYPOINT ["java", "-jar", "/app/atlas-server.jar"]


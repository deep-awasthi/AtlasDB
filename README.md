# AtlasDB

**AtlasDB** is a production-grade, distributed, dual-mode database engine written in pure Java — operable as a **NoSQL key-value store**, a **SQL relational database**, or **both simultaneously** in Hybrid mode. It is configurable via a single environment variable with no code changes required.

---

## Features

- **Dual-Mode Engine** — Switch between SQL, NoSQL, or Hybrid at boot time via `DB_MODE`
- **MVCC Storage Engine** — Multi-Version Concurrency Control with garbage collection
- **SQL Engine** — `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, JOINs, `GROUP BY`, aggregates
- **Transactions** — Snapshot Isolation, Optimistic/Pessimistic concurrency, 2PC, deadlock detection
- **Write-Ahead Log (WAL)** — Durable crash-safe persistence with WAL replay on startup
- **Cluster Membership** — Gossip-based node discovery, heartbeats, failure detection
- **Raft Consensus** — Leader election, log replication, and automatic failover
- **Replication** — Quorum-based reads/writes with read-repair anti-entropy
- **Security** — TLS/SSL, RBAC with salted SHA-256 password hashing, AES-GCM encryption-at-rest
- **Backup & PITR** — Snapshot + incremental WAL backups, Point-in-Time Recovery
- **Monitoring** — Prometheus `/metrics`, JMX MBeans, liveness/readiness HTTP probes
- **Deployment** — Docker, Docker Compose (3-node), Kubernetes StatefulSet

---

## Step-by-Step Setup & Running Guide

### Step 1: Git and Workspace Setup
Initialize your git repository (using the root [.gitignore](file:///Users/deepawasthi/Developer/AtlasDB/.gitignore) file we set up):
```bash
git init
git add .
git commit -m "Initial commit of AtlasDB codebase"
```

### Step 2: Build the Project
AtlasDB requires **Java 21** and **Maven 3.9+** (configured in [pom.xml](file:///Users/deepawasthi/Developer/AtlasDB/pom.xml)). Compile and package the multi-module project:
```bash
mvn clean package -DskipTests
```
This builds all submodules and generates the executable fat JAR at:
`atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar`

### Step 3: Run the Integration Tests
Verify that all core components (Raft, replication, transactions, SQL, storage engine) work correctly by running the integration tests:
```bash
# Run all integration tests
mvn clean test

# Run only mode-switching tests
mvn -pl atlas-tests test -Dtest=ModeSwitchingIntegrationTest

# Run only SQL tests
mvn -pl atlas-tests test -Dtest=SqlIntegrationTest
```

---

## Running a Single Node Locally

You can configure the database mode on boot using the `DB_MODE` environment variable.

### Option A: NoSQL Key-Value Mode
Accepts key-value packets and rejects SQL statements.
```bash
DB_MODE=NOSQL \
NODE_ID=node1 \
CLIENT_PORT=8601 \
java -jar atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar
```

#### Wire Format & Operations Example
Connect to the server using the client class `TcpClient` in [TcpClient.java](file:///Users/deepawasthi/Developer/AtlasDB/atlas-network/src/main/java/com/atlasdb/network/client/TcpClient.java).

```java
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

TcpClient client = new TcpClient("127.0.0.1", 8601, 5000);
client.connect();

// 1. PUT key-value pair
byte[] key = "user:42".getBytes(StandardCharsets.UTF_8);
byte[] val = "{\"name\":\"Alice\",\"city\":\"Boston\"}".getBytes(StandardCharsets.UTF_8);
ByteBuffer payload = ByteBuffer.allocate(4 + key.length + 4 + val.length);
payload.putInt(key.length); payload.put(key);
payload.putInt(val.length); payload.put(val);

Packet putResp = client.send(new Packet(Packet.TYPE_REQ_PUT, 1, 0, payload.array()));

// 2. GET key value
Packet getResp = client.send(new Packet(Packet.TYPE_REQ_GET, 2, 0, "user:42".getBytes(StandardCharsets.UTF_8)));
ByteBuffer buf = ByteBuffer.wrap(getResp.getPayload());
long version = buf.getLong(); // MVCC version timestamp
byte[] valueBytes = new byte[buf.getInt()];
buf.get(valueBytes);
System.out.println(new String(valueBytes)); // {"name":"Alice","city":"Boston"}

// 3. DELETE key
Packet delResp = client.send(new Packet(Packet.TYPE_REQ_DELETE, 3, 0, "user:42".getBytes(StandardCharsets.UTF_8)));

client.close();
```

*Packet Specs:*
* `PUT` $\rightarrow$ `Packet(TYPE_REQ_PUT, requestId, 0, [4-byte keyLen][keyBytes][4-byte valLen][valBytes])`
* `GET` $\rightarrow$ `Packet(TYPE_REQ_GET, requestId, 0, [keyBytes])`
* `DEL` $\rightarrow$ `Packet(TYPE_REQ_DELETE, requestId, 0, [keyBytes])`

### Option B: SQL Relational Mode
Accepts SQL statements and rejects raw key-value packets.
```bash
DB_MODE=SQL \
NODE_ID=node1 \
CLIENT_PORT=8601 \
java -jar atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar
```

#### SQL Client Example
```java
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import java.nio.charset.StandardCharsets;

TcpClient client = new TcpClient("127.0.0.1", 8601, 5000);
client.connect();

// Helper to execute SQL queries
Packet execute(String sql, long reqId) throws Exception {
    return client.send(new Packet(Packet.TYPE_REQ_SQL, reqId, 0, sql.getBytes(StandardCharsets.UTF_8)));
}

// CREATE TABLE
execute("CREATE TABLE users (id INT, name VARCHAR, city VARCHAR)", 1);

// INSERT Rows
execute("INSERT INTO users (id, name, city) VALUES (1, 'Alice', 'Boston')", 2);
execute("INSERT INTO users (id, name, city) VALUES (2, 'Bob', 'NewYork')", 3);

// SELECT all rows
Packet selectAll = execute("SELECT * FROM users", 4);
System.out.println(new String(selectAll.getPayload()));
// id | name  | city
// 1  | Alice | Boston
// 2  | Bob   | NewYork

// SELECT with WHERE
Packet selectWhere = execute("SELECT name, city FROM users WHERE id = 2", 5);
System.out.println(new String(selectWhere.getPayload()));

// UPDATE & DELETE
execute("UPDATE users SET city = 'Chicago' WHERE id = 2", 6);
execute("DELETE FROM users WHERE id = 1", 7);

client.close();
```

#### Supported SQL Commands

| Statement | Example |
|---|---|
| `CREATE TABLE` | `CREATE TABLE users (id INT, name VARCHAR, city VARCHAR)` |
| `INSERT` | `INSERT INTO users (id, name) VALUES (1, 'Alice')` |
| `SELECT` | `SELECT * FROM users` |
| `SELECT` with `WHERE` | `SELECT name FROM users WHERE id = 1` |
| `SELECT` with `JOIN` | `SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id` |
| `SELECT` with `GROUP BY` + aggregates | `SELECT city, COUNT(*) FROM users GROUP BY city` |
| `UPDATE` | `UPDATE users SET city = 'LA' WHERE id = 1` |
| `DELETE` | `DELETE FROM users WHERE id = 1` |

### Option C: Hybrid Mode (Default)
Enables both SQL and NoSQL interfaces concurrently on the same port, allowing you to use both access patterns against the same underlying storage.
```bash
DB_MODE=HYBRID \
NODE_ID=node1 \
CLIENT_PORT=8601 \
java -jar atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar
```

---

## Cluster Deployments

### 1. Multi-Node Cluster with Docker Compose
A 3-node gossip-backed cluster config is defined in [docker-compose.yml](file:///Users/deepawasthi/Developer/AtlasDB/deployment/docker-compose.yml).
```bash
cd deployment/

# Build and start the cluster
docker-compose up --build
```
Nodes will expose ports:
- **Node 1:** client port `8601`, metrics port `7601`
- **Node 2:** client port `8602`, metrics port `7602`
- **Node 3:** client port `8603`, metrics port `7603`

### 2. Multi-Node Cluster with Kubernetes
A StatefulSet deployment config is located in [kubernetes.yaml](file:///Users/deepawasthi/Developer/AtlasDB/deployment/kubernetes.yaml).
```bash
# Deploy a 3-replica StatefulSet
kubectl apply -f deployment/kubernetes.yaml

# Monitor Pod creation and status
kubectl get pods -l app=atlasdb

# Stream logs from the leader node (atlasdb-0)
kubectl logs atlasdb-0 -f
```

---

## Monitoring and Diagnostics

Every node runs a metrics HTTP server on `MONITORING_PORT` (default `7601`).

| Endpoint | Description |
|---|---|
| `GET /metrics` | Prometheus scraped metrics (DB size, Tx counts, etc.) |
| `GET /health/liveness` | Node health checking (`UP` / `DOWN`) |
| `GET /health/readiness` | Cluster consensus readiness state |

### Running Node Diagnostics
Run the [diagnose.sh](file:///Users/deepawasthi/Developer/AtlasDB/deployment/diagnose.sh) shell script to fetch liveness, readiness, and metrics summary:
```bash
./deployment/diagnose.sh 7601
```

### Real-Time Terminal Dashboard
Launch the ANSI interactive dashboard using [dashboard.sh](file:///Users/deepawasthi/Developer/AtlasDB/deployment/dashboard.sh):
```bash
# Connect to default 127.0.0.1:7601
./deployment/dashboard.sh

# Custom host, port, and refresh interval (in ms)
./deployment/dashboard.sh 127.0.0.1 7601 500
```
Alternatively, invoke the main class from the target jar:
```bash
java -cp atlas-server/target/atlas-server-1.0.0-SNAPSHOT.jar \
     com.atlasdb.server.TerminalDashboard [host] [metricsPort] [refreshMs]
```

---

## Backup & Restore (PITR)

AtlasDB provides full snapshot and point-in-time recovery (PITR) log replication via the [backup.sh](file:///Users/deepawasthi/Developer/AtlasDB/deployment/backup.sh) and [restore.sh](file:///Users/deepawasthi/Developer/AtlasDB/deployment/restore.sh) scripts.

### 1. Local Execution (Host Machine)
Ensure the project is built via Maven first, so the CLI tool finds the local jar:

```bash
# Create a snapshot backup
./deployment/backup.sh <dataDir> <outputFile.bak> <passphrase>

# Restore data to a specific timestamp from snapshot
./deployment/restore.sh <dataDir> <snapshotFile.bak> <passphrase> <targetTimestamp>

# Point-in-Time Recovery (PITR) with incremental WAL logs
./deployment/restore.sh <dataDir> <snapshotFile.bak> <passphrase> <targetTimestamp> wal1.bak wal2.bak
```

### 2. Container Execution (Docker / Kubernetes)
The script files are copied directly into the Docker image ([Dockerfile](file:///Users/deepawasthi/Developer/AtlasDB/deployment/Dockerfile)). You can run commands inside the running containers directly:

**Docker Compose:**
```bash
# Backup node1 data
docker exec -it atlasdb-node1 /app/backup.sh /var/lib/atlasdb/data /var/lib/atlasdb/backup_v1.bak mySecretPass

# Restore node1 data to a timestamp
docker exec -it atlasdb-node1 /app/restore.sh /var/lib/atlasdb/data /var/lib/atlasdb/backup_v1.bak mySecretPass 1700000000
```

**Kubernetes Pods:**
```bash
# Backup pod data
kubectl exec -it atlasdb-0 -- /app/backup.sh /var/lib/atlasdb/data /var/lib/atlasdb/backup_v1.bak mySecretPass
```

---

## Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | `node1` | Unique identifier for this node |
| `HOST` | `127.0.0.1` | Bind address |
| `CLIENT_PORT` | `8601` | TCP port for client connections |
| `CLUSTER_PORT` | `9601` | TCP port for inter-node gossip |
| `MONITORING_PORT` | `7601` | HTTP port for metrics and health probes |
| `DB_MODE` | `HYBRID` | Database mode: `SQL`, `NOSQL`, or `HYBRID` |
| `DATA_DIR` | `data/<NODE_ID>` | Directory for WAL and data files |
| `SEEDS` | *(empty)* | Comma-separated `host:port` of known cluster seed nodes |

---

## Module Architecture

```
atlasdb/
├── atlas-common        # VersionGenerator, shared utilities
├── atlas-storage       # MVCC HashStorageEngine, WAL, secondary indexing
├── atlas-network       # Binary TCP protocol, TcpServer (SQL+NoSQL routing), TcpClient
├── atlas-transaction   # Transaction lifecycle, Snapshot Isolation, deadlock detection
├── atlas-sql           # SQL lexer, parser, AST, executor (DDL + DML + DQL)
├── atlas-cluster       # Gossip membership, heartbeat, failure detection
├── atlas-replication   # Quorum reads/writes, read-repair anti-entropy
├── atlas-raft          # Raft leader election and log replication
├── atlas-security      # TLS/SSL, RBAC, AES-GCM encryption-at-rest
├── atlas-backup        # Snapshot + incremental WAL backup, PITR recovery
├── atlas-monitoring    # Micrometer/Prometheus metrics, JMX MBeans, HTTP health server
├── atlas-server        # Executable fat JAR boot loader (ServerRunner)
├── atlas-benchmark     # JMH micro-benchmarks
└── atlas-tests         # All integration tests
```

package com.atlasdb.server;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Real-time ANSI terminal dashboard for AtlasDB node performance.
 *
 * Usage:
 *   java -cp atlas-server.jar com.atlasdb.server.TerminalDashboard [host] [metricsPort] [refreshMs]
 *
 * Defaults: host=127.0.0.1  metricsPort=7601  refreshMs=1000
 */
public final class TerminalDashboard {

    // ── ANSI escape codes ──────────────────────────────────────────────────────
    private static final String RESET    = "\033[0m";
    private static final String BOLD     = "\033[1m";
    private static final String DIM      = "\033[2m";
    private static final String RED      = "\033[31m";
    private static final String GREEN    = "\033[32m";
    private static final String YELLOW   = "\033[33m";
    private static final String CYAN     = "\033[36m";
    private static final String WHITE    = "\033[37m";
    private static final String MAGENTA  = "\033[35m";
    private static final String BG_DARK  = "\033[40m";
    private static final String CLEAR    = "\033[2J\033[H";

    // ── Box-drawing characters ────────────────────────────────────────────────
    private static final String TL = "╔", TR = "╗", BL = "╚", BR = "╝";
    private static final String H  = "═", V  = "║";
    private static final String ML = "╠", MR = "╣", MT = "╦", MB = "╩", MC = "╬";

    private static final int WIDTH = 72;

    public static void main(String[] args) throws Exception {
        String host       = args.length > 0 ? args[0] : "127.0.0.1";
        int    port       = args.length > 1 ? Integer.parseInt(args[1]) : 7601;
        long   refreshMs  = args.length > 2 ? Long.parseLong(args[2]) : 1000;

        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        System.out.print(CLEAR);
        hideCursor();

        // Previous metric snapshot for delta/throughput calculations
        Map<String, Double> prev = new HashMap<>();
        Instant startTime = Instant.now();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            showCursor();
            System.out.println();
            System.out.println(CYAN + "AtlasDB Dashboard stopped." + RESET);
        }));

        while (!Thread.currentThread().isInterrupted()) {
            long tickStart = System.currentTimeMillis();

            String rawMetrics = scrape(http, host, port, "/metrics");
            String liveness   = scrape(http, host, port, "/health/liveness");
            String readiness  = scrape(http, host, port, "/health/readiness");

            Map<String, Double> metrics = parsePrometheus(rawMetrics);

            long elapsed = Duration.between(startTime, Instant.now()).getSeconds();
            double deltaSeconds = refreshMs / 1000.0;

            double dbSize        = metrics.getOrDefault("atlasdb_database_size", 0.0);
            double activeTx      = metrics.getOrDefault("atlasdb_transactions_active", 0.0);
            double committedTx   = metrics.getOrDefault("atlasdb_transactions_committed_total", 0.0);
            double abortedTx     = metrics.getOrDefault("atlasdb_transactions_aborted_total", 0.0);
            double netRequests   = metrics.getOrDefault("atlasdb_network_requests_total_total", 0.0);
            double latencySum    = metrics.getOrDefault("atlasdb_network_requests_duration_seconds_sum", 0.0);
            double latencyCount  = metrics.getOrDefault("atlasdb_network_requests_duration_seconds_count", 0.0);

            // Throughput deltas
            double prevCommitted = prev.getOrDefault("atlasdb_transactions_committed_total", committedTx);
            double prevAborted   = prev.getOrDefault("atlasdb_transactions_aborted_total", abortedTx);
            double prevRequests  = prev.getOrDefault("atlasdb_network_requests_total_total", netRequests);
            double prevLatSum    = prev.getOrDefault("atlasdb_network_requests_duration_seconds_sum", latencySum);
            double prevLatCount  = prev.getOrDefault("atlasdb_network_requests_duration_seconds_count", latencyCount);

            double txPerSec   = (committedTx - prevCommitted) / deltaSeconds;
            double reqPerSec  = (netRequests  - prevRequests)  / deltaSeconds;
            double latDelta   = latencyCount - prevLatCount;
            double avgLatMs   = latDelta > 0
                    ? ((latencySum - prevLatSum) / latDelta) * 1000.0
                    : 0.0;

            prev.put("atlasdb_transactions_committed_total",               committedTx);
            prev.put("atlasdb_transactions_aborted_total",                 abortedTx);
            prev.put("atlasdb_network_requests_total_total",               netRequests);
            prev.put("atlasdb_network_requests_duration_seconds_sum",      latencySum);
            prev.put("atlasdb_network_requests_duration_seconds_count",    latencyCount);

            // JVM memory
            Runtime rt = Runtime.getRuntime();
            long heapUsed  = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
            long heapTotal = rt.totalMemory() / (1024 * 1024);
            long heapMax   = rt.maxMemory()   / (1024 * 1024);
            int  heapPct   = (int) (heapUsed * 100.0 / heapMax);

            boolean live  = "UP".equalsIgnoreCase(liveness.trim());
            boolean ready = "READY".equalsIgnoreCase(readiness.trim());

            // ── Render ─────────────────────────────────────────────────────────
            StringBuilder sb = new StringBuilder();
            sb.append(CLEAR);

            // Title bar
            sb.append(BOLD).append(CYAN);
            sb.append(TL).append(H.repeat(WIDTH)).append(TR).append("\n");
            sb.append(V);
            String title = "  AtlasDB  Real-Time Performance Dashboard";
            sb.append(centerPad(BOLD + CYAN + title + RESET + BOLD + CYAN, WIDTH));
            sb.append(V).append("\n");

            String subline = String.format("  Host: %s:%d   Uptime: %s   Refresh: %dms",
                    host, port, formatUptime(elapsed), refreshMs);
            sb.append(V);
            sb.append(DIM + WHITE);
            sb.append(pad(subline, WIDTH));
            sb.append(RESET + BOLD + CYAN + V).append("\n");
            sb.append(RESET);

            // ── Row 1 header: DATABASE | TRANSACTIONS ─────────────────────────
            sb.append(CYAN).append(ML).append(H.repeat(34)).append(MT).append(H.repeat(36)).append(MR).append("\n").append(RESET);

            sb.append(CYAN + V + RESET + BOLD + "  DATABASE                         " + CYAN + V + RESET + BOLD + "  TRANSACTIONS                       " + CYAN + V + RESET + "\n");
            sb.append(CYAN + V + RESET);

            sb.append(formatRow(String.format("  DB Size:      %s records", fmtLong((long) dbSize)), 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Active:       %s", fmtLong((long) activeTx)), 36));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(formatRow("", 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Committed:    %s", fmtLong((long) committedTx)), 36));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(formatRow("", 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Aborted:      %s", fmtLong((long) abortedTx)), 36));
            sb.append(CYAN + V + RESET + "\n");

            // ── Row 2 header: THROUGHPUT | NETWORK ───────────────────────────
            sb.append(CYAN).append(ML).append(H.repeat(34)).append(MC).append(H.repeat(36)).append(MR).append("\n").append(RESET);

            sb.append(CYAN + V + RESET + BOLD + "  THROUGHPUT  (per second)          " + CYAN + V + RESET + BOLD + "  NETWORK                             " + CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(txThroughput("  Txns/s:  ", txPerSec, 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Total Requests: %s", fmtLong((long) netRequests)), 36));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(txThroughput("  Req/s:   ", reqPerSec, 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Avg Latency:    %.2f ms", avgLatMs), 36));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(formatRow("", 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow("", 36));
            sb.append(CYAN + V + RESET + "\n");

            // ── Row 3 header: MEMORY | HEALTH ─────────────────────────────────
            sb.append(CYAN).append(ML).append(H.repeat(34)).append(MC).append(H.repeat(36)).append(MR).append("\n").append(RESET);

            sb.append(CYAN + V + RESET + BOLD + "  MEMORY  (JVM Heap)                " + CYAN + V + RESET + BOLD + "  HEALTH PROBES                       " + CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Used:  %d MB / %d MB  (%d%%)", heapUsed, heapMax, heapPct), 34));
            sb.append(CYAN + V + RESET);
            String liveStr = live  ? GREEN + "✓ UP"    + RESET : RED + "✗ DOWN" + RESET;
            sb.append(padAnsi(String.format("  Liveness:   " + liveStr), 36, 36 + (live ? GREEN.length() + RESET.length() : RED.length() + RESET.length())));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(heapBar(heapUsed, heapMax, 34));
            sb.append(CYAN + V + RESET);
            String readyStr = ready ? GREEN + "✓ READY" + RESET : YELLOW + "⚠ NOT READY" + RESET;
            sb.append(padAnsi(String.format("  Readiness:  " + readyStr), 36, 36 + (ready ? GREEN.length() + RESET.length() : YELLOW.length() + RESET.length())));
            sb.append(CYAN + V + RESET + "\n");

            sb.append(CYAN + V + RESET);
            sb.append(formatRow(String.format("  Total: %d MB  Free: %d MB", heapTotal, rt.freeMemory() / (1024 * 1024)), 34));
            sb.append(CYAN + V + RESET);
            sb.append(formatRow("", 36));
            sb.append(CYAN + V + RESET + "\n");

            // ── Footer ────────────────────────────────────────────────────────
            sb.append(CYAN).append(BL).append(H.repeat(34)).append(MB).append(H.repeat(36)).append(BR).append("\n").append(RESET);

            String footer = DIM + "  Monitoring: http://" + host + ":" + port + "/metrics  │  Press Ctrl+C to exit" + RESET;
            sb.append(footer).append("\n");

            System.out.print(sb);

            long tickMs = System.currentTimeMillis() - tickStart;
            long sleepMs = Math.max(0, refreshMs - tickMs);
            Thread.sleep(sleepMs);
        }
    }

    // ── Metric scraper ──────────────────────────────────────────────────────────

    private static String scrape(HttpClient http, String host, int port, String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + port + path))
                    .timeout(Duration.ofSeconds(2))
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.body();
        } catch (Exception e) {
            return "";
        }
    }

    // ── Prometheus text format parser ──────────────────────────────────────────

    private static Map<String, Double> parsePrometheus(String text) {
        Map<String, Double> map = new HashMap<>();
        if (text == null || text.isBlank()) return map;
        for (String line : text.split("\n")) {
            line = line.trim();
            if (line.startsWith("#") || line.isEmpty()) continue;
            int sp = line.lastIndexOf(' ');
            if (sp < 0) continue;
            String key = line.substring(0, sp).replaceAll("\\{[^}]*}", "").trim();
            String val = line.substring(sp + 1).trim();
            try {
                map.put(key, Double.parseDouble(val));
            } catch (NumberFormatException ignored) {}
        }
        return map;
    }

    // ── Rendering helpers ───────────────────────────────────────────────────────

    private static String centerPad(String s, int width) {
        int visible = visibleLength(s);
        int pad = Math.max(0, width - visible);
        int left  = pad / 2;
        int right = pad - left;
        return " ".repeat(left) + s + " ".repeat(right);
    }

    private static String pad(String s, int width) {
        int visible = visibleLength(s);
        int right = Math.max(0, width - visible);
        return s + " ".repeat(right);
    }

    private static String padAnsi(String s, int targetVisible, int bufLen) {
        int visible = visibleLength(s);
        int right = Math.max(0, targetVisible - visible);
        return s + " ".repeat(right);
    }

    private static String formatRow(String s, int width) {
        int visible = visibleLength(s);
        int right = Math.max(0, width - visible);
        return s + " ".repeat(right);
    }

    private static String txThroughput(String label, double val, int width) {
        String color = val > 500 ? GREEN : val > 100 ? YELLOW : WHITE;
        String s = label + color + String.format("%,8.1f", val) + RESET;
        int visible = visibleLength(s);
        int right = Math.max(0, width - visible);
        return s + " ".repeat(right);
    }

    private static String heapBar(long used, long max, int width) {
        int barWidth = 26;
        int filled = (int) (used * barWidth / Math.max(1, max));
        String color = filled > barWidth * 0.8 ? RED : filled > barWidth * 0.5 ? YELLOW : GREEN;
        String bar = color + "█".repeat(filled) + DIM + "░".repeat(barWidth - filled) + RESET;
        String s = "  [" + bar + "]";
        int visible = visibleLength(s);
        int right = Math.max(0, width - visible);
        return s + " ".repeat(right);
    }

    // ── Utilities ───────────────────────────────────────────────────────────────

    private static String fmtLong(long v) {
        return String.format("%,d", v);
    }

    private static String formatUptime(long seconds) {
        long h = seconds / 3600, m = (seconds % 3600) / 60, s = seconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    /** Strips ANSI sequences to measure printable length. */
    private static int visibleLength(String s) {
        return s.replaceAll("\033\\[[0-9;]*m", "").length();
    }

    private static void hideCursor() { System.out.print("\033[?25l"); }
    private static void showCursor() { System.out.print("\033[?25h"); }
}

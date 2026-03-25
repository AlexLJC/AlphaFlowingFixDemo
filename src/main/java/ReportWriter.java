import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class ReportWriter {
    private ReportWriter() {
    }

    static void write(
            final Path runDir,
            final RuntimeOptions options,
            final List<CaseResult> results,
            final Set<String> inboundFromCases,
            final EventRecorder recorder
    ) throws IOException {
        final Set<String> inbound = new HashSet<>(inboundFromCases);
        inbound.addAll(recorder.inboundTypes());
        final List<String> inboundSorted = new ArrayList<>(inbound);
        inboundSorted.sort(Comparator.naturalOrder());
        writeJson(runDir, options, results, inboundSorted);
        writeMarkdown(runDir, options, results, inboundSorted);
        Files.write(
                runDir.resolve("sanitized-session.log"),
                recorder.lines(),
                StandardCharsets.UTF_8
        );
    }

    private static void writeJson(
            final Path runDir,
            final RuntimeOptions options,
            final List<CaseResult> results,
            final List<String> inboundTypes
    ) throws IOException {
        final long pass = results.stream().filter(r -> r.status() == CaseStatus.PASS).count();
        final long fail = results.stream().filter(r -> r.status() == CaseStatus.FAIL).count();
        final long skip = results.stream().filter(r -> r.status() == CaseStatus.SKIP).count();
        final StringBuilder out = new StringBuilder();
        out.append("{\n");
        jsonField(out, "generatedAt", Instant.now().toString(), true, 2);
        jsonField(out, "suite", options.suite().name().toLowerCase(Locale.ROOT), true, 2);
        jsonField(out, "host", options.host(), true, 2);
        out.append("  \"port\": ").append(options.port()).append(",\n");
        jsonField(out, "targetCompId", options.targetCompId(), true, 2);
        jsonField(out, "clientId", options.clientId(), true, 2);
        jsonField(out, "symbol", options.symbol(), true, 2);
        out.append("  \"summary\": {\"pass\": ").append(pass)
                .append(", \"fail\": ").append(fail)
                .append(", \"skip\": ").append(skip).append("},\n");
        out.append("  \"observedInboundMsgTypes\": [");
        for (int i = 0; i < inboundTypes.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            out.append("\"").append(escapeJson(inboundTypes.get(i))).append("\"");
        }
        out.append("],\n");
        out.append("  \"cases\": [\n");
        for (int i = 0; i < results.size(); i++) {
            final CaseResult r = results.get(i);
            out.append("    {\n");
            jsonField(out, "id", r.spec().id(), true, 6);
            jsonField(out, "section", r.spec().section(), true, 6);
            jsonField(out, "name", r.spec().name(), true, 6);
            jsonField(out, "expected", r.spec().expected(), true, 6);
            jsonField(out, "status", r.status().name(), true, 6);
            out.append("      \"durationMs\": ").append(r.durationMs()).append(",\n");
            jsonField(out, "reason", r.reason(), true, 6);
            out.append("      \"notes\": ").append(jsonArray(r.notes())).append(",\n");
            out.append("      \"evidence\": ").append(jsonArray(r.evidence())).append("\n");
            out.append("    }");
            if (i < results.size() - 1) {
                out.append(",");
            }
            out.append("\n");
        }
        out.append("  ]\n");
        out.append("}\n");
        Files.writeString(
                runDir.resolve("conformance-report.json"),
                out.toString(),
                StandardCharsets.UTF_8
        );
    }

    private static void writeMarkdown(
            final Path runDir,
            final RuntimeOptions options,
            final List<CaseResult> results,
            final List<String> inboundTypes
    ) throws IOException {
        final StringBuilder md = new StringBuilder();
        md.append("# Conformance Report\n\n");
        md.append("- Host: ").append(options.host()).append(":").append(options.port()).append("\n");
        md.append("- TargetCompID: ").append(options.targetCompId()).append("\n");
        md.append("- ClientID: ").append(options.clientId()).append("\n");
        md.append("- Symbol: ").append(options.symbol()).append("\n");
        md.append("- Suite: ").append(options.suite().name().toLowerCase(Locale.ROOT)).append("\n\n");
        md.append("Observed inbound MsgType: ").append(String.join(", ", inboundTypes)).append("\n\n");
        md.append("| Case | Section | Status | Expected |\n");
        md.append("| --- | --- | --- | --- |\n");
        for (final CaseResult r : results) {
            md.append("| ").append(r.spec().id()).append(" ").append(r.spec().name())
                    .append(" | ").append(r.spec().section())
                    .append(" | ").append(r.status())
                    .append(" | ").append(r.spec().expected().replace("|", "\\|"))
                    .append(" |\n");
        }
        md.append("\n## Failures\n\n");
        boolean hasFail = false;
        for (final CaseResult r : results) {
            if (r.status() != CaseStatus.FAIL) {
                continue;
            }
            hasFail = true;
            md.append("### ").append(r.spec().id()).append(" ").append(r.spec().name()).append("\n\n");
            md.append("- reason: ").append(r.reason()).append("\n");
            if (!r.notes().isEmpty()) {
                md.append("- notes: ").append(String.join("; ", r.notes())).append("\n");
            }
            if (!r.evidence().isEmpty()) {
                md.append("\n```text\n");
                for (final String line : r.evidence()) {
                    md.append(line).append("\n");
                }
                md.append("```\n");
            }
            md.append("\n");
        }
        if (!hasFail) {
            md.append("No failures.\n");
        }
        Files.writeString(
                runDir.resolve("conformance-report.md"),
                md.toString(),
                StandardCharsets.UTF_8
        );
    }

    private static void jsonField(
            final StringBuilder out,
            final String key,
            final String value,
            final boolean comma,
            final int indent
    ) {
        out.append(" ".repeat(Math.max(indent, 0)))
                .append("\"").append(escapeJson(key)).append("\": ")
                .append("\"").append(escapeJson(value == null ? "" : value))
                .append("\"");
        if (comma) {
            out.append(",");
        }
        out.append("\n");
    }

    private static String jsonArray(final List<String> values) {
        final StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            out.append("\"").append(escapeJson(values.get(i))).append("\"");
        }
        out.append("]");
        return out.toString();
    }

    private static String escapeJson(final String text) {
        return text
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}

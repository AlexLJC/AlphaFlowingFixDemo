import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportWriterSmokeTest {
    @Test
    void reportWriterProducesMdJsonAndSanitizedLog() throws Exception {
        Properties file = new Properties();
        file.setProperty("fixdemo.host", "demo-host");
        file.setProperty("fixdemo.port", "9877");
        file.setProperty("fixdemo.targetCompId", "ALPHAFLOW");
        file.setProperty("fixdemo.clientId", "demo-client");
        file.setProperty("fixdemo.passwordPlain", "123");
        file.setProperty("fixdemo.symbol", "EURUSD-A");
        file.setProperty(
                "fixdemo.dictionary.path",
                Path.of("src", "main", "resources", "FIX44-ALPHAFLOW-CLIENT.xml")
                        .toAbsolutePath()
                        .normalize()
                        .toString()
        );
        file.setProperty("fixdemo.tls.enabledProtocols", "TLSv1.2,TLSv1.3");
        file.setProperty("fixdemo.suite", "all");
        file.setProperty("fixdemo.report.out", "target/conformance");
        file.setProperty("fixdemo.onlyLogon", "false");

        RuntimeOptions options = RuntimeOptions.fromSources(Map.of(), new Properties(), file);
        EventRecorder recorder = new EventRecorder();
        List<CaseResult> results = List.of(
                CaseResult.pass(
                        CaseSpec.authPositive(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis(),
                        List.of("ok"),
                        List.of("evidence")
                )
        );

        Path runDir = Files.createTempDirectory("fixdemo-report-test");
        ReportWriter.write(runDir, options, results, Set.of("A"), recorder);

        Path md = runDir.resolve("conformance-report.md");
        Path json = runDir.resolve("conformance-report.json");
        Path log = runDir.resolve("sanitized-session.log");

        assertTrue(Files.exists(md));
        assertTrue(Files.exists(json));
        assertTrue(Files.exists(log));
        assertTrue(Files.readString(md).contains("Conformance Report"));
    }
}

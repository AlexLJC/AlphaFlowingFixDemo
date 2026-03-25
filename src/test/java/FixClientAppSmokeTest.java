import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixClientAppSmokeTest {
    private static final String[] FIX_PROPS = {
            "fixdemo.configFile",
            "fixdemo.host",
            "fixdemo.port",
            "fixdemo.targetCompId",
            "fixdemo.clientId",
            "fixdemo.passwordPlain",
            "fixdemo.symbol",
            "fixdemo.dictionary.path",
            "fixdemo.tls.trustStore",
            "fixdemo.tls.trustStorePassword",
            "fixdemo.tls.enabledProtocols",
            "fixdemo.suite",
            "fixdemo.report.out",
            "fixdemo.onlyLogon"
    };

    @AfterEach
    void cleanup() {
        for (String key : FIX_PROPS) {
            System.clearProperty(key);
        }
    }

    @Test
    void sha256HashUsesLowerHex() {
        String hash = FixClientApp.sha256LowerHex("123");
        assertEquals(
                "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                hash
        );
    }

    @Test
    void sanitizeFixWireMasks553And554() {
        String raw = "8=FIX.4.4\u000135=A\u000149=abc\u0001553=abc\u0001554=secret\u0001";
        String masked = FixClientApp.sanitizeFixWire(raw);
        assertTrue(masked.contains("553=<masked>"));
        assertTrue(masked.contains("554=<masked_sha256>"));
        assertTrue(!masked.contains("secret"));
    }

    @Test
    void runtimeOptionsFailFastOnMissingRequiredTarget() {
        Properties system = new Properties();
        Properties file = baseConfig("123");
        file.remove("fixdemo.targetCompId");
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeOptions.fromSources(Map.of(), system, file)
        );
        assertTrue(ex.getMessage().contains("fixdemo.targetCompId"));
    }

    @Test
    void runtimeOptionsFailFastOnMissingRequiredHost() {
        Properties system = new Properties();
        Properties file = baseConfig("123");
        file.remove("fixdemo.host");
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeOptions.fromSources(Map.of(), system, file)
        );
        assertTrue(ex.getMessage().contains("fixdemo.host"));
    }

    @Test
    void runtimeOptionsLoadsConfiguredValues() {
        Properties system = new Properties();
        Properties file = baseConfig("123");
        file.setProperty("fixdemo.suite", "negative");

        RuntimeOptions options = RuntimeOptions.fromSources(Map.of(), system, file);

        assertEquals(Suite.NEGATIVE, options.suite());
        assertEquals("EURUSD-A", options.symbol());
        assertEquals(
                "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                options.passwordHash()
        );
    }

    @Test
    void envPasswordTakesPriorityOverSystemAndConfig() {
        Properties system = new Properties();
        system.setProperty("fixdemo.passwordPlain", "fromSystem");
        Properties file = baseConfig("fromFile");

        RuntimeOptions options = RuntimeOptions.fromSources(
                Map.of("FIXDEMO_PASSWORD_PLAIN", "fromEnv"),
                system,
                file
        );

        assertEquals("fromEnv", options.passwordPlain());
        assertEquals(FixClientApp.sha256LowerHex("fromEnv"), options.passwordHash());
    }

    @Test
    void systemPropertyOverridesConfigFileValues() {
        Properties system = new Properties();
        system.setProperty("fixdemo.suite", "smoke");
        Properties file = baseConfig("123");
        file.setProperty("fixdemo.suite", "all");

        RuntimeOptions options = RuntimeOptions.fromSources(Map.of(), system, file);

        assertEquals(Suite.SMOKE, options.suite());
    }

    @Test
    void dictionaryPathMustExistWhenExplicitlyConfigured() {
        Properties system = new Properties();
        Properties file = baseConfig("123");
        file.setProperty("fixdemo.dictionary.path", "target/not-found-dictionary.xml");

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeOptions.fromSources(Map.of(), system, file)
        );
        assertTrue(ex.getMessage().contains("dictionary missing"));
    }

    @Test
    void fromSystemFailsWhenConfigFileNotProvided() {
        System.clearProperty("fixdemo.configFile");
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                RuntimeOptions::fromSystem
        );
        assertTrue(ex.getMessage().contains("fixdemo.configFile"));
    }

    @Test
    void fromSystemFailsWhenConfigFileMissing() {
        System.setProperty("fixdemo.configFile", "target/does-not-exist.properties");
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                RuntimeOptions::fromSystem
        );
        assertTrue(ex.getMessage().contains("config file not found"));
    }

    @Test
    void fromSystemFailsWhenConfigFileMalformed() throws Exception {
        Path bad = Files.createTempFile("fixdemo-bad", ".properties");
        Files.writeString(bad, "broken=\\u12");
        System.setProperty("fixdemo.configFile", bad.toString());

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                RuntimeOptions::fromSystem
        );
        assertTrue(ex.getMessage().contains("failed to read config file"));
    }

    private Properties baseConfig(String password) {
        Properties file = new Properties();
        file.setProperty("fixdemo.host", "demo-host");
        file.setProperty("fixdemo.port", "9877");
        file.setProperty("fixdemo.targetCompId", "ALPHAFLOW");
        file.setProperty("fixdemo.clientId", "demo-client");
        file.setProperty("fixdemo.passwordPlain", password);
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
        return file;
    }
}

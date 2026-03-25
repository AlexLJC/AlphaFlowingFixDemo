import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

record RuntimeOptions(
        String host,
        int port,
        String targetCompId,
        String clientId,
        String passwordPlain,
        String passwordHash,
        String symbol,
        Path dictionaryPath,
        Path trustStore,
        String trustStorePassword,
        String enabledProtocols,
        Suite suite,
        Path reportOut,
        boolean onlyLogon
) {
    static RuntimeOptions fromSystem() {
        final Properties fileProps = loadConfigProperties();
        final Properties system = new Properties();
        system.putAll(System.getProperties());
        return fromSources(System.getenv(), system, fileProps);
    }

    static RuntimeOptions fromSources(
            final Map<String, String> env,
            final Properties system,
            final Properties fileProps
    ) {
        final String host = required("fixdemo.host", system, fileProps);
        final int port = parseInt(required("fixdemo.port", system, fileProps), "fixdemo.port");
        final String target = required("fixdemo.targetCompId", system, fileProps);
        final String client = required("fixdemo.clientId", system, fileProps);
        final String passwordPlain = passwordPlain(env, system, fileProps);
        final String passwordHash = FixClientApp.sha256LowerHex(passwordPlain);
        final String symbol = required("fixdemo.symbol", system, fileProps);

        final Path dict = Paths.get(required("fixdemo.dictionary.path", system, fileProps))
                .toAbsolutePath()
                .normalize();
        if (!Files.exists(dict)) {
            throw new IllegalArgumentException(
                    "dictionary missing: " + dict
                            + " (set fixdemo.dictionary.path via -D or config file)"
            );
        }

        final String trustRaw = value("fixdemo.tls.trustStore", system, fileProps);
        final Path trust = trustRaw == null || trustRaw.isBlank()
                ? null
                : Paths.get(trustRaw).toAbsolutePath().normalize();
        final String trustPass = value("fixdemo.tls.trustStorePassword", system, fileProps);
        final String enabledProtocols = required(
                "fixdemo.tls.enabledProtocols",
                system,
                fileProps
        );
        final Suite suite = Suite.parse(required("fixdemo.suite", system, fileProps));
        final Path reportOut = Paths.get(required("fixdemo.report.out", system, fileProps))
                .toAbsolutePath()
                .normalize();
        final boolean onlyLogon = parseBoolean(
                required("fixdemo.onlyLogon", system, fileProps),
                "fixdemo.onlyLogon"
        );
        return new RuntimeOptions(
                host,
                port,
                target,
                client,
                passwordPlain,
                passwordHash,
                symbol,
                dict,
                trust,
                trustPass,
                enabledProtocols,
                suite,
                reportOut,
                onlyLogon
        );
    }

    private static Properties loadConfigProperties() {
        final String configRaw = trimToNull(System.getProperty("fixdemo.configFile"));
        if (configRaw == null) {
            throw new IllegalArgumentException(
                    "missing required fixdemo.configFile "
                            + "(set -Dfixdemo.configFile=<private-properties-path>)"
            );
        }
        final Path configPath = Paths.get(configRaw).toAbsolutePath().normalize();
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException("config file not found: " + configPath);
        }
        final Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            props.load(reader);
            return props;
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "failed to read config file: " + configPath,
                    e
            );
        }
    }

    private static String passwordPlain(
            final Map<String, String> env,
            final Properties system,
            final Properties fileProps
    ) {
        final String envPassword = trimToNull(
                env == null ? null : env.get("FIXDEMO_PASSWORD_PLAIN")
        );
        if (envPassword != null) {
            return envPassword;
        }
        final String sysPassword = trimToNull(system.getProperty("fixdemo.passwordPlain"));
        if (sysPassword != null) {
            return sysPassword;
        }
        final String filePassword = trimToNull(fileProps.getProperty("fixdemo.passwordPlain"));
        if (filePassword != null) {
            return filePassword;
        }
        throw new IllegalArgumentException(
                "missing password: set FIXDEMO_PASSWORD_PLAIN or -Dfixdemo.passwordPlain "
                        + "or fixdemo.passwordPlain in config file"
        );
    }

    private static String required(
            final String key,
            final Properties system,
            final Properties fileProps
    ) {
        final String value = value(key, system, fileProps);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "missing required " + key
                            + " (set -D" + key + " or provide in config file)"
            );
        }
        return value.trim();
    }

    private static String value(
            final String key,
            final Properties system,
            final Properties fileProps
    ) {
        final String sys = trimToNull(system.getProperty(key));
        if (sys != null) {
            return sys;
        }
        final String fromFile = trimToNull(fileProps.getProperty(key));
        if (fromFile != null) {
            return fromFile;
        }
        return null;
    }

    private static int parseInt(final String raw, final String key) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer for " + key + ": " + raw, e);
        }
    }

    private static boolean parseBoolean(final String raw, final String key) {
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("invalid boolean for " + key + ": " + raw);
    }

    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}

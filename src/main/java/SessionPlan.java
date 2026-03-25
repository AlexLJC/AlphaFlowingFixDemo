import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

record SessionPlan(
        String name,
        String senderCompId,
        String targetCompId,
        String username,
        String passwordHash,
        Path storePath,
        Path logPath
) {
    static SessionPlan main(final RuntimeOptions opt, final Path runDir)
            throws IOException {
        return create(
                "main",
                opt.clientId(),
                opt.targetCompId(),
                opt.clientId(),
                opt.passwordHash(),
                runDir
        );
    }

    static SessionPlan invalidSender(final RuntimeOptions opt, final Path runDir)
            throws IOException {
        return create(
                "auth-invalid-sender",
                opt.clientId() + "_MISMATCH",
                opt.targetCompId(),
                opt.clientId(),
                opt.passwordHash(),
                runDir
        );
    }

    static SessionPlan invalidTarget(final RuntimeOptions opt, final Path runDir)
            throws IOException {
        return create(
                "auth-invalid-target",
                opt.clientId(),
                opt.targetCompId() + "_WRONG",
                opt.clientId(),
                opt.passwordHash(),
                runDir
        );
    }

    static SessionPlan invalidCredential(
            final RuntimeOptions opt,
            final Path runDir
    ) throws IOException {
        return create(
                "auth-invalid-credential",
                opt.clientId(),
                opt.targetCompId(),
                opt.clientId(),
                FixClientApp.sha256LowerHex(opt.passwordPlain() + "#wrong"),
                runDir
        );
    }

    private static SessionPlan create(
            final String name,
            final String sender,
            final String target,
            final String username,
            final String passwordHash,
            final Path runDir
    ) throws IOException {
        final Path root = runDir.resolve("quickfix");
        final Path store = root.resolve("store-" + name);
        final Path log = root.resolve("log-" + name);
        Files.createDirectories(store);
        Files.createDirectories(log);
        return new SessionPlan(
                name,
                sender,
                target,
                username,
                passwordHash,
                store,
                log
        );
    }
}

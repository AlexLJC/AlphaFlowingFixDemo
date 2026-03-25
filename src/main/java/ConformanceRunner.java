import quickfix.field.Username;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

final class ConformanceRunner {
    private static final Duration AUTH_WAIT = FixClientApp.AUTH_WAIT;
    private static final Duration RESPONSE_WAIT = FixClientApp.RESPONSE_WAIT;
    private static final Duration MD_INCREMENTAL_WAIT = FixClientApp.MD_INCREMENTAL_WAIT;

    private static final int TAG_SENDER = FixClientApp.TAG_SENDER;
    private static final int TAG_TARGET = FixClientApp.TAG_TARGET;
    private static final int TAG_TEXT = FixClientApp.TAG_TEXT;
    private static final int TAG_CLORD = FixClientApp.TAG_CLORD;
    private static final int TAG_ORIG_CLORD = FixClientApp.TAG_ORIG_CLORD;
    private static final int TAG_ORDER_ID = FixClientApp.TAG_ORDER_ID;
    private static final int TAG_EXEC_TYPE = FixClientApp.TAG_EXEC_TYPE;
    private static final int TAG_MASS_STATUS_REQ_ID = FixClientApp.TAG_MASS_STATUS_REQ_ID;
    private static final int TAG_POS_REQ_ID = FixClientApp.TAG_POS_REQ_ID;
    private static final int TAG_SEC_REQ_ID = FixClientApp.TAG_SEC_REQ_ID;
    private static final int TAG_SECURITY_REQUEST_RESULT = FixClientApp.TAG_SECURITY_REQUEST_RESULT;
    private static final int TAG_MD_REQ_ID = FixClientApp.TAG_MD_REQ_ID;
    private static final int TAG_QOS_APPLIED = FixClientApp.TAG_QOS_APPLIED;
    private static final int TAG_ERROR_CODE = FixClientApp.TAG_ERROR_CODE;
    private static final int TAG_RETRYABLE = FixClientApp.TAG_RETRYABLE;
    private static final int TAG_BUSINESS_REJECT_REF_ID = FixClientApp.TAG_BUSINESS_REJECT_REF_ID;
    private static final int TAG_REF_MSG_TYPE = FixClientApp.TAG_REF_MSG_TYPE;

    private static final String TOKEN_INVALID_SENDER = FixClientApp.TOKEN_INVALID_SENDER;
    private static final String TOKEN_INVALID_TARGET = FixClientApp.TOKEN_INVALID_TARGET;
    private static final String TOKEN_INVALID_CREDENTIAL = FixClientApp.TOKEN_INVALID_CREDENTIAL;
    private static final String TOKEN_MD_DEPTH_RANGE = FixClientApp.TOKEN_MD_DEPTH_RANGE;
    private static final String TOKEN_MD_QOS_INVALID = FixClientApp.TOKEN_MD_QOS_INVALID;
    private static final String TOKEN_QTY_INVALID = FixClientApp.TOKEN_QTY_INVALID;
    private static final String TOKEN_SECURITY_SCOPE = FixClientApp.TOKEN_SECURITY_SCOPE;

    private final RuntimeOptions options;
    private final EventRecorder recorder = new EventRecorder();
    private final List<CaseResult> results = new ArrayList<>();
    private final AtomicLong seq = new AtomicLong(1L);
    private final Set<String> inboundTypes = new HashSet<>();
    private final String runNonce = Long.toString(System.currentTimeMillis());
    private Path runDir;

    private String marketClOrdId;
    private String marketOrderId;
    private String workingClOrdId;

    ConformanceRunner(final RuntimeOptions options) {
        this.options = options;
    }

    int run() throws Exception {
        prepareRunDir();
        runAuthCases();
        if (!options.onlyLogon()) {
            runBusinessCases();
        }
        ReportWriter.write(runDir, options, results, inboundTypes, recorder);
        final long fail = results.stream()
                .filter(r -> r.status() == CaseStatus.FAIL)
                .count();
        final long pass = results.stream()
                .filter(r -> r.status() == CaseStatus.PASS)
                .count();
        final long skip = results.stream()
                .filter(r -> r.status() == CaseStatus.SKIP)
                .count();
        System.out.println("Conformance done. pass=" + pass
                + " fail=" + fail + " skip=" + skip);
        System.out.println("report=" + runDir);
        return fail == 0 ? 0 : 1;
    }

    private void prepareRunDir() throws IOException {
        final String stamp = DateTimeFormatter
                .ofPattern("yyyyMMdd-HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        runDir = options.reportOut().resolve("run-" + stamp);
        Files.createDirectories(runDir);
    }

    private void runAuthCases() {
        runCase(CaseSpec.authPositive(), ctx -> {
            if (options.suite() == Suite.NEGATIVE) {
                throw new SkipCaseException("suite=negative");
            }
            final SessionPlan plan = SessionPlan.main(options, runDir);
            try (FixClientApp app = new FixClientApp(options, plan, recorder, true)) {
                app.start();
                final boolean ok = app.awaitLogon(AUTH_WAIT);
                if (!ok || !app.isLoggedOn()) {
                    throw new IllegalStateException("logon timeout");
                }
                final MessageRecord outLogon = awaitOrThrow(
                        ctx.marker(),
                        AUTH_WAIT,
                        r -> r.isOutbound() && "A".equals(r.msgType()),
                        "no outbound A"
                );
                final String sender = outLogon.tag(TAG_SENDER);
                final String user = outLogon.tag(Username.FIELD);
                final String target = outLogon.tag(TAG_TARGET);
                if (!Objects.equals(sender, user)) {
                    throw new IllegalStateException("49!=553");
                }
                if (!Objects.equals(target, options.targetCompId())) {
                    throw new IllegalStateException("56 mismatch");
                }
            }
        });
        if (options.onlyLogon()) {
            return;
        }

        runCase(CaseSpec.authInvalidSender(), ctx ->
                runAuthNegative(
                        ctx,
                        SessionPlan.invalidSender(options, runDir),
                        TOKEN_INVALID_SENDER
                ));
        runCase(CaseSpec.authInvalidTarget(), ctx ->
                runAuthNegative(
                        ctx,
                        SessionPlan.invalidTarget(options, runDir),
                        TOKEN_INVALID_TARGET
                ));
        runCase(CaseSpec.authInvalidCredential(), ctx ->
                runAuthNegative(
                        ctx,
                        SessionPlan.invalidCredential(options, runDir),
                        TOKEN_INVALID_CREDENTIAL
                ));
    }

    private void runAuthNegative(
            final CaseContext ctx,
            final SessionPlan plan,
            final String expectedToken
    ) throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        try (FixClientApp app = new FixClientApp(options, plan, recorder, false)) {
            app.start();
            awaitOrThrow(
                    ctx.marker(),
                    AUTH_WAIT,
                    r -> r.isOutbound()
                            && "A".equals(r.msgType())
                            && plan.senderCompId().equals(r.tag(TAG_SENDER))
                            && plan.targetCompId().equals(r.tag(TAG_TARGET)),
                    "no outbound logon attempt"
            );
            final boolean ok = app.awaitLogon(AUTH_WAIT);
            if (ok) {
                throw new IllegalStateException("unexpected auth success");
            }
            final MessageRecord reject = recorder.await(
                    ctx.marker(),
                    AUTH_WAIT,
                    r -> r.isInbound() && any(r.msgType(), "3", "5", "j")
            );
            if (reject != null) {
                expectToken(reject, expectedToken);
                captureReject(ctx, reject);
                return;
            }
            if (TOKEN_INVALID_TARGET.equals(expectedToken)) {
                final MessageRecord logout = recorder.await(
                        ctx.marker(),
                        Duration.ofSeconds(2),
                        r -> "META".equals(r.msgType()) && r.containsToken("ONLOGOUT")
                );
                if (logout != null) {
                    ctx.note("silent_disconnect_on_invalid_target");
                    return;
                }
            }
            throw new IllegalStateException("no auth reject");
        }
    }

    private void runBusinessCases() throws Exception {
        final SessionPlan plan = SessionPlan.main(options, runDir);
        try (FixClientApp app = new FixClientApp(options, plan, recorder, true)) {
            app.start();
            final boolean ok = app.awaitLogon(AUTH_WAIT);
            if (!ok || !app.isLoggedOn()) {
                throw new IllegalStateException(
                        "business session logon failed before running cases"
                );
            }
            runCase(CaseSpec.vwxPositive(), ctx -> caseVwxPositive(app, ctx));
            runCase(CaseSpec.vDepthNegative(), ctx -> caseVDepthNeg(app, ctx));
            runCase(CaseSpec.vQosNegative(), ctx -> caseVQosNeg(app, ctx));
            runCase(CaseSpec.dPositive(), ctx -> caseDPositive(app, ctx));
            runCase(CaseSpec.dReplay(), ctx -> caseDReplay(app, ctx));
            runCase(CaseSpec.dQtyNegative(), ctx -> caseDQtyNeg(app, ctx));
            runCase(CaseSpec.gPositive(), ctx -> caseGPositive(app, ctx));
            runCase(CaseSpec.gUnknownNegative(), ctx -> caseGUnknownNeg(app, ctx));
            runCase(CaseSpec.hPositive(), ctx -> caseHPositive(app, ctx));
            runCase(CaseSpec.hNotFoundNegative(), ctx -> caseHNotFoundNeg(app, ctx));
            runCase(CaseSpec.fPositive(), ctx -> caseFPositive(app, ctx));
            runCase(CaseSpec.fUnknownNegative(), ctx -> caseFUnknownNeg(app, ctx));
            runCase(CaseSpec.qPositive(), ctx -> caseQPositive(app, ctx));
            runCase(CaseSpec.qNoCancellable(), ctx -> caseQNoCancellable(app, ctx));
            runCase(CaseSpec.afPositive(), ctx -> caseAfPositive(app, ctx));
            runCase(CaseSpec.afMissingReqId(), ctx -> caseAfMissingReqId(app, ctx));
            runCase(CaseSpec.anApPositive(), ctx -> caseAnApPositive(app, ctx));
            runCase(CaseSpec.anMissingReqId(), ctx -> caseAnMissingReqId(app, ctx));
            runCase(CaseSpec.xyPositive(), ctx -> caseXyPositive(app, ctx));
            runCase(CaseSpec.xWrongTypeNegative(), ctx -> caseXWrongTypeNeg(app, ctx));
        }
    }

    private void runCase(final CaseSpec spec, final CaseExecutor executor) {
        if (!options.suite().shouldRun(spec)) {
            results.add(CaseResult.skip(spec, "suite filter"));
            return;
        }
        final long marker = recorder.mark();
        final long start = System.currentTimeMillis();
        final CaseContext ctx = new CaseContext(marker);
        try {
            executor.run(ctx);
            results.add(CaseResult.pass(
                    spec,
                    start,
                    System.currentTimeMillis(),
                    ctx.notes(),
                    recorder.tail(marker, 10)
            ));
        } catch (SkipCaseException skip) {
            results.add(CaseResult.skip(spec, skip.getMessage()));
        } catch (Exception e) {
            results.add(CaseResult.fail(
                    spec,
                    start,
                    System.currentTimeMillis(),
                    e.getMessage(),
                    ctx.notes(),
                    recorder.tail(marker, 10)
            ));
        }
    }

    private void caseVwxPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        requireSession(app);
        final String mdReqId = nextId("md");
        app.send(marketDataRequest(mdReqId, options.symbol(), 10, 12));
        final MessageRecord snapshot = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "W".equals(r.msgType())
                        && mdReqId.equals(r.tag(TAG_MD_REQ_ID)),
                "missing W"
        );
        inboundTypes.add(snapshot.msgType());
        final String applied = snapshot.tag(TAG_QOS_APPLIED);
        if (applied == null || applied.isBlank()) {
            throw new IllegalStateException("W missing 9021");
        }
        final MessageRecord inc = recorder.await(
                snapshot.index() + 1,
                MD_INCREMENTAL_WAIT,
                r -> r.isInbound()
                        && "X".equals(r.msgType())
                        && mdReqId.equals(r.tag(TAG_MD_REQ_ID))
        );
        if (inc != null) {
            inboundTypes.add(inc.msgType());
        } else {
            ctx.note("X not observed; W-only stream accepted");
        }
    }

    private void caseVDepthNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String mdReqId = nextId("md-depth");
        app.send(marketDataRequest(mdReqId, options.symbol(), 0, null));
        final MessageRecord reject = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "Y".equals(r.msgType())
                        && mdReqId.equals(r.tag(TAG_MD_REQ_ID)),
                "missing Y for depth"
        );
        expectToken(reject, TOKEN_MD_DEPTH_RANGE);
        captureReject(ctx, reject);
    }

    private void caseVQosNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String mdReqId = nextId("md-qos");
        app.send(marketDataRequest(mdReqId, options.symbol(), 10, 0));
        final MessageRecord reject = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "Y".equals(r.msgType())
                        && mdReqId.equals(r.tag(TAG_MD_REQ_ID)),
                "missing Y for qos"
        );
        expectToken(reject, TOKEN_MD_QOS_INVALID);
        captureReject(ctx, reject);
    }

    private void caseDPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        requireSession(app);
        final String clOrdId = nextId("ord-mkt");
        app.send(newOrderSingle(
                clOrdId,
                options.symbol(),
                1000.0d,
                '1',
                null,
                seq.getAndIncrement()
        ));
        final MessageRecord report = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "8".equals(r.msgType())
                        && clOrdId.equals(r.tag(TAG_CLORD)),
                "missing 8 for D"
        );
        marketClOrdId = clOrdId;
        marketOrderId = report.tag(TAG_ORDER_ID);
        inboundTypes.add(report.msgType());
    }

    private void caseDReplay(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        if (marketClOrdId == null) {
            throw new IllegalStateException("source D missing");
        }
        app.send(newOrderSingle(
                marketClOrdId,
                options.symbol(),
                1000.0d,
                '1',
                null,
                seq.getAndIncrement()
        ));
        final MessageRecord replay = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "8", "j")
                        && referencesClientRef(r, marketClOrdId),
                "missing replay response"
        );
        if ("j".equals(replay.msgType())) {
            if (!replay.containsToken("DUPLICATE_CLORDID_REPLAY_UNAVAILABLE")
                    && !replay.containsToken("REPLAY")) {
                throw new IllegalStateException("replay reject semantics missing");
            }
            captureReject(ctx, replay);
            return;
        }
        final String replayOrderId = replay.tag(TAG_ORDER_ID);
        if (marketOrderId != null
                && replayOrderId != null
                && !marketOrderId.equals(replayOrderId)) {
            throw new IllegalStateException("replay orderId changed");
        }
    }

    private void caseDQtyNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String bad = nextId("ord-bad-qty");
        app.send(newOrderSingle(
                bad,
                options.symbol(),
                0.0d,
                '1',
                null,
                seq.getAndIncrement()
        ));
        final MessageRecord reject = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "8", "9", "j", "3")
                        && referencesClientRef(r, bad),
                "missing reject for bad qty"
        );
        if (!reject.containsToken(TOKEN_QTY_INVALID)
                && !reject.containsToken("QTY")) {
            throw new IllegalStateException("expected qty token");
        }
        captureReject(ctx, reject);
    }

    private void caseGPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        final String current = ensureWorkingOrder(app);
        final String repl = nextId("repl");
        app.send(replaceRequest(
                repl,
                current,
                options.symbol(),
                1500.0d,
                0.00010d
        ));
        awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "8".equals(r.msgType())
                        && repl.equals(r.tag(TAG_CLORD)),
                "missing 8 for G"
        );
        workingClOrdId = repl;
    }

    private void caseGUnknownNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String cl = nextId("repl-miss");
        app.send(replaceRequest(
                cl,
                "not-exist-" + nextId("orig"),
                options.symbol(),
                1200.0d,
                0.00011d
        ));
        final MessageRecord reject = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "9", "8", "j")
                        && cl.equals(r.tag(TAG_CLORD)),
                "missing reject for unknown G"
        );
        if (!"9".equals(reject.msgType())
                && !reject.containsToken("UNKNOWN")
                && !reject.containsToken("NOT_FOUND")) {
            throw new IllegalStateException("unknown G reject not detected");
        }
        captureReject(ctx, reject);
    }

    private void caseHPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        final String current = ensureWorkingOrder(app);
        app.send(statusRequest(current));
        final MessageRecord report = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "8".equals(r.msgType())
                        && current.equals(r.tag(TAG_CLORD)),
                "missing 8 for H"
        );
        final String execType = safe(report.tag(TAG_EXEC_TYPE));
        if (!"I".equals(execType)) {
            ctx.note("150 expected I, actual=" + execType);
        }
    }

    private void caseHNotFoundNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String req = "status-not-found-" + nextId("id");
        app.send(statusRequest(req));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "8", "9", "j")
                        && req.equals(r.tag(TAG_CLORD)),
                "missing status not-found response"
        );
        if (!res.containsToken("NOT_FOUND")
                && !res.containsToken("UNKNOWN")
                && !"8".equals(res.msgType())) {
            throw new IllegalStateException("not-found semantics missing");
        }
        captureReject(ctx, res);
    }

    private void caseFPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        final String current = ensureWorkingOrder(app);
        final String cxl = nextId("cxl");
        app.send(cancelRequest(cxl, current, options.symbol()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "8", "9")
                        && cxl.equals(r.tag(TAG_CLORD)),
                "missing response for F"
        );
        if (!"8".equals(res.msgType())) {
            throw new IllegalStateException("F positive expected 8");
        }
        workingClOrdId = null;
    }

    private void caseFUnknownNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String cxl = nextId("cxl-miss");
        app.send(cancelRequest(
                cxl,
                "not-exist-" + nextId("orig"),
                options.symbol()
        ));
        final MessageRecord reject = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && any(r.msgType(), "9", "8", "j")
                        && cxl.equals(r.tag(TAG_CLORD)),
                "missing reject for unknown F"
        );
        if (!"9".equals(reject.msgType())
                && !reject.containsToken("UNKNOWN")
                && !reject.containsToken("NOT_FOUND")) {
            throw new IllegalStateException("unknown F reject not detected");
        }
        captureReject(ctx, reject);
    }

    private void caseQPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        ensureWorkingOrder(app);
        final String req = nextId("mass-cxl");
        app.send(massCancelRequest(req, options.symbol()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound() && any(r.msgType(), "8", "9", "j"),
                "missing response for q"
        );
        if (!"8".equals(res.msgType())) {
            ctx.note("q positive returned " + res.msgType());
        }
        workingClOrdId = null;
    }

    private void caseQNoCancellable(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String req = nextId("mass-cxl-empty");
        app.send(massCancelRequest(req, options.symbol()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound() && any(r.msgType(), "8", "9", "j"),
                "missing response for empty q"
        );
        ctx.note("msgType=" + res.msgType());
        captureReject(ctx, res);
    }

    private void caseAfPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        ensureWorkingOrder(app);
        final String req = nextId("mass-status");
        app.send(massStatusRequest(req, options.clientId()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound() && "8".equals(r.msgType()),
                "missing 8 for AF"
        );
        inboundTypes.add(res.msgType());
    }

    private void caseAfMissingReqId(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        app.send(massStatusMissingReqId(options.clientId()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound() && any(r.msgType(), "8", "3", "j", "9"),
                "missing response for AF without 584"
        );
        ctx.note("msgType=" + res.msgType());
        if ("8".equals(res.msgType())) {
            captureReject(ctx, res);
            return;
        }
        if (res.containsToken("584")
                || res.containsToken("MASSSTATUSREQID")
                || res.containsToken("CORRELATION")) {
            ctx.note("degraded-correlation-visible");
            captureReject(ctx, res);
            return;
        }
        throw new IllegalStateException("AF missing 584 not processed");
    }

    private void caseAnApPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.NEGATIVE) {
            throw new SkipCaseException("suite=negative");
        }
        requireSession(app);
        final String req = nextId("pos");
        app.send(positionsRequest(req, options.clientId()));
        final MessageRecord report = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "AP".equals(r.msgType())
                        && req.equals(r.tag(TAG_POS_REQ_ID)),
                "missing AP for AN"
        );
        inboundTypes.add(report.msgType());
    }

    private void caseAnMissingReqId(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        app.send(positionsMissingReqId(options.clientId()));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> (r.isInbound() && any(r.msgType(), "AP", "AO", "8"))
                        || (r.isInbound()
                        && any(r.msgType(), "3", "j")
                        && any(r.tag(TAG_REF_MSG_TYPE), "AN", "AP"))
                        || (r.isOutbound()
                        && "3".equals(r.msgType())
                        && "AP".equals(r.tag(TAG_REF_MSG_TYPE))),
                "missing response for AN without 710"
        );
        ctx.note("msgType=" + res.msgType() + " dir=" + (res.isInbound() ? "IN" : "OUT"));
        if (res.isOutbound() && "3".equals(res.msgType())) {
            ctx.note("client-dictionary-reject-on-AP");
            captureReject(ctx, res);
            return;
        }
        if (res.isInbound() && any(res.msgType(), "AP", "AO", "8", "3", "j")) {
            inboundTypes.add(res.msgType());
            captureReject(ctx, res);
            return;
        }
        throw new IllegalStateException("AN missing 710 not processed");
    }

    private void caseXyPositive(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        requireSession(app);
        final String req = nextId("sec");
        app.send(securityListRequest(req, "FOR", 4));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "y".equals(r.msgType())
                        && req.equals(r.tag(TAG_SEC_REQ_ID)),
                "missing y for x"
        );
        final String result = safe(res.tag(TAG_SECURITY_REQUEST_RESULT));
        if (!"0".equals(result) && !"2".equals(result)) {
            throw new IllegalStateException("x positive expected 560 in {0,2}");
        }
        if ("2".equals(result)) {
            ctx.note("560=2 (no instruments for positive filter)");
        }
        ctx.note("560=" + result);
        inboundTypes.add(res.msgType());
    }

    private void caseXWrongTypeNeg(final FixClientApp app, final CaseContext ctx)
            throws Exception {
        if (options.suite() == Suite.SMOKE) {
            throw new SkipCaseException("suite=smoke");
        }
        requireSession(app);
        final String req = nextId("sec-bad");
        app.send(securityListRequest(req, "FUT", 5));
        final MessageRecord res = awaitOrThrow(
                ctx.marker(),
                RESPONSE_WAIT,
                r -> (r.isInbound()
                        && "y".equals(r.msgType())
                        && req.equals(r.tag(TAG_SEC_REQ_ID)))
                        || (r.isInbound()
                        && any(r.msgType(), "j", "3")
                        && "x".equals(r.tag(TAG_REF_MSG_TYPE))),
                "missing response for x no-match"
        );
        if (!"y".equals(res.msgType())) {
            throw new IllegalStateException(
                    "x no-match expected y, actual " + res.msgType()
            );
        }
        final String result = safe(res.tag(TAG_SECURITY_REQUEST_RESULT));
        if (!"2".equals(result)) {
            throw new IllegalStateException("x no-match expected 560=2");
        }
        ctx.note("560=" + result);
    }

    private String ensureWorkingOrder(final FixClientApp app) throws Exception {
        if (workingClOrdId != null) {
            return workingClOrdId;
        }
        final String cl = nextId("ord-work");
        final long marker = recorder.mark();
        app.send(newOrderSingle(
                cl,
                options.symbol(),
                2000.0d,
                '2',
                0.00001d,
                seq.getAndIncrement()
        ));
        awaitOrThrow(
                marker,
                RESPONSE_WAIT,
                r -> r.isInbound()
                        && "8".equals(r.msgType())
                        && cl.equals(r.tag(TAG_CLORD)),
                "missing working order ack"
        );
        workingClOrdId = cl;
        return cl;
    }

    private void requireSession(final FixClientApp app) {
        if (!app.isLoggedOn()) {
            throw new IllegalStateException("session not logged on");
        }
    }

    private MessageRecord awaitOrThrow(
            final long marker,
            final Duration timeout,
            final Predicate<MessageRecord> predicate,
            final String message
    ) throws Exception {
        final MessageRecord record = recorder.await(marker, timeout, predicate);
        if (record == null) {
            throw new IllegalStateException(message);
        }
        return record;
    }

    private void expectToken(final MessageRecord record, final String token) {
        if (record.containsToken(token)) {
            return;
        }
        for (final String alias : tokenAliases(token)) {
            if (record.containsToken(alias)) {
                return;
            }
        }
        throw new IllegalStateException("missing token " + token);
    }

    private List<String> tokenAliases(final String token) {
        return switch (token) {
            case TOKEN_INVALID_SENDER -> List.of(
                    "SENDERCOMPID MUST EQUAL USERNAME",
                    "49 MUST EQUAL 553",
                    "49!=553"
            );
            case TOKEN_INVALID_TARGET -> List.of(
                    "TARGETCOMPID",
                    "56 MISMATCH",
                    "UNKNOWN TARGET",
                    "INVALID TARGET"
            );
            case TOKEN_INVALID_CREDENTIAL -> List.of(
                    "INVALID USERNAME/PASSWORD",
                    "INVALID PASSWORD",
                    "INVALID CREDENTIAL"
            );
            case TOKEN_MD_DEPTH_RANGE -> List.of(
                    "MARKETDEPTH(264)",
                    "WITHIN 1..30",
                    "264"
            );
            case TOKEN_SECURITY_SCOPE -> List.of(
                    "CLIENTINSTRUMENT",
                    "SECURITYTYPE",
                    "FIELD=167"
            );
            default -> List.of();
        };
    }

    private void captureReject(final CaseContext ctx, final MessageRecord reject) {
        final String text = reject.tag(TAG_TEXT);
        if (text != null && !text.isBlank()) {
            ctx.note("58=" + text);
        } else {
            ctx.note("58 missing");
        }
        final String code = reject.tag(TAG_ERROR_CODE);
        if (code != null && !code.isBlank()) {
            ctx.note("9017=" + code);
        }
        final String retry = reject.tag(TAG_RETRYABLE);
        if (retry != null && !retry.isBlank()) {
            ctx.note("9018=" + retry);
        }
    }

    private boolean referencesClientRef(
            final MessageRecord record,
            final String clientRef
    ) {
        return clientRef.equals(record.tag(TAG_CLORD))
                || clientRef.equals(record.tag(TAG_ORIG_CLORD))
                || clientRef.equals(record.tag(TAG_BUSINESS_REJECT_REF_ID))
                || record.containsToken(clientRef);
    }

    private String nextId(final String prefix) {
        return prefix + "-" + runNonce + "-" + seq.getAndIncrement();
    }

    private static quickfix.Message marketDataRequest(
            final String mdReqId,
            final String symbol,
            final int depth,
            final Integer qos
    ) {
        return FixClientApp.marketDataRequest(mdReqId, symbol, depth, qos);
    }

    private static quickfix.Message newOrderSingle(
            final String clOrdId,
            final String symbol,
            final double qty,
            final char ordType,
            final Double price,
            final long extRef
    ) {
        return FixClientApp.newOrderSingle(clOrdId, symbol, qty, ordType, price, extRef);
    }

    private static quickfix.Message cancelRequest(
            final String clOrdId,
            final String origClOrdId,
            final String symbol
    ) {
        return FixClientApp.cancelRequest(clOrdId, origClOrdId, symbol);
    }

    private static quickfix.Message replaceRequest(
            final String clOrdId,
            final String origClOrdId,
            final String symbol,
            final double qty,
            final double price
    ) {
        return FixClientApp.replaceRequest(clOrdId, origClOrdId, symbol, qty, price);
    }

    private static quickfix.Message statusRequest(final String clOrdId) {
        return FixClientApp.statusRequest(clOrdId);
    }

    private static quickfix.Message massCancelRequest(
            final String reqId,
            final String symbol
    ) {
        return FixClientApp.massCancelRequest(reqId, symbol);
    }

    private static quickfix.Message massStatusRequest(final String reqId, final String account) {
        return FixClientApp.massStatusRequest(reqId, account);
    }

    private static quickfix.Message massStatusMissingReqId(final String account) {
        return FixClientApp.massStatusMissingReqId(account);
    }

    private static quickfix.Message positionsRequest(final String reqId, final String account) {
        return FixClientApp.positionsRequest(reqId, account);
    }

    private static quickfix.Message positionsMissingReqId(final String account) {
        return FixClientApp.positionsMissingReqId(account);
    }

    private static quickfix.Message securityListRequest(
            final String reqId,
            final String secType,
            final Integer product
    ) {
        return FixClientApp.securityListRequest(reqId, secType, product);
    }

    private static boolean any(final String value, final String... options) {
        return FixClientApp.any(value, options);
    }

    private static String safe(final String value) {
        return FixClientApp.safe(value);
    }
}

interface CaseExecutor {
    void run(CaseContext context) throws Exception;
}

final class CaseContext {
    private final long marker;
    private final List<String> notes = new ArrayList<>();

    CaseContext(final long marker) {
        this.marker = marker;
    }

    long marker() {
        return marker;
    }

    void note(final String note) {
        if (note != null && !note.isBlank()) {
            notes.add(note);
        }
    }

    List<String> notes() {
        return List.copyOf(notes);
    }
}

final class SkipCaseException extends RuntimeException {
    SkipCaseException(final String message) {
        super(message);
    }
}




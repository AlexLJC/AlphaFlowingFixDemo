import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.Group;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;
import quickfix.field.Password;
import quickfix.field.ResetSeqNumFlag;
import quickfix.field.Username;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.time.LocalDateTime;

/**
 * UAT conformance connector for AlphaFlowing FIX client guide.
 */
public final class FixClientApp implements Application, AutoCloseable {
    static final String BEGIN_STRING = "FIX.4.4";

    static final int TAG_MSG_TYPE = 35;
    static final int TAG_SENDER = 49;
    static final int TAG_TARGET = 56;
    static final int TAG_TEXT = 58;
    static final int TAG_ACCOUNT = 1;
    static final int TAG_CLORD = 11;
    static final int TAG_ORIG_CLORD = 41;
    static final int TAG_ORDER_ID = 37;
    static final int TAG_EXEC_TYPE = 150;
    static final int TAG_ORD_STATUS = 39;
    static final int TAG_SYMBOL = 55;
    static final int TAG_SIDE = 54;
    static final int TAG_QTY = 38;
    static final int TAG_ORD_TYPE = 40;
    static final int TAG_PRICE = 44;
    static final int TAG_TIF = 59;
    static final int TAG_TRANSACT_TIME = 60;
    static final int TAG_MASS_CANCEL_REQ_TYPE = 530;
    static final int TAG_MASS_STATUS_REQ_ID = 584;
    static final int TAG_POS_REQ_ID = 710;
    static final int TAG_POS_REQ_TYPE = 724;
    static final int TAG_SEC_REQ_ID = 320;
    static final int TAG_SECURITY_LIST_REQ_TYPE = 559;
    static final int TAG_SECURITY_REQUEST_RESULT = 560;
    static final int TAG_SECURITY_TYPE = 167;
    static final int TAG_PRODUCT = 460;
    static final int TAG_MD_REQ_ID = 262;
    static final int TAG_SUB_REQ_TYPE = 263;
    static final int TAG_MD_DEPTH = 264;
    static final int TAG_NO_MD_TYPES = 267;
    static final int TAG_MD_ENTRY_TYPE = 269;
    static final int TAG_NO_RELATED_SYM = 146;
    static final int TAG_NO_MD_ENTRIES = 268;
    static final int TAG_CL_ORD_LINK_ID = 583;
    static final int TAG_QOS_REQ = 9020;
    static final int TAG_QOS_APPLIED = 9021;
    static final int TAG_ERROR_CODE = 9017;
    static final int TAG_RETRYABLE = 9018;
    static final int TAG_BUSINESS_REJECT_REF_ID = 379;
    static final int TAG_REF_MSG_TYPE = 372;

    static final String TOKEN_INVALID_SENDER = "FIX_AUTH_INVALID_SENDER";
    static final String TOKEN_INVALID_TARGET = "FIX_AUTH_INVALID_TARGET";
    static final String TOKEN_INVALID_CREDENTIAL = "FIX_AUTH_INVALID_CREDENTIAL";
    static final String TOKEN_MD_DEPTH_RANGE = "FIX_MD_DEPTH_RANGE";
    static final String TOKEN_MD_QOS_INVALID = "FIX_MD_QOS_INVALID";
    static final String TOKEN_QTY_INVALID = "FIX_QTY_INVALID";
    static final String TOKEN_SECURITY_SCOPE =
            "FIX_SECURITY_SCOPE_ONLY_CUSTOMER_INSTRUMENT";

    static final Duration AUTH_WAIT = Duration.ofSeconds(18);
    static final Duration RESPONSE_WAIT = Duration.ofSeconds(25);
    static final Duration MD_INCREMENTAL_WAIT = Duration.ofSeconds(35);

    private final RuntimeOptions options;
    private final SessionPlan sessionPlan;
    private final EventRecorder recorder;
    private final boolean strictIdentity;
    private final CountDownLatch logonLatch = new CountDownLatch(1);
    private final AtomicBoolean loggedOn = new AtomicBoolean(false);
    private final AtomicReference<SessionID> sessionRef = new AtomicReference<>();
    private volatile Initiator initiator;

    FixClientApp(
            final RuntimeOptions options,
            final SessionPlan sessionPlan,
            final EventRecorder recorder,
            final boolean strictIdentity
    ) {
        this.options = options;
        this.sessionPlan = sessionPlan;
        this.recorder = recorder;
        this.strictIdentity = strictIdentity;
    }

    public static void main(final String[] args) throws Exception {
        final RuntimeOptions options = RuntimeOptions.fromSystem();
        final ConformanceRunner runner = new ConformanceRunner(options);
        final int exitCode = runner.run();
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    // ---- section: session lifecycle / quickfix application ----

    void start() throws ConfigError {
        final SessionSettings settings = buildSessionSettings(options, sessionPlan);
        final MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        final quickfix.LogFactory logFactory = new SilentLogFactory();
        final MessageFactory messageFactory = new DefaultMessageFactory();
        final Initiator localInitiator = new SocketInitiator(
                this,
                storeFactory,
                settings,
                logFactory,
                messageFactory
        );
        this.initiator = localInitiator;
        recorder.meta("initiator_start " + sessionPlan.name(), null);
        localInitiator.start();
    }

    boolean awaitLogon(final Duration timeout) throws InterruptedException {
        return logonLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean isLoggedOn() {
        return loggedOn.get();
    }

    void send(final Message message) throws SessionNotFound {
        final SessionID id = sessionRef.get();
        if (id == null) {
            throw new IllegalStateException("session not ready");
        }
        Session.sendToTarget(message, id);
    }

    @Override
    public void onCreate(final SessionID sessionID) {
        sessionRef.compareAndSet(null, sessionID);
        recorder.meta("onCreate", sessionID);
    }

    @Override
    public void onLogon(final SessionID sessionID) {
        sessionRef.compareAndSet(null, sessionID);
        loggedOn.set(true);
        logonLatch.countDown();
        recorder.meta("onLogon", sessionID);
    }

    @Override
    public void onLogout(final SessionID sessionID) {
        loggedOn.set(false);
        recorder.meta("onLogout", sessionID);
    }

    @Override
    public void toAdmin(final Message message, final SessionID sessionID) {
        applyLogonFields(message);
        recorder.record(Direction.OUT_ADMIN, message, sessionID);
    }

    @Override
    public void fromAdmin(final Message message, final SessionID sessionID)
            throws FieldNotFound,
            IncorrectDataFormat,
            IncorrectTagValue,
            RejectLogon {
        recorder.record(Direction.IN_ADMIN, message, sessionID);
    }

    @Override
    public void toApp(final Message message, final SessionID sessionID)
            throws DoNotSend {
        recorder.record(Direction.OUT_APP, message, sessionID);
    }

    @Override
    public void fromApp(final Message message, final SessionID sessionID)
            throws FieldNotFound,
            UnsupportedMessageType,
            IncorrectTagValue {
        recorder.record(Direction.IN_APP, message, sessionID);
    }

    private void applyLogonFields(final Message message) {
        if (!MsgType.LOGON.equals(safeMsgType(message))) {
            return;
        }
        message.getHeader().setString(TAG_SENDER, sessionPlan.senderCompId());
        message.getHeader().setString(TAG_TARGET, sessionPlan.targetCompId());
        message.setBoolean(ResetSeqNumFlag.FIELD, true);
        message.setString(Username.FIELD, sessionPlan.username());
        message.setString(Password.FIELD, sessionPlan.passwordHash());
        if (!strictIdentity) {
            return;
        }
        try {
            final String sender = message.getHeader().getString(TAG_SENDER);
            final String username = message.getString(Username.FIELD);
            final String target = message.getHeader().getString(TAG_TARGET);
            if (!sender.equals(username)) {
                throw new IllegalStateException("49 must equal 553");
            }
            if (!target.equals(options.targetCompId())) {
                throw new IllegalStateException("56 mismatch");
            }
        } catch (FieldNotFound e) {
            throw new IllegalStateException("missing identity tags", e);
        }
    }

    @Override
    public void close() {
        final Initiator local = initiator;
        if (local != null) {
            recorder.meta("initiator_stop " + sessionPlan.name(), null);
            local.stop();
        }
    }

    static SessionSettings buildSessionSettings(
            final RuntimeOptions options,
            final SessionPlan plan
    ) throws ConfigError {
        final SessionSettings settings = new SessionSettings();
        settings.setString("ConnectionType", "initiator");
        settings.setString("HeartBtInt", "30");
        settings.setString("ReconnectInterval", "5");
        settings.setString("StartTime", "00:00:00");
        settings.setString("EndTime", "23:59:59");
        settings.setString("TimeZone", "UTC");
        settings.setString("UseDataDictionary", "Y");
        settings.setString("DataDictionary", options.dictionaryPath().toString());
        settings.setString("ResetOnLogon", "Y");
        settings.setString("ResetOnLogout", "Y");
        settings.setString("ResetOnDisconnect", "Y");
        settings.setString("SocketConnectHost", options.host());
        settings.setString("SocketConnectPort", Integer.toString(options.port()));
        settings.setString("SocketUseSSL", "Y");
        settings.setString("SocketTcpNoDelay", "Y");
        settings.setString("EnabledProtocols", options.enabledProtocols());
        settings.setString("FileStorePath", plan.storePath().toString());
        settings.setString("FileLogPath", plan.logPath().toString());
        if (options.trustStore() != null) {
            settings.setString("SocketTrustStore", options.trustStore().toString());
            if (options.trustStorePassword() != null) {
                settings.setString(
                        "SocketTrustStorePassword",
                        options.trustStorePassword()
                );
            }
        }
        final SessionID sessionID = new SessionID(
                BEGIN_STRING,
                plan.senderCompId(),
                plan.targetCompId()
        );
        settings.setString(sessionID, "BeginString", BEGIN_STRING);
        settings.setString(sessionID, "SenderCompID", plan.senderCompId());
        settings.setString(sessionID, "TargetCompID", plan.targetCompId());
        settings.setString(sessionID, "Username", plan.username());
        settings.setString(sessionID, "Password", plan.passwordHash());
        return settings;
    }

    static String sha256LowerHex(final String plain) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hash = digest.digest(plain.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String sanitizeFixWire(final String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        final String normalized = raw.replace('\u0001', '|');
        final String[] parts = normalized.split("\\|");
        final StringJoiner joiner = new StringJoiner("|");
        for (final String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            if (part.startsWith("553=")) {
                joiner.add("553=<masked>");
            } else if (part.startsWith("554=")) {
                joiner.add("554=<masked_sha256>");
            } else {
                joiner.add(part);
            }
        }
        return joiner.toString() + "|";
    }

    private static String safeMsgType(final Message message) {
        try {
            return message.getHeader().getString(TAG_MSG_TYPE);
        } catch (FieldNotFound e) {
            return "?";
        }
    }

    private static Message newMsg(final String msgType) {
        final Message message = new Message();
        message.getHeader().setString(TAG_MSG_TYPE, msgType);
        return message;
    }

    static Message marketDataRequest(
            final String mdReqId,
            final String symbol,
            final int depth,
            final Integer qos
    ) {
        final Message message = newMsg(MsgType.MARKET_DATA_REQUEST);
        message.setString(TAG_MD_REQ_ID, mdReqId);
        message.setChar(TAG_SUB_REQ_TYPE, '1');
        message.setInt(TAG_MD_DEPTH, depth);
        message.setInt(TAG_NO_MD_TYPES, 2);
        final Group bid = new Group(TAG_NO_MD_TYPES, TAG_MD_ENTRY_TYPE);
        bid.setChar(TAG_MD_ENTRY_TYPE, '0');
        message.addGroup(bid);
        final Group ask = new Group(TAG_NO_MD_TYPES, TAG_MD_ENTRY_TYPE);
        ask.setChar(TAG_MD_ENTRY_TYPE, '1');
        message.addGroup(ask);
        message.setInt(TAG_NO_RELATED_SYM, 1);
        final Group symbolGroup = new Group(TAG_NO_RELATED_SYM, TAG_SYMBOL);
        symbolGroup.setString(TAG_SYMBOL, symbol);
        message.addGroup(symbolGroup);
        if (qos != null) {
            message.setInt(TAG_QOS_REQ, qos);
        }
        return message;
    }

    static Message newOrderSingle(
            final String clOrdId,
            final String symbol,
            final double qty,
            final char ordType,
            final Double price,
            final long extRef
    ) {
        final Message message = newMsg(MsgType.ORDER_SINGLE);
        message.setString(TAG_CLORD, clOrdId);
        message.setString(TAG_SYMBOL, symbol);
        message.setChar(TAG_SIDE, '1');
        message.setDouble(TAG_QTY, qty);
        message.setChar(TAG_ORD_TYPE, ordType);
        message.setChar(TAG_TIF, '0');
        message.setUtcTimeStamp(
                TAG_TRANSACT_TIME,
                LocalDateTime.now(ZoneOffset.UTC),
                true
        );
        message.setString(TAG_CL_ORD_LINK_ID, Long.toString(extRef));
        if (price != null) {
            message.setDouble(TAG_PRICE, price);
        }
        return message;
    }

    static Message cancelRequest(
            final String clOrdId,
            final String origClOrdId,
            final String symbol
    ) {
        final Message message = newMsg(MsgType.ORDER_CANCEL_REQUEST);
        message.setString(TAG_CLORD, clOrdId);
        message.setString(TAG_ORIG_CLORD, origClOrdId);
        message.setString(TAG_SYMBOL, symbol);
        message.setChar(TAG_SIDE, '1');
        message.setUtcTimeStamp(
                TAG_TRANSACT_TIME,
                LocalDateTime.now(ZoneOffset.UTC),
                true
        );
        return message;
    }

    static Message replaceRequest(
            final String clOrdId,
            final String origClOrdId,
            final String symbol,
            final double qty,
            final double price
    ) {
        final Message message = newMsg(MsgType.ORDER_CANCEL_REPLACE_REQUEST);
        message.setString(TAG_CLORD, clOrdId);
        message.setString(TAG_ORIG_CLORD, origClOrdId);
        message.setString(TAG_SYMBOL, symbol);
        message.setChar(TAG_SIDE, '1');
        message.setDouble(TAG_QTY, qty);
        message.setChar(TAG_ORD_TYPE, '2');
        message.setDouble(TAG_PRICE, price);
        message.setUtcTimeStamp(
                TAG_TRANSACT_TIME,
                LocalDateTime.now(ZoneOffset.UTC),
                true
        );
        return message;
    }

    static Message statusRequest(final String clOrdId) {
        final Message message = newMsg(MsgType.ORDER_STATUS_REQUEST);
        message.setString(TAG_CLORD, clOrdId);
        return message;
    }

    static Message massCancelRequest(
            final String reqId,
            final String symbol
    ) {
        final Message message = newMsg(MsgType.ORDER_MASS_CANCEL_REQUEST);
        message.setString(TAG_CLORD, reqId);
        message.setInt(TAG_MASS_CANCEL_REQ_TYPE, 7);
        message.setString(TAG_SYMBOL, symbol);
        message.setUtcTimeStamp(
                TAG_TRANSACT_TIME,
                LocalDateTime.now(ZoneOffset.UTC),
                true
        );
        return message;
    }

    static Message massStatusRequest(final String reqId, final String account) {
        final Message message = newMsg(MsgType.ORDER_MASS_STATUS_REQUEST);
        message.setString(TAG_MASS_STATUS_REQ_ID, reqId);
        if (account != null && !account.isBlank()) {
            message.setString(TAG_ACCOUNT, account);
        }
        return message;
    }

    static Message massStatusMissingReqId(final String account) {
        final Message message = newMsg(MsgType.ORDER_MASS_STATUS_REQUEST);
        if (account != null && !account.isBlank()) {
            message.setString(TAG_ACCOUNT, account);
        }
        return message;
    }

    static Message positionsRequest(final String reqId, final String account) {
        final Message message = newMsg(MsgType.REQUEST_FOR_POSITIONS);
        message.setString(TAG_POS_REQ_ID, reqId);
        message.setInt(TAG_POS_REQ_TYPE, 0);
        if (account != null && !account.isBlank()) {
            message.setString(TAG_ACCOUNT, account);
        }
        return message;
    }

    static Message positionsMissingReqId(final String account) {
        final Message message = newMsg(MsgType.REQUEST_FOR_POSITIONS);
        message.setInt(TAG_POS_REQ_TYPE, 0);
        if (account != null && !account.isBlank()) {
            message.setString(TAG_ACCOUNT, account);
        }
        return message;
    }

    static Message securityListRequest(
            final String reqId,
            final String secType,
            final Integer product
    ) {
        final Message message = newMsg(MsgType.SECURITY_LIST_REQUEST);
        message.setString(TAG_SEC_REQ_ID, reqId);
        message.setInt(TAG_SECURITY_LIST_REQ_TYPE, 4);
        if (secType != null && !secType.isBlank()) {
            message.setString(TAG_SECURITY_TYPE, secType);
        }
        if (product != null) {
            message.setInt(TAG_PRODUCT, product);
        }
        return message;
    }

    private static final class SilentLogFactory implements quickfix.LogFactory {
        @Override
        public quickfix.Log create(final SessionID sessionID) {
            return new quickfix.Log() {
                @Override
                public void clear() {
                    // no-op
                }

                @Override
                public void onIncoming(final String message) {
                    // no-op
                }

                @Override
                public void onOutgoing(final String message) {
                    // no-op
                }

                @Override
                public void onEvent(final String text) {
                    // no-op
                }

                @Override
                public void onErrorEvent(final String text) {
                    // no-op
                }
            };
        }
    }


    static boolean any(final String value, final String... options) {
        for (final String option : options) {
            if (Objects.equals(value, option)) {
                return true;
            }
        }
        return false;
    }

    static String safe(final String value) {
        return value == null ? "" : value;
    }
}


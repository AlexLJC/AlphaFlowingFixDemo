
import quickfix.*;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.field.*;
import quickfix.fix44.*;
import quickfix.fix44.MessageCracker;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FixClientApp extends MessageCracker implements Application, AutoCloseable {
    private Initiator initiator;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ConcurrentMap<SessionID, ScheduledFuture<?>> testReqTasks = new ConcurrentHashMap<>();
    private final AtomicLong testReqSeq = new AtomicLong(1);

    public static void main(String[] args) throws Exception {
        // 从 resources 读取 initiator.cfg
        try (var in = FixClientApp.class.getResourceAsStream("/initiator.cfg")) {
            if (in == null) throw new IllegalStateException("initiator.cfg not found on classpath");
            SessionSettings settings = new SessionSettings(in);

            Application app = new FixClientApp();
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new FileLogFactory(settings);
            MessageFactory msgFactory = new DefaultMessageFactory();

            Initiator initiator = new SocketInitiator(app, storeFactory, settings, logFactory, msgFactory);
            ((FixClientApp) app).initiator = initiator;

            initiator.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { ((FixClientApp) app).close(); } catch (Exception ignored) {}
            }));

            Thread.currentThread().join();
        }
    }

    // ===== Application callbacks =====
    @Override
    public void onCreate(SessionID sessionID) {

    }

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println(">> LOGON: " + sessionID);

        ScheduledFuture<?> f = scheduler.scheduleAtFixedRate(() -> {
            try {
                TestRequest tr = new TestRequest(new TestReqID("cli-" + testReqSeq.getAndIncrement()));
                Session.sendToTarget(tr, sessionID);
                System.out.println(".. sent TestRequest to " + sessionID);
            } catch (SessionNotFound ignored) {}
        }, 2, 20, TimeUnit.SECONDS);
        testReqTasks.put(sessionID, f);

        sendSecurityListRequest(sessionID, "req-1");

        sendMarketDataRequest(sessionID, "BTCUSD-R", 10);
    }

    @Override
    public void onLogout(SessionID sessionID) {
        System.out.println(">> LOGOUT: " + sessionID);
        ScheduledFuture<?> f = testReqTasks.remove(sessionID);
        if (f != null) f.cancel(false);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        System.out.println("-> ADMIN " + message);
    }

    @Override public void fromAdmin(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        System.out.println("<- ADMIN " + message);
    }

    @Override public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        System.out.println("-> APP   " + message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
        crack(message, sessionID);
        System.out.println("<- APP   " + message);
    }

    // ===== MessageCracker handlers =====

    @quickfix.MessageCracker.Handler
    public void onMessage(SecurityList secList, SessionID sessionID) throws FieldNotFound {
        int total = secList.isSetField(TotNoRelatedSym.FIELD) ? secList.getInt(TotNoRelatedSym.FIELD) : -1;
        System.out.println("<- SecurityList: TotNoRelatedSym=" + total);

        int count = secList.getInt(NoRelatedSym.FIELD);
        for (int i = 1; i <= count; i++) {
            SecurityList.NoRelatedSym g = new SecurityList.NoRelatedSym();
            secList.getGroup(i, g);
            String sym = g.isSetField(Symbol.FIELD) ? g.getString(Symbol.FIELD) : "(no 55)";
            System.out.println("   - " + sym);
        }
    }

    @quickfix.MessageCracker.Handler
    public void onMessage(MarketDataIncrementalRefresh inc, SessionID sessionID) throws FieldNotFound {
        int n = inc.getInt(NoMDEntries.FIELD);
        System.out.println("<- MDIncRefresh entries=" + n);
        for (int i = 1; i <= n; i++) {
            MarketDataIncrementalRefresh.NoMDEntries g = new MarketDataIncrementalRefresh.NoMDEntries();
            inc.getGroup(i, g);
            char typ = g.getChar(MDEntryType.FIELD);
            String id = g.isSetField(MDEntryID.FIELD) ? g.getString(MDEntryID.FIELD) : "?";
            int pos = g.isSetField(MDEntryPositionNo.FIELD) ? g.getInt(MDEntryPositionNo.FIELD) : -1;
            String sym = g.isSetField(Symbol.FIELD) ? g.getString(Symbol.FIELD) : "";
            String px  = g.isSetField(MDEntryPx.FIELD) ? String.valueOf(g.getDouble(MDEntryPx.FIELD)) : "";
            String sz  = g.isSetField(MDEntrySize.FIELD) ? String.valueOf(g.getDouble(MDEntrySize.FIELD)) : "";
            System.out.printf("   %s %s pos=%d %s px=%s sz=%s%n",
                    typ == MDEntryType.BID ? "BID " :
                            typ == MDEntryType.OFFER ? "ASK " : String.valueOf(typ),
                    id, pos, sym, px, sz);
        }
    }

    @quickfix.MessageCracker.Handler
    public void onMessage(MarketDataRequestReject rej, SessionID sessionID) throws FieldNotFound {
        String reason = rej.isSetField(Text.FIELD) ? rej.getString(Text.FIELD) : "(no reason)";
        System.out.println("<- MDR Reject: " + reason);
    }

    // ===== helpers =====
    private void sendSecurityListRequest(SessionID sid, String reqId) {
        try {
            SecurityListRequest req = new SecurityListRequest();
            req.set(new SecurityReqID(reqId));
            req.set(new SecurityListRequestType(SecurityListRequestType.SYMBOL));
            Session.sendToTarget(req, sid);
            System.out.println("-> SecurityListRequest " + reqId);
        } catch (SessionNotFound ignored) {}
    }

    private void sendMarketDataRequest(SessionID sid, String symbol, int depth) {
        try {
            MarketDataRequest mdr = new MarketDataRequest();
            mdr.set(new MDReqID("md-" + System.currentTimeMillis()));
            mdr.set(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
            mdr.set(new MarketDepth(Math.max(1, Math.min(depth, 30))));
            mdr.set(new MDUpdateType(MDUpdateType.INCREMENTAL_REFRESH)); // 265=1

            MarketDataRequest.NoMDEntryTypes t1 = new MarketDataRequest.NoMDEntryTypes();
            t1.set(new MDEntryType(MDEntryType.BID));
            mdr.addGroup(t1);
            MarketDataRequest.NoMDEntryTypes t2 = new MarketDataRequest.NoMDEntryTypes();
            t2.set(new MDEntryType(MDEntryType.OFFER));
            mdr.addGroup(t2);

            MarketDataRequest.NoRelatedSym g = new MarketDataRequest.NoRelatedSym();
            g.set(new Symbol(symbol));
            mdr.addGroup(g);

            Session.sendToTarget(mdr, sid);
            System.out.println("-> MarketDataRequest " + symbol + " depth=" + depth);
        } catch (SessionNotFound ignored) {}
    }

    @Override
    public void close() {
        System.out.println("Shutting down...");
        testReqTasks.values().forEach(f -> f.cancel(false));
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(Duration.ofSeconds(2).toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {}
        if (initiator != null) {
            initiator.stop();
        }
    }
}

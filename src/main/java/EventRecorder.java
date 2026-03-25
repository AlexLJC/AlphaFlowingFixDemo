import quickfix.Message;
import quickfix.SessionID;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

final class EventRecorder {
    private final Object lock = new Object();
    private final List<MessageRecord> records = new ArrayList<>();
    private long seq = 0L;

    void record(
            final Direction direction,
            final Message message,
            final SessionID sessionID
    ) {
        final String raw = message.toString();
        final Map<Integer, String> tags = parseTags(raw);
        final String wire = FixClientApp.sanitizeFixWire(raw);
        final String type = tags.getOrDefault(FixClientApp.TAG_MSG_TYPE, "?");
        final MessageRecord rec = new MessageRecord(
                nextSeq(),
                Instant.now(),
                direction,
                type,
                sessionID,
                wire,
                tags
        );
        synchronized (lock) {
            records.add(rec);
            lock.notifyAll();
        }
    }

    void meta(final String text, final SessionID sessionID) {
        final MessageRecord rec = new MessageRecord(
                nextSeq(),
                Instant.now(),
                Direction.META,
                "META",
                sessionID,
                "meta=" + text,
                Map.of()
        );
        synchronized (lock) {
            records.add(rec);
            lock.notifyAll();
        }
    }

    long mark() {
        synchronized (lock) {
            return seq + 1L;
        }
    }

    MessageRecord await(
            final long startSeq,
            final Duration timeout,
            final Predicate<MessageRecord> predicate
    ) throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (lock) {
            while (true) {
                for (final MessageRecord rec : records) {
                    if (rec.index() < startSeq) {
                        continue;
                    }
                    if (predicate.test(rec)) {
                        return rec;
                    }
                }
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                final long waitMs = TimeUnit.NANOSECONDS.toMillis(remaining);
                lock.wait(Math.max(waitMs, 1L));
            }
        }
    }

    List<String> tail(final long startSeq, final int max) {
        final List<String> out = new ArrayList<>();
        synchronized (lock) {
            for (final MessageRecord rec : records) {
                if (rec.index() >= startSeq) {
                    out.add(rec.line());
                }
            }
        }
        if (out.size() <= max) {
            return out;
        }
        return out.subList(0, max);
    }

    List<String> inboundTypes() {
        final Set<String> out = new HashSet<>();
        synchronized (lock) {
            for (final MessageRecord rec : records) {
                if (rec.isInbound()) {
                    out.add(rec.msgType());
                }
            }
        }
        final List<String> sorted = new ArrayList<>(out);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

    List<String> lines() {
        final List<String> out = new ArrayList<>();
        synchronized (lock) {
            for (final MessageRecord rec : records) {
                out.add(rec.line());
            }
        }
        return out;
    }

    private long nextSeq() {
        synchronized (lock) {
            seq += 1L;
            return seq;
        }
    }

    private static Map<Integer, String> parseTags(final String wire) {
        if (wire == null || wire.isBlank()) {
            return Map.of();
        }
        final Map<Integer, String> tags = new HashMap<>();
        final String normalized = wire.replace('\u0001', '|');
        final String[] parts = normalized.split("\\|");
        for (final String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            final int idx = part.indexOf('=');
            if (idx <= 0 || idx >= part.length() - 1) {
                continue;
            }
            final String key = part.substring(0, idx);
            final String value = part.substring(idx + 1);
            try {
                tags.put(Integer.parseInt(key), value);
            } catch (NumberFormatException ignored) {
                // ignore malformed tag key
            }
        }
        return tags;
    }
}

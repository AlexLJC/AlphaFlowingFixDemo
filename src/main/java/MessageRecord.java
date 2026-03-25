import quickfix.SessionID;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;

final class MessageRecord {
    private final long index;
    private final Instant timestamp;
    private final Direction direction;
    private final String msgType;
    private final SessionID sessionID;
    private final String wire;
    private final Map<Integer, String> tags;

    MessageRecord(
            final long index,
            final Instant timestamp,
            final Direction direction,
            final String msgType,
            final SessionID sessionID,
            final String wire,
            final Map<Integer, String> tags
    ) {
        this.index = index;
        this.timestamp = timestamp;
        this.direction = direction;
        this.msgType = msgType;
        this.sessionID = sessionID;
        this.wire = wire;
        this.tags = tags;
    }

    long index() {
        return index;
    }

    String msgType() {
        return msgType;
    }

    boolean isInbound() {
        return direction.inbound();
    }

    boolean isOutbound() {
        return direction.outbound();
    }

    String tag(final int tag) {
        return tags.get(tag);
    }

    boolean containsToken(final String token) {
        if (token == null || token.isBlank()) {
            return false;
        }
        final String needle = token.toUpperCase(Locale.ROOT);
        if (wire.toUpperCase(Locale.ROOT).contains(needle)) {
            return true;
        }
        for (final String value : tags.values()) {
            if (value != null && value.toUpperCase(Locale.ROOT).contains(needle)) {
                return true;
            }
        }
        return false;
    }

    String line() {
        return timestamp + " " + direction + " " + msgType + " " + wire;
    }
}

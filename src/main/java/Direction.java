enum Direction {
    IN_ADMIN,
    IN_APP,
    OUT_ADMIN,
    OUT_APP,
    META;

    boolean inbound() {
        return this == IN_ADMIN || this == IN_APP;
    }

    boolean outbound() {
        return this == OUT_ADMIN || this == OUT_APP;
    }
}

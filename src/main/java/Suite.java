enum Suite {
    ALL,
    SMOKE,
    NEGATIVE;

    static Suite parse(final String raw) {
        final String normalized = raw == null
                ? "all"
                : raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "all" -> ALL;
            case "smoke" -> SMOKE;
            case "negative" -> NEGATIVE;
            default -> throw new IllegalArgumentException("suite must be all|smoke|negative");
        };
    }

    boolean shouldRun(final CaseSpec spec) {
        if (this == ALL) {
            return true;
        }
        if (this == SMOKE) {
            return spec.smoke();
        }
        return spec.negative();
    }
}

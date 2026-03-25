import java.util.List;

record CaseResult(
        CaseSpec spec,
        CaseStatus status,
        long startedAt,
        long finishedAt,
        String reason,
        List<String> notes,
        List<String> evidence
) {
    static CaseResult pass(
            final CaseSpec spec,
            final long start,
            final long end,
            final List<String> notes,
            final List<String> evidence
    ) {
        return new CaseResult(spec, CaseStatus.PASS, start, end, "", notes, evidence);
    }

    static CaseResult fail(
            final CaseSpec spec,
            final long start,
            final long end,
            final String reason,
            final List<String> notes,
            final List<String> evidence
    ) {
        return new CaseResult(
                spec,
                CaseStatus.FAIL,
                start,
                end,
                reason == null ? "" : reason,
                notes,
                evidence
        );
    }

    static CaseResult skip(final CaseSpec spec, final String reason) {
        return new CaseResult(
                spec,
                CaseStatus.SKIP,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                reason == null ? "skip" : reason,
                List.of(),
                List.of()
        );
    }

    long durationMs() {
        return Math.max(0L, finishedAt - startedAt);
    }
}

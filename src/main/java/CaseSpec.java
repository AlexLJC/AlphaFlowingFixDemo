record CaseSpec(
        String id,
        String section,
        String name,
        String expected,
        boolean negative,
        boolean smoke
) {
    static CaseSpec authPositive() {
        return new CaseSpec(
                "8.1-A-POS",
                "8.1",
                "Logon positive",
                "A accepted with 49=553 and correct 56",
                false,
                true
        );
    }

    static CaseSpec authInvalidSender() {
        return new CaseSpec(
                "8.1-A-NEG-SENDER",
                "8.1",
                "Logon invalid sender",
                FixClientApp.TOKEN_INVALID_SENDER,
                true,
                false
        );
    }

    static CaseSpec authInvalidTarget() {
        return new CaseSpec(
                "8.1-A-NEG-TARGET",
                "8.1",
                "Logon invalid target",
                FixClientApp.TOKEN_INVALID_TARGET,
                true,
                false
        );
    }

    static CaseSpec authInvalidCredential() {
        return new CaseSpec(
                "8.1-A-NEG-CRED",
                "8.1",
                "Logon invalid credential",
                FixClientApp.TOKEN_INVALID_CREDENTIAL,
                true,
                false
        );
    }

    static CaseSpec vwxPositive() {
        return new CaseSpec(
                "8.2-8.4-VWX-POS",
                "8.2/8.3/8.4",
                "V then W",
                "W with 9021 (X optional)",
                false,
                true
        );
    }

    static CaseSpec vDepthNegative() {
        return new CaseSpec(
                "8.2-V-NEG-DEPTH",
                "8.2",
                "V invalid depth",
                FixClientApp.TOKEN_MD_DEPTH_RANGE,
                true,
                false
        );
    }

    static CaseSpec vQosNegative() {
        return new CaseSpec(
                "8.2-V-NEG-QOS",
                "8.2",
                "V invalid qos",
                FixClientApp.TOKEN_MD_QOS_INVALID,
                true,
                false
        );
    }

    static CaseSpec dPositive() {
        return new CaseSpec(
                "8.6-D-POS",
                "8.6",
                "D market positive",
                "8 accepted",
                false,
                true
        );
    }

    static CaseSpec dReplay() {
        return new CaseSpec(
                "6.2-D-REPLAY",
                "6.2",
                "D duplicate replay",
                "same clOrd replay or replay-unavailable reject",
                false,
                false
        );
    }

    static CaseSpec dQtyNegative() {
        return new CaseSpec(
                "8.6-D-NEG-QTY",
                "8.6",
                "D qty invalid",
                FixClientApp.TOKEN_QTY_INVALID,
                true,
                false
        );
    }

    static CaseSpec fPositive() {
        return new CaseSpec("8.7-F-POS", "8.7", "F positive", "8 cancel", false, false);
    }

    static CaseSpec fUnknownNegative() {
        return new CaseSpec(
                "8.7-F-NEG-UNKNOWN",
                "8.7",
                "F unknown",
                "9 or unknown token",
                true,
                false
        );
    }

    static CaseSpec gPositive() {
        return new CaseSpec("8.8-G-POS", "8.8", "G positive", "8 replace", false, false);
    }

    static CaseSpec gUnknownNegative() {
        return new CaseSpec(
                "8.8-G-NEG-UNKNOWN",
                "8.8",
                "G unknown",
                "9 or unknown token",
                true,
                false
        );
    }

    static CaseSpec hPositive() {
        return new CaseSpec("8.9-H-POS", "8.9", "H positive", "8 status", false, true);
    }

    static CaseSpec hNotFoundNegative() {
        return new CaseSpec(
                "8.9-H-NEG-NOTFOUND",
                "8.9",
                "H not found",
                "not found status",
                true,
                false
        );
    }

    static CaseSpec qPositive() {
        return new CaseSpec("8.10-q-POS", "8.10", "q positive", "cancel stream", false, false);
    }

    static CaseSpec qNoCancellable() {
        return new CaseSpec(
                "8.10-q-NEG-EMPTY",
                "8.10",
                "q empty path",
                "reject/empty path visible",
                true,
                false
        );
    }

    static CaseSpec afPositive() {
        return new CaseSpec("8.11-AF-POS", "8.11", "AF positive", "8 status flow", false, false);
    }

    static CaseSpec afMissingReqId() {
        return new CaseSpec(
                "8.11-AF-NEG-584",
                "8.11",
                "AF missing 584",
                "processable response with degraded correlation",
                true,
                false
        );
    }

    static CaseSpec anApPositive() {
        return new CaseSpec("8.12-ANAP-POS", "8.12", "AN/AP positive", "AP by 710", false, false);
    }

    static CaseSpec anMissingReqId() {
        return new CaseSpec(
                "8.12-AN-NEG-710",
                "8.12",
                "AN missing 710",
                "processable response or visible AP validation",
                true,
                false
        );
    }

    static CaseSpec xyPositive() {
        return new CaseSpec(
                "8.13-xy-POS",
                "8.13",
                "x/y positive",
                "y with 560=0/2 for 167=FOR,460=4",
                false,
                true
        );
    }

    static CaseSpec xWrongTypeNegative() {
        return new CaseSpec(
                "8.13-x-NEG-167",
                "8.13",
                "x no-match filter",
                "y with 560=2 for 167=FUT,460=5",
                true,
                false
        );
    }
}
